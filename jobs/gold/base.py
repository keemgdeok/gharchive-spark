"""
실버 -> 골드 집계 파이프라인
- Top repos, 이벤트 타입 집계
- TODO: Challenge #1 데이터 쏠림 재현/완화(살팅 + 브로드캐스트 조인)
- TODO: Challenge #2 작은 파일 완화(coalesce)
"""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from jobs.gold.cli import configure_logging, parse_args, resolve_date_range
from jobs.gold.io import (
    build_gold_path,
    build_silver_path,
    read_events_base,
    read_events_push,
    write_gold,
)
from jobs.gold.metrics import (
    attach_date_range,
    build_repo_dim,
    compute_daily_repo_counts,
    compute_daily_topn,
    compute_event_type_counts,
    compute_push_branch_ratio,
    compute_repo_counts,
    compute_repo_event_type_counts,
)
from jobs.gold.skew import (
    inflate_skew,
    resolve_skew_target,
    resolve_skewed_repos,
    salted_repo_counts,
)
from jobs.spark_runtime import get_spark


def log_runtime_context(*, spark, base_path, start_date, end_date) -> None:
    logging.info("silver base=%s", base_path)
    logging.info("date range=%s ~ %s", start_date, end_date)
    logging.info(
        "AQE=%s shuffle.partitions=%s",
        spark.conf.get("spark.sql.adaptive.enabled", "unknown"),
        spark.conf.get("spark.sql.shuffle.partitions", "unknown"),
    )


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    spark = get_spark("gh-archive-gold")
    try:
        start_date, end_date = resolve_date_range(args)
        base_path = build_silver_path(args.bucket, args.silver_prefix, "events_base")

        log_runtime_context(
            spark=spark,
            base_path=base_path,
            start_date=start_date,
            end_date=end_date,
        )

        events_df = read_events_base(
            spark=spark, base_path=base_path, start_date=start_date, end_date=end_date
        )

        base_repo_counts = compute_repo_counts(df=events_df)
        skew_target_repo = resolve_skew_target(
            repo_counts=base_repo_counts,
            preferred_repo=args.skew_repo,
        )
        if args.skew_multiplier > 1:
            if skew_target_repo:
                events_df = inflate_skew(
                    df=events_df,
                    repo_name=skew_target_repo,
                    multiplier=args.skew_multiplier,
                )
                base_repo_counts = compute_repo_counts(df=events_df)
            else:
                logging.warning("쏠림 대상 레포가 없어 skew 재현을 건너뜁니다")

        skew_top_k = max(1, args.skew_top_k)
        skewed_repos = resolve_skewed_repos(
            repo_counts=base_repo_counts,
            top_k=skew_top_k,
            preferred_repo=args.skew_repo,
        )
        logging.info("salting 대상 repos=%s", skewed_repos)
        if args.explain:
            base_repo_counts.explain(True)

        if args.disable_salting:
            repo_counts = base_repo_counts
        else:
            # TODO: Challenge #1 데이터 쏠림 완화(살팅)
            repo_counts = salted_repo_counts(
                df=events_df,
                skewed_repos=skewed_repos,
                salt_buckets=max(1, args.salt_buckets),
                salt_seed=args.salt_seed,
            )
            if args.explain:
                repo_counts.explain(True)

        top_repos = attach_date_range(
            df=repo_counts.limit(max(1, args.top_n)),
            start_date=start_date,
            end_date=end_date,
        )

        event_type_counts = attach_date_range(
            df=compute_event_type_counts(df=events_df).limit(
                max(1, args.top_event_types)
            ),
            start_date=start_date,
            end_date=end_date,
        )

        repo_dim = build_repo_dim(top_repos=top_repos)
        if args.broadcast_dim:
            # TODO: Challenge #1 브로드캐스트 조인으로 셔플 회피
            repo_dim = F.broadcast(repo_dim)

        repo_events = events_df.join(repo_dim, on="repo_name", how="inner")
        if args.explain:
            repo_events.explain(True)

        repo_event_type_counts = attach_date_range(
            df=compute_repo_event_type_counts(repo_events=repo_events),
            start_date=start_date,
            end_date=end_date,
        )

        daily_top_repos = None
        daily_top_n = max(0, args.daily_top_n)
        if daily_top_n > 0:
            daily_repo_counts = compute_daily_repo_counts(df=events_df)
            daily_top_repos = attach_date_range(
                df=compute_daily_topn(
                    df=daily_repo_counts,
                    metric_col="event_count",
                    top_n=daily_top_n,
                ),
                start_date=start_date,
                end_date=end_date,
            )
            if args.explain:
                daily_top_repos.explain(True)

        push_events_path = build_silver_path(
            args.bucket,
            args.silver_prefix,
            "events_push",
        )
        push_events_df = read_events_push(
            spark=spark,
            base_path=push_events_path,
            start_date=start_date,
            end_date=end_date,
        )
        push_branch_ratio = attach_date_range(
            df=compute_push_branch_ratio(df=push_events_df),
            start_date=start_date,
            end_date=end_date,
        )
        if args.explain:
            push_branch_ratio.explain(True)

        # TODO: Challenge #2 작은 파일 완화(coalesce)
        write_gold(
            df=top_repos,
            output_path=build_gold_path(args.bucket, args.gold_prefix, "top_repos"),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=event_type_counts,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "event_type_counts"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=repo_event_type_counts,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "top_repo_event_types"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        if daily_top_repos is not None:
            write_gold(
                df=daily_top_repos,
                output_path=build_gold_path(
                    args.bucket, args.gold_prefix, "daily_top_repos"
                ),
                fmt=args.output_format,
                coalesce=args.coalesce,
                partition_cols=["dt"],
            )
        write_gold(
            df=push_branch_ratio,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "push_branch_ratio"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=["dt"],
        )

        logging.info("gold 집계 완료")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
