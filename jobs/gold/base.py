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
    write_gold,
)
from jobs.gold.metrics import (
    attach_date_range,
    build_repo_dim,
    compute_event_type_counts,
    compute_repo_counts,
    compute_repo_event_type_counts,
)
from jobs.gold.skew import inflate_skew, pick_skewed_repos, salted_repo_counts
from jobs.spark_runtime import get_spark


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)

    spark = get_spark("gh-archive-gold")
    try:
        start_date, end_date = resolve_date_range(args)
        base_path = build_silver_path(args.bucket, args.silver_prefix, "events_base")

        logging.info("silver base=%s", base_path)
        logging.info("date range=%s ~ %s", start_date, end_date)
        logging.info(
            "AQE=%s shuffle.partitions=%s",
            spark.conf.get("spark.sql.adaptive.enabled", "unknown"),
            spark.conf.get("spark.sql.shuffle.partitions", "unknown"),
        )

        events_df = read_events_base(
            spark=spark, base_path=base_path, start_date=start_date, end_date=end_date
        )

        base_repo_counts = compute_repo_counts(df=events_df)
        if args.explain:
            base_repo_counts.explain(True)

        top_repo_rows = base_repo_counts.limit(1).collect()
        if args.skew_multiplier > 1 and top_repo_rows:
            top_repo_name = top_repo_rows[0]["repo_name"]
            events_df = inflate_skew(
                df=events_df, repo_name=top_repo_name, multiplier=args.skew_multiplier
            )
            base_repo_counts = compute_repo_counts(df=events_df)

        skew_top_k = max(1, args.skew_top_k)
        skewed_repos = pick_skewed_repos(base_repo_counts, skew_top_k)
        logging.info("salting 대상 repos=%s", skewed_repos)

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

        # TODO: Challenge #2 작은 파일 완화(coalesce)
        write_gold(
            df=top_repos,
            output_path=build_gold_path(args.bucket, args.gold_prefix, "top_repos"),
            fmt=args.output_format,
            coalesce=args.coalesce,
        )
        write_gold(
            df=event_type_counts,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "event_type_counts"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
        )
        write_gold(
            df=repo_event_type_counts,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "top_repo_event_types"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
        )

        logging.info("gold 집계 완료")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
