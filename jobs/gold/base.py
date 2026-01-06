"""
실버 -> 골드 집계 파이프라인
- Top repos, 이벤트 타입 집계
"""

from __future__ import annotations

import logging

from jobs.gold.agg.actor import build_actor_marts
from jobs.gold.agg.daily import build_daily_top_repos
from jobs.gold.agg.pr import build_pr_review_latency
from jobs.gold.agg.push import build_push_branch_ratio
from jobs.gold.agg.repo import build_repo_marts
from jobs.gold.cli import configure_logging, parse_args, resolve_date_range
from jobs.gold.io import (
    build_gold_path,
    build_silver_path,
    read_events_base,
    write_gold,
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
        explain = args.explain
        events_df, repo_marts = build_repo_marts(
            events_df=events_df,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=explain,
        )
        actor_marts = build_actor_marts(
            events_df=events_df,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=explain,
        )
        daily_top_repos = build_daily_top_repos(
            events_df=events_df,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=explain,
        )
        push_branch_ratio = build_push_branch_ratio(
            spark=spark,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=explain,
        )
        pr_review_latency = build_pr_review_latency(
            spark=spark,
            args=args,
            start_date=start_date,
            end_date=end_date,
            explain=explain,
        )

        # TODO: Challenge #2 작은 파일 완화(coalesce)
        write_gold(
            df=repo_marts["top_repos"],
            output_path=build_gold_path(args.bucket, args.gold_prefix, "top_repos"),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=repo_marts["event_type_counts"],
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "event_type_counts"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=repo_marts["top_repo_event_types"],
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "top_repo_event_types"
            ),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=actor_marts["top_actors"],
            output_path=build_gold_path(args.bucket, args.gold_prefix, "top_actors"),
            fmt=args.output_format,
            coalesce=args.coalesce,
            partition_cols=None,
        )
        write_gold(
            df=actor_marts["top_actor_event_types"],
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "top_actor_event_types"
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
        write_gold(
            df=pr_review_latency,
            output_path=build_gold_path(
                args.bucket, args.gold_prefix, "pull_request_review_latency"
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
