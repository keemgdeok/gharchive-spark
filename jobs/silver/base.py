"""
브론즈(JSON) -> 실버(events_base + 멀티 트랙)
"""

from __future__ import annotations

import logging
import sys

from pyspark import StorageLevel
from pyspark.sql import functions as F

from jobs.spark_runtime import get_spark
from jobs.silver.cli import (
    build_silver_path,
    configure_logging,
    parse_args,
    resolve_input_path,
    resolve_target_date,
    log_context,
)
from jobs.silver.events_base import (
    build_events_base,
    normalize_events,
    parse_events,
    read_bronze_raw,
    show_invalid_samples,
)
from jobs.silver.output import (
    log_track_stats,
    log_type_counts,
    post_checks,
    write_silver,
)
from jobs.silver.registry import build_tracks


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    if args.coalesce > 0 and args.repartition > 0:
        raise ValueError("coalesce/repartition은 동시에 사용할 수 없습니다")

    spark = get_spark("gh-archive-silver-base")
    try:
        target_date = resolve_target_date(args)
        input_path = resolve_input_path(
            spark=spark,
            bucket=args.bucket,
            bronze_prefix=args.bronze_prefix,
            target_date=target_date,
            hour=args.hour,
        )
        log_context(
            spark=spark,
            input_path=input_path,
            bucket=args.bucket,
            silver_prefix=args.silver_prefix,
        )

        raw_df = read_bronze_raw(spark=spark, input_path=input_path)
        events_df, invalid_df = parse_events(raw_df=raw_df)
        show_invalid_samples(invalid_df=invalid_df, verbose=args.verbose)

        events_df = normalize_events(events_df=events_df)
        base_df = build_events_base(events_df=events_df)
        base_partition_df = base_df.filter(F.col("dt") == F.lit(target_date)).persist(
            StorageLevel.DISK_ONLY
        )
        base_partition_df.count()

        base_path = build_silver_path(args.bucket, args.silver_prefix, "events_base")
        write_silver(
            df=base_partition_df,
            output_path=base_path,
            verbose=args.verbose,
            coalesce=args.coalesce,
            repartition=args.repartition,
        )
        post_checks(
            spark=spark,
            output_path=base_path,
            target_date=target_date,
            verbose=args.verbose,
            label="events_base",
            preview_cols=[
                "dt",
                "event_type",
                "repo_name",
                "actor_login",
                "created_at_ts",
            ],
        )

        if args.verbose:
            log_type_counts(base_df=base_partition_df)

        for label, df in build_tracks(base_df=base_partition_df):
            output_path = build_silver_path(args.bucket, args.silver_prefix, label)
            write_silver(
                df=df,
                output_path=output_path,
                verbose=args.verbose,
                coalesce=args.coalesce,
                repartition=args.repartition,
            )
            post_checks(
                spark=spark,
                output_path=output_path,
                target_date=target_date,
                verbose=args.verbose,
                label=label,
                preview_cols=None,
            )
            log_track_stats(df=df, label=label, verbose=args.verbose)

        logging.info("완료")
    finally:
        if "base_partition_df" in locals():
            base_partition_df.unpersist()
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.exception("실패: %s", exc)
        sys.exit(1)
