"""
브론즈(JSON) -> 실버(events_base + 멀티 트랙)

- CLI: spark-submit jobs/silver/base.py --date 2024-01-01
- @task.pyspark: process(spark, target_date) 호출
"""

from __future__ import annotations

import datetime as dt
import logging
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

from jobs.spark_runtime import get_spark
from jobs.silver.cli import (
    build_bronze_glob,
    build_silver_path,
    configure_logging,
    parse_args,
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
from jobs.spark_fs import list_files_under


def _resolve_input_path(
    spark: SparkSession,
    bucket: str,
    bronze_prefix: str,
    target_date: dt.date,
) -> str:
    """Bronze 입력 경로 해석"""
    input_dir = f"s3a://{bucket}/{bronze_prefix.rstrip('/')}/{target_date:%Y/%m/%d}"
    candidates = [
        p for p, _ in list_files_under(spark, input_dir) if p.endswith(".json.gz")
    ]
    if not candidates:
        raise FileNotFoundError(f"입력 파일이 없습니다: {input_dir}/*.json.gz")
    return build_bronze_glob(bucket, bronze_prefix, target_date)


def process(
    spark: SparkSession,
    target_date: str | dt.date,
    *,
    bucket: str = "gharchive",
    bronze_prefix: str = "bronze",
    silver_prefix: str = "silver",
    verbose: bool = False,
    coalesce: int = 0,
    repartition: int = 0,
) -> None:
    """
    Silver 레이어 처리 (core logic)

    Args:
        spark: SparkSession (외부 주입 또는 내부 생성)
        target_date: 처리 대상 날짜 (YYYY-MM-DD 문자열 또는 date 객체)
        bucket: S3 버킷 이름
        bronze_prefix: Bronze 경로 prefix
        silver_prefix: Silver 경로 prefix
        verbose: 상세 로그 출력
        coalesce: 출력 병합 파티션 수 (0이면 비활성)
        repartition: 출력 재분배 파티션 수 (0이면 비활성)
    """
    if coalesce > 0 and repartition > 0:
        raise ValueError("coalesce/repartition은 동시에 사용할 수 없습니다")

    # 문자열 -> date 변환
    if isinstance(target_date, str):
        target_date = dt.datetime.strptime(target_date, "%Y-%m-%d").date()

    input_path = _resolve_input_path(spark, bucket, bronze_prefix, target_date)
    log_context(
        spark=spark,
        input_path=input_path,
        bucket=bucket,
        silver_prefix=silver_prefix,
    )

    raw_df = read_bronze_raw(spark=spark, input_path=input_path)
    events_df, invalid_df = parse_events(raw_df=raw_df)
    show_invalid_samples(invalid_df=invalid_df, verbose=verbose)

    events_df = normalize_events(events_df=events_df)
    base_df = build_events_base(events_df=events_df)
    base_partition_df = base_df.filter(F.col("dt") == F.lit(target_date)).persist(
        StorageLevel.DISK_ONLY
    )
    base_partition_df.count()

    base_path = build_silver_path(bucket, silver_prefix, "events_base")
    write_silver(
        df=base_partition_df,
        output_path=base_path,
        verbose=verbose,
        coalesce=coalesce,
        repartition=repartition,
    )
    post_checks(
        spark=spark,
        output_path=base_path,
        target_date=target_date,
        verbose=verbose,
        label="events_base",
        preview_cols=[
            "dt",
            "event_type",
            "repo_name",
            "actor_login",
            "created_at_ts",
        ],
    )

    if verbose:
        log_type_counts(base_df=base_partition_df)

    for label, df in build_tracks(base_df=base_partition_df):
        output_path = build_silver_path(bucket, silver_prefix, label)
        write_silver(
            df=df,
            output_path=output_path,
            verbose=verbose,
            coalesce=coalesce,
            repartition=repartition,
        )
        post_checks(
            spark=spark,
            output_path=output_path,
            target_date=target_date,
            verbose=verbose,
            label=label,
            preview_cols=None,
        )
        log_track_stats(df=df, label=label, verbose=verbose)

    base_partition_df.unpersist()
    logging.info("Silver 처리 완료: %s", target_date)


def main() -> None:
    """CLI 엔트리포인트"""
    args = parse_args()
    configure_logging(args.verbose)

    spark = get_spark("gh-archive-silver-base")
    try:
        # CLI에서는 args.date 또는 args.hour 사용
        if args.hour is not None:
            target_date = args.hour.date()
        elif args.date is not None:
            target_date = args.date
        else:
            target_date = (
                dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=1)
            ).date()

        process(
            spark=spark,
            target_date=target_date,
            bucket=args.bucket,
            bronze_prefix=args.bronze_prefix,
            silver_prefix=args.silver_prefix,
            verbose=args.verbose,
            coalesce=args.coalesce,
            repartition=args.repartition,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.exception("실패: %s", exc)
        sys.exit(1)
