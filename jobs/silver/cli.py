from __future__ import annotations

import argparse
import datetime as dt
import logging
import os

from jobs.spark_fs import list_files_under, path_exists

UTC = dt.timezone.utc


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
    if verbose:
        logging.getLogger("py4j").setLevel(logging.WARN)
        logging.getLogger("pyspark").setLevel(logging.WARN)


def parse_hour(text: str) -> dt.datetime:
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--hour 형식 오류: {exc}") from exc


def parse_date(text: str) -> dt.date:
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"--date 형식 오류: {exc}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten bronze JSON to silver events_base + multi-track"
    )
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "gharchive"))
    parser.add_argument("--bronze-prefix", default="bronze")
    parser.add_argument("--silver-prefix", default="silver")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", type=parse_date, help="UTC 기준 단일 날짜 yyyy-mm-dd")
    group.add_argument(
        "--hour", type=parse_hour, help="UTC 기준 단일 시간 yyyy-mm-dd-HH"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="DEBUG 로그 활성화",
    )
    return parser.parse_args()


def build_bronze_glob(bucket: str, prefix: str, target_date: dt.date) -> str:
    return f"s3a://{bucket}/{prefix.rstrip('/')}/{target_date:%Y/%m/%d}/*.json.gz"


def build_bronze_hour_path(bucket: str, prefix: str, target_hour: dt.datetime) -> str:
    date_str = target_hour.strftime("%Y-%m-%d")
    return (
        f"s3a://{bucket}/{prefix.rstrip('/')}/{target_hour:%Y/%m/%d}/"
        f"{date_str}-{target_hour.hour}.json.gz"
    )


def build_silver_path(bucket: str, prefix: str, dataset: str) -> str:
    return f"s3a://{bucket}/{prefix.rstrip('/')}/{dataset}"


def resolve_target_date(args: argparse.Namespace) -> dt.date:
    target_date = args.date
    if target_date is None and args.hour is None:
        return (dt.datetime.now(tz=UTC) - dt.timedelta(days=1)).date()
    if target_date is None and args.hour is not None:
        return args.hour.date()
    if target_date is None:
        raise RuntimeError("처리 대상 날짜를 결정하지 못했습니다")
    return target_date


def resolve_input_path(
    *,
    spark,
    bucket: str,
    bronze_prefix: str,
    target_date: dt.date,
    hour: dt.datetime | None,
) -> str:
    if hour is not None:
        input_path = build_bronze_hour_path(bucket, bronze_prefix, hour)
        if not path_exists(spark, input_path):
            raise FileNotFoundError(f"입력 파일이 없습니다: {input_path}")
        return input_path

    input_dir = f"s3a://{bucket}/{bronze_prefix.rstrip('/')}/{target_date:%Y/%m/%d}"
    candidates = [
        p for p, _ in list_files_under(spark, input_dir) if p.endswith(".json.gz")
    ]
    if not candidates:
        raise FileNotFoundError(f"입력 파일이 없습니다: {input_dir}/*.json.gz")
    return build_bronze_glob(bucket, bronze_prefix, target_date)


def log_context(*, spark, input_path: str, bucket: str, silver_prefix: str) -> None:
    logging.info("bronze input=%s", input_path)
    logging.info("silver output bucket=%s prefix=%s", bucket, silver_prefix)
    logging.info("spark master=%s", spark.conf.get("spark.master", "unknown"))
    logging.info(
        "s3a endpoint=%s",
        spark.conf.get("spark.hadoop.fs.s3a.endpoint", "unknown"),
    )
    logging.info(
        "AQE=%s shuffle.partitions=%s",
        spark.conf.get("spark.sql.adaptive.enabled", "unknown"),
        spark.conf.get("spark.sql.shuffle.partitions", "unknown"),
    )
