"""
Bronze(JSON) -> Silver(Parquet) 변환
- Challenge #3 (Complex Nested Schema): explode/col("a.b.c") 기반 flatten + Superset 스키마
"""

import argparse
import datetime as dt
import logging
import os
import sys

from pyspark.sql import functions as F

from jobs.spark_fs import list_files_under, path_exists
from jobs.spark_runtime import get_spark
from jobs.silver.schema import BRONZE_SCHEMA

UTC = dt.timezone.utc


def configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
    if verbose:
        logging.getLogger("py4j").setLevel(logging.WARN)
        logging.getLogger("pyspark").setLevel(logging.WARN)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flatten bronze JSON to silver Parquet"
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


def read_bronze(*, spark, input_path: str):
    return (
        spark.read.schema(BRONZE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .json(input_path)
        .withColumn("source_file", F.input_file_name())
    )


def show_corrupt_samples(*, raw_df, verbose: bool) -> None:
    if not verbose:
        return

    corrupt_sample = (
        raw_df.filter(F.col("_corrupt_record").isNotNull())
        .select("id", "created_at", "_corrupt_record", "source_file")
        .limit(5)
    )
    if corrupt_sample.take(1):
        logging.warning("corrupt record 샘플(최대 5개)")
        corrupt_sample.show(truncate=False)


def normalize_events(*, raw_df):
    return (
        raw_df.filter(F.col("_corrupt_record").isNull())
        .withColumn(
            "created_at_ts",
            F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ssX"),
        )
        .withColumn("dt", F.to_date("created_at_ts"))
        .filter(F.col("created_at_ts").isNotNull())
        .filter(F.col("dt").isNotNull())
    )


def collect_push_payload_stats(*, events_df):
    push_df = events_df.filter(F.col("type") == F.lit("PushEvent"))
    row = push_df.agg(
        F.count(F.lit(1)).alias("push_events"),
        F.sum(
            F.when(F.col("payload.commits").isNotNull(), F.lit(1)).otherwise(0)
        ).alias("commits_not_null"),
        F.sum(
            F.when(F.size(F.col("payload.commits")) > 0, F.lit(1)).otherwise(0)
        ).alias("commits_nonempty"),
        F.sum(F.when(F.col("payload.size").isNotNull(), F.lit(1)).otherwise(0)).alias(
            "size_not_null"
        ),
        F.sum(
            F.when(F.col("payload.distinct_size").isNotNull(), F.lit(1)).otherwise(0)
        ).alias("distinct_size_not_null"),
    ).collect()[0]
    return {
        "push_events": int(row["push_events"]),
        "commits_not_null": int(row["commits_not_null"] or 0),
        "commits_nonempty": int(row["commits_nonempty"] or 0),
        "size_not_null": int(row["size_not_null"] or 0),
        "distinct_size_not_null": int(row["distinct_size_not_null"] or 0),
    }


def transform_push_events(*, events_df):
    return (
        events_df.filter(F.col("type") == F.lit("PushEvent"))
        .select(
            F.col("dt"),
            F.col("created_at_ts"),
            F.col("id").alias("event_id"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.name").alias("repo_name"),
            F.col("payload.repository_id").alias("repository_id"),
            F.col("payload.push_id").alias("push_id"),
            F.col("payload.ref").alias("ref"),
            F.col("payload.head").alias("head"),
            F.col("payload.before").alias("before"),
            F.col("payload.size").alias("push_size"),
            F.col("payload.distinct_size").alias("push_distinct_size"),
            F.when(
                F.col("payload.commits").isNull(),
                F.lit(None).cast("int"),
            )
            .otherwise(F.size(F.col("payload.commits")))
            .alias("commit_count"),
            F.col("public").alias("public"),
            F.col("source_file"),
        )
        .filter(F.col("repo_name").isNotNull())
    )


def transform_push_commits(*, events_df):
    return (
        events_df.filter(F.col("type") == F.lit("PushEvent"))
        .withColumn("commit", F.explode_outer(F.col("payload.commits")))
        .select(
            F.col("dt"),
            F.col("created_at_ts"),
            F.col("id").alias("event_id"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.name").alias("repo_name"),
            F.col("payload.repository_id").alias("repository_id"),
            F.col("payload.push_id").alias("push_id"),
            F.col("payload.size").alias("push_size"),
            F.col("payload.distinct_size").alias("push_distinct_size"),
            F.col("commit.sha").alias("commit_sha"),
            F.col("commit.author.email").alias("commit_author_email"),
            F.col("commit.author.name").alias("commit_author_name"),
            F.col("commit.message").alias("commit_message"),
            F.col("commit.distinct").alias("commit_distinct"),
            F.col("commit.url").alias("commit_url"),
            F.col("public").alias("public"),
            F.col("source_file"),
        )
        .filter(F.col("commit_sha").isNotNull())
    )


def write_silver(*, df, output_path: str, verbose: bool) -> None:
    logging.info("write silver=%s", output_path)
    logging.info("write partitions=%d", df.rdd.getNumPartitions())
    if verbose:
        df.explain(True)
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)


def post_checks(
    *,
    spark,
    output_path: str,
    target_date: dt.date,
    verbose: bool,
    label: str,
    preview_cols: list[str],
) -> None:
    if not path_exists(spark, output_path):
        raise RuntimeError(f"Silver 출력 경로가 생성되지 않았습니다: {output_path}")

    dt_partition = f"{output_path}/dt={target_date:%Y-%m-%d}"
    if path_exists(spark, dt_partition):
        files = list_files_under(spark, dt_partition)
        total_bytes = sum(size for _, size in files)
        logging.info(
            "silver(%s) 파일 개수=%d 총크기=%d bytes (dt=%s)",
            label,
            len(files),
            total_bytes,
            target_date,
        )
    else:
        logging.warning(
            "silver(%s) dt 파티션이 없습니다(데이터가 없을 수 있음): %s",
            label,
            dt_partition,
        )

    if verbose:
        preview = (
            spark.read.parquet(output_path)
            .filter(F.col("dt") == F.lit(target_date))
            .select(*preview_cols)
            .limit(20)
        )
        logging.info("silver(%s) 미리보기(최대 20건)", label)
        preview.show(truncate=False)


def main():
    args = parse_args()
    configure_logging(args.verbose)

    spark = get_spark("gh-archive-silver")
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

        raw_df = read_bronze(spark=spark, input_path=input_path)
        show_corrupt_samples(raw_df=raw_df, verbose=args.verbose)

        events_df = normalize_events(raw_df=raw_df)
        push_stats = collect_push_payload_stats(events_df=events_df)
        logging.info(
            "PushEvent payload 통계 push=%d commits(non-null)=%d commits(non-empty)=%d size=%d distinct_size=%d",
            push_stats["push_events"],
            push_stats["commits_not_null"],
            push_stats["commits_nonempty"],
            push_stats["size_not_null"],
            push_stats["distinct_size_not_null"],
        )

        push_events_df = transform_push_events(events_df=events_df)
        push_events_path = build_silver_path(
            args.bucket, args.silver_prefix, "push_events"
        )
        write_silver(
            df=push_events_df, output_path=push_events_path, verbose=args.verbose
        )
        post_checks(
            spark=spark,
            output_path=push_events_path,
            target_date=target_date,
            verbose=args.verbose,
            label="push_events",
            preview_cols=[
                "dt",
                "repo_name",
                "actor_login",
                "push_id",
                "ref",
                "head",
                "before",
                "commit_count",
                "created_at_ts",
            ],
        )

        if push_stats["commits_nonempty"] > 0:
            commits_df = transform_push_commits(events_df=events_df)
            push_commits_path = build_silver_path(
                args.bucket, args.silver_prefix, "push_commits"
            )
            write_silver(
                df=commits_df, output_path=push_commits_path, verbose=args.verbose
            )
            post_checks(
                spark=spark,
                output_path=push_commits_path,
                target_date=target_date,
                verbose=args.verbose,
                label="push_commits",
                preview_cols=[
                    "dt",
                    "repo_name",
                    "actor_login",
                    "push_id",
                    "commit_sha",
                    "commit_author_email",
                    "created_at_ts",
                ],
            )
        else:
            logging.warning(
                "push_commits 생성을 건너뜁니다(commits 배열 없음 또는 비어있음)"
            )
        logging.info("완료")
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        logging.exception("실패: %s", exc)
        sys.exit(1)
