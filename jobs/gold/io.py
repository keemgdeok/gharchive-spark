"""
골드 입출력 유틸
"""

from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import DataFrame, functions as F


def build_silver_path(bucket: str, prefix: str, dataset: str) -> str:
    return f"s3a://{bucket}/{prefix.rstrip('/')}/{dataset}"


def build_gold_path(bucket: str, prefix: str, dataset: str) -> str:
    return f"s3a://{bucket}/{prefix.rstrip('/')}/{dataset}"


def read_events_base(*, spark, base_path: str, start_date: dt.date, end_date: dt.date):
    df = spark.read.parquet(base_path).select(
        "dt",
        "repo_name",
        "actor_login",
        "event_type",
        "event_id",
    )
    return df.filter(
        (F.col("dt") >= F.lit(start_date)) & (F.col("dt") <= F.lit(end_date))
    )


def read_events_push(
    *,
    spark,
    base_path: str,
    start_date: dt.date,
    end_date: dt.date,
) -> DataFrame:
    df = spark.read.parquet(base_path).select(
        "dt",
        "repo_name",
        "ref",
    )
    return df.filter(
        (F.col("dt") >= F.lit(start_date)) & (F.col("dt") <= F.lit(end_date))
    )


def read_events_pull_request(
    *,
    spark,
    base_path: str,
    start_date: dt.date,
    end_date: dt.date,
) -> DataFrame:
    df = spark.read.parquet(base_path).select(
        "dt",
        "repo_name",
        "created_at_ts",
        "action",
        "pull_request_id",
        "pull_request_number",
        "pull_request_merged",
        "pull_request_merged_at",
        "commit_count",
        "additions",
        "deletions",
        "changed_files",
    )
    return df.filter(
        (F.col("dt") >= F.lit(start_date)) & (F.col("dt") <= F.lit(end_date))
    )


def read_events_pull_request_review(
    *,
    spark,
    base_path: str,
    start_date: dt.date,
    end_date: dt.date,
) -> DataFrame:
    df = spark.read.parquet(base_path).select(
        "dt",
        "repo_name",
        "pull_request_id",
        "review_id",
        "review_state",
        "review_submitted_at",
    )
    return df.filter(
        (F.col("dt") >= F.lit(start_date)) & (F.col("dt") <= F.lit(end_date))
    )


def read_events_pull_request_review_comment(
    *,
    spark,
    base_path: str,
    start_date: dt.date,
    end_date: dt.date,
) -> DataFrame:
    df = spark.read.parquet(base_path).select(
        "dt",
        "repo_name",
        "pull_request_id",
        "comment_id",
        "comment_created_at",
    )
    return df.filter(
        (F.col("dt") >= F.lit(start_date)) & (F.col("dt") <= F.lit(end_date))
    )


def write_gold(
    *,
    df: DataFrame,
    output_path: str,
    fmt: str,
    coalesce: int,
    partition_cols: list[str] | None = None,
) -> None:
    logging.info("gold write path=%s format=%s coalesce=%d", output_path, fmt, coalesce)
    if coalesce and coalesce > 0:
        df = df.coalesce(coalesce)
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    if fmt == "csv":
        writer.option("header", True).csv(output_path)
    else:
        writer.parquet(output_path)
