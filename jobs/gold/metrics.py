"""
골드 집계 메트릭 계산
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F


def compute_repo_counts(*, df: DataFrame) -> DataFrame:
    return df.groupBy("repo_name").count().orderBy(F.desc("count"))


def compute_event_type_counts(*, df: DataFrame) -> DataFrame:
    return df.groupBy("event_type").count().orderBy(F.desc("count"))


def attach_date_range(*, df: DataFrame, start_date, end_date) -> DataFrame:
    return df.withColumn("start_dt", F.lit(start_date)).withColumn(
        "end_dt", F.lit(end_date)
    )


def build_repo_dim(*, top_repos: DataFrame) -> DataFrame:
    return top_repos.select("repo_name", F.col("count").alias("repo_event_count"))


def compute_repo_event_type_counts(*, repo_events: DataFrame) -> DataFrame:
    return (
        repo_events.groupBy("repo_name", "event_type")
        .agg(
            F.count(F.lit(1)).alias("event_type_count"),
            F.max("repo_event_count").alias("repo_event_count"),
        )
        .orderBy(F.desc("event_type_count"))
    )
