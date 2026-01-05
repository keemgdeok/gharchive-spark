"""
골드 집계 메트릭 계산
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def compute_repo_counts(*, df: DataFrame) -> DataFrame:
    return df.groupBy("repo_name").count().orderBy(F.desc("count"))


def compute_daily_repo_counts(*, df: DataFrame) -> DataFrame:
    return df.groupBy("dt", "repo_name").agg(F.count(F.lit(1)).alias("event_count"))


def compute_daily_topn(*, df: DataFrame, metric_col: str, top_n: int) -> DataFrame:
    top_n = max(1, top_n)
    window = Window.partitionBy("dt").orderBy(F.desc(metric_col))
    return (
        df.withColumn("daily_rank", F.row_number().over(window))
        .filter(F.col("daily_rank") <= F.lit(top_n))
        .orderBy(F.col("dt"), F.col("daily_rank"))
    )


def compute_push_branch_ratio(*, df: DataFrame) -> DataFrame:
    branch_name = F.regexp_replace(F.col("ref"), "^refs/heads/", "")
    branch_group = (
        F.when(F.col("ref").isNull(), F.lit("unknown"))
        .when(branch_name == F.lit("main"), F.lit("main"))
        .when(branch_name == F.lit("master"), F.lit("master"))
        .otherwise(F.lit("other"))
    )
    branch_counts = (
        df.withColumn("branch_group", branch_group)
        .groupBy("dt", "branch_group")
        .agg(F.count(F.lit(1)).alias("push_count"))
    )
    totals = branch_counts.groupBy("dt").agg(
        F.sum("push_count").alias("total_push_count")
    )
    return branch_counts.join(totals, on="dt", how="left").withColumn(
        "branch_ratio",
        F.when(
            F.col("total_push_count") > 0,
            F.col("push_count") / F.col("total_push_count"),
        ).otherwise(F.lit(0.0)),
    )


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
