"""
골드 집계 메트릭 계산
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


def compute_repo_counts(*, df: DataFrame) -> DataFrame:
    return (
        df.groupBy("repo_name")
        .agg(F.count(F.lit(1)).alias("repo_event_count"))
        .orderBy(F.desc("repo_event_count"))
    )


def compute_actor_counts(*, df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("actor_login").isNotNull())
        .groupBy("actor_login")
        .agg(F.count(F.lit(1)).alias("actor_event_count"))
        .orderBy(F.desc("actor_event_count"))
    )


def compute_actor_distinct_repo_counts(*, df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("actor_login").isNotNull())
        .groupBy("actor_login")
        .agg(F.countDistinct("repo_name").alias("actor_repo_count"))
        .orderBy(F.desc("actor_repo_count"))
    )


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
    return (
        df.groupBy("event_type")
        .agg(F.count(F.lit(1)).alias("event_count"))
        .orderBy(F.desc("event_count"))
    )


def attach_date_range(*, df: DataFrame, start_date, end_date) -> DataFrame:
    return df.withColumn("start_dt", F.lit(start_date)).withColumn(
        "end_dt", F.lit(end_date)
    )


def build_repo_dim(*, top_repos: DataFrame) -> DataFrame:
    return top_repos.select("repo_name", "repo_event_count")


def build_actor_dim(*, top_actors: DataFrame, metric_col: str) -> DataFrame:
    return top_actors.select("actor_login", F.col(metric_col))


def compute_repo_event_type_counts(*, repo_events: DataFrame) -> DataFrame:
    return (
        repo_events.groupBy("repo_name", "event_type")
        .agg(
            F.count(F.lit(1)).alias("event_type_count"),
            F.max("repo_event_count").alias("repo_event_count"),
        )
        .orderBy(F.desc("event_type_count"))
    )


def compute_actor_event_type_counts(
    *,
    actor_events: DataFrame,
    metric_col: str,
) -> DataFrame:
    return (
        actor_events.groupBy("actor_login", "event_type")
        .agg(
            F.count(F.lit(1)).alias("event_type_count"),
            F.max(metric_col).alias(metric_col),
        )
        .orderBy(F.desc("event_type_count"))
    )


def build_pull_request_base(*, pr_events: DataFrame) -> DataFrame:
    merged_at = F.to_timestamp("pull_request_merged_at", "yyyy-MM-dd'T'HH:mm:ssX")
    opened_at = F.when(F.col("action") == F.lit("opened"), F.col("created_at_ts"))
    return (
        pr_events.withColumn("opened_at", opened_at)
        .withColumn("merged_at", merged_at)
        .filter(F.col("pull_request_id").isNotNull())
        .groupBy("repo_name", "pull_request_id")
        .agg(
            F.min("opened_at").alias("opened_at"),
            F.max("merged_at").alias("merged_at"),
            F.max("pull_request_merged").alias("pull_request_merged"),
            F.max("commit_count").alias("commit_count"),
            F.max("additions").alias("additions"),
            F.max("deletions").alias("deletions"),
            F.max("changed_files").alias("changed_files"),
        )
        .filter(F.col("opened_at").isNotNull())
        .withColumn("dt", F.to_date("opened_at"))
    )


def compute_pr_review_stats(*, pr_reviews: DataFrame) -> DataFrame:
    submitted_at = F.to_timestamp("review_submitted_at", "yyyy-MM-dd'T'HH:mm:ssX")
    return (
        pr_reviews.withColumn("review_submitted_at", submitted_at)
        .filter(F.col("pull_request_id").isNotNull())
        .filter(F.col("review_id").isNotNull())
        .groupBy("repo_name", "pull_request_id")
        .agg(
            F.min("review_submitted_at").alias("first_review_at"),
            F.countDistinct("review_id").alias("review_count"),
            F.sum(
                F.when(F.col("review_state") == F.lit("approved"), F.lit(1)).otherwise(
                    F.lit(0)
                )
            ).alias("approval_count"),
        )
    )


def compute_pr_review_comment_stats(*, pr_review_comments: DataFrame) -> DataFrame:
    comment_at = F.to_timestamp("comment_created_at", "yyyy-MM-dd'T'HH:mm:ssX")
    return (
        pr_review_comments.withColumn("comment_created_at", comment_at)
        .filter(F.col("pull_request_id").isNotNull())
        .filter(F.col("comment_id").isNotNull())
        .groupBy("repo_name", "pull_request_id")
        .agg(
            F.min("comment_created_at").alias("first_review_comment_at"),
            F.countDistinct("comment_id").alias("review_comment_count"),
        )
    )


def compute_pr_review_latency_metrics(
    *,
    pr_base: DataFrame,
    review_stats: DataFrame,
    review_comment_stats: DataFrame,
) -> DataFrame:
    joined = (
        pr_base.join(review_stats, on=["repo_name", "pull_request_id"], how="left")
        .join(review_comment_stats, on=["repo_name", "pull_request_id"], how="left")
        .withColumn("review_count", F.coalesce(F.col("review_count"), F.lit(0)))
        .withColumn("approval_count", F.coalesce(F.col("approval_count"), F.lit(0)))
        .withColumn(
            "review_comment_count", F.coalesce(F.col("review_comment_count"), F.lit(0))
        )
    )
    first_review_hours = (
        F.unix_timestamp("first_review_at") - F.unix_timestamp("opened_at")
    ) / F.lit(3600.0)
    first_comment_hours = (
        F.unix_timestamp("first_review_comment_at") - F.unix_timestamp("opened_at")
    ) / F.lit(3600.0)
    enriched = (
        joined.withColumn(
            "first_review_hours",
            F.when(F.col("first_review_at").isNotNull(), first_review_hours),
        )
        .withColumn(
            "first_review_comment_hours",
            F.when(F.col("first_review_comment_at").isNotNull(), first_comment_hours),
        )
        .withColumn(
            "reviewed_pr",
            F.when(F.col("first_review_at").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )
    )
    # TODO: Challenge #1 Window 기반 정렬로 레포별 쏠림 재현
    repo_activity_window = Window.partitionBy("repo_name").orderBy("opened_at")
    rolling_window = (
        Window.partitionBy("repo_name").orderBy("opened_at").rowsBetween(-50, 0)
    )
    activity = (
        enriched.withColumn(
            "prev_opened_at", F.lag("opened_at").over(repo_activity_window)
        )
        .withColumn(
            "pr_gap_hours",
            (F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at"))
            / F.lit(3600.0),
        )
        .withColumn("rolling_pr_count_50", F.count(F.lit(1)).over(rolling_window))
        .withColumn(
            "rolling_avg_first_review_hours_50",
            F.avg("first_review_hours").over(rolling_window),
        )
    )
    repo_window = Window.partitionBy("repo_name").orderBy(
        F.col("first_review_hours").asc_nulls_last()
    )
    daily_window = Window.partitionBy("repo_name", "dt").orderBy(
        F.col("first_review_hours").asc_nulls_last()
    )
    ranked = (
        activity.withColumn("repo_review_speed_rank", F.row_number().over(repo_window))
        .withColumn(
            "repo_review_speed_percent_rank", F.percent_rank().over(repo_window)
        )
        .withColumn("daily_review_speed_rank", F.row_number().over(daily_window))
    )
    return (
        ranked.groupBy("dt", "repo_name")
        .agg(
            F.countDistinct("pull_request_id").alias("pr_count"),
            F.sum("reviewed_pr").alias("reviewed_pr_count"),
            F.avg("first_review_hours").alias("avg_first_review_hours"),
            F.expr("percentile_approx(first_review_hours, 0.9)").alias(
                "p90_first_review_hours"
            ),
            F.expr("percentile_approx(first_review_hours, 0.95)").alias(
                "p95_first_review_hours"
            ),
            F.avg("first_review_comment_hours").alias("avg_first_review_comment_hours"),
            F.avg("review_count").alias("avg_review_count"),
            F.avg("review_comment_count").alias("avg_review_comment_count"),
            F.sum("review_comment_count").alias("total_review_comment_count"),
            F.sum("approval_count").alias("total_approval_count"),
            F.avg("repo_review_speed_rank").alias("avg_repo_review_speed_rank"),
            F.expr("percentile_approx(repo_review_speed_rank, 0.9)").alias(
                "p90_repo_review_speed_rank"
            ),
            F.avg("repo_review_speed_percent_rank").alias(
                "avg_repo_review_speed_percent_rank"
            ),
            F.avg("daily_review_speed_rank").alias("avg_daily_review_speed_rank"),
            F.avg("pr_gap_hours").alias("avg_pr_gap_hours"),
            F.avg("rolling_pr_count_50").alias("avg_rolling_pr_count_50"),
            F.max("rolling_pr_count_50").alias("max_rolling_pr_count_50"),
            F.avg("rolling_avg_first_review_hours_50").alias(
                "avg_rolling_first_review_hours_50"
            ),
        )
        .orderBy(F.desc("pr_count"))
    )
