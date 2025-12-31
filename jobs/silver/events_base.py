from __future__ import annotations

import logging

from pyspark.sql import functions as F

from jobs.silver.schema import EVENT_BASE_SCHEMA


def read_bronze_raw(*, spark, input_path: str):
    return (
        spark.read.text(input_path)
        .withColumnRenamed("value", "raw_json")
        .withColumn("source_file", F.input_file_name())
    )


def parse_events(*, raw_df):
    parsed_df = raw_df.select(
        F.from_json(F.col("raw_json"), EVENT_BASE_SCHEMA).alias("event"),
        F.col("raw_json"),
        F.col("source_file"),
    )
    invalid_df = parsed_df.filter(F.col("event").isNull())
    events_df = (
        parsed_df.filter(F.col("event").isNotNull())
        .select("event.*", "raw_json", "source_file")
        .withColumn("payload_raw", F.get_json_object("raw_json", "$.payload"))
        .drop("raw_json")
    )
    return events_df, invalid_df


def show_invalid_samples(*, invalid_df, verbose: bool) -> None:
    if not verbose:
        return

    if invalid_df.take(1):
        logging.warning("JSON 파싱 실패 샘플(최대 5개)")
        invalid_df.select("raw_json", "source_file").limit(5).show(truncate=False)


def normalize_events(*, events_df):
    return (
        events_df.withColumn(
            "created_at_ts",
            F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ssX"),
        )
        .withColumn("dt", F.to_date("created_at_ts"))
        .filter(F.col("created_at_ts").isNotNull())
        .filter(F.col("dt").isNotNull())
        .filter(F.col("id").isNotNull())
        .filter(F.col("type").isNotNull())
    )


def build_events_base(*, events_df):
    return events_df.select(
        F.col("dt"),
        F.col("created_at"),
        F.col("created_at_ts"),
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("actor.display_login").alias("actor_display_login"),
        F.col("actor.gravatar_id").alias("actor_gravatar_id"),
        F.col("actor.url").alias("actor_url"),
        F.col("actor.avatar_url").alias("actor_avatar_url"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("repo.url").alias("repo_url"),
        F.col("org.id").alias("org_id"),
        F.col("org.login").alias("org_login"),
        F.col("org.gravatar_id").alias("org_gravatar_id"),
        F.col("org.url").alias("org_url"),
        F.col("org.avatar_url").alias("org_avatar_url"),
        F.col("public").alias("public"),
        F.col("payload_raw"),
        F.col("source_file"),
    )


def base_track_columns():
    return [
        F.col("dt"),
        F.col("created_at_ts"),
        F.col("event_id"),
        F.col("event_type"),
        F.col("actor_id"),
        F.col("actor_login"),
        F.col("actor_display_login"),
        F.col("repo_id"),
        F.col("repo_name"),
        F.col("repo_url"),
        F.col("org_id"),
        F.col("org_login"),
        F.col("public"),
        F.col("source_file"),
    ]


def with_payload(*, base_df, event_type: str, payload_schema):
    return base_df.filter(F.col("event_type") == F.lit(event_type)).withColumn(
        "payload", F.from_json(F.col("payload_raw"), payload_schema)
    )
