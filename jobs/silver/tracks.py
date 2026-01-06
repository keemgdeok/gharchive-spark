from __future__ import annotations

from pyspark.sql import functions as F
from pyspark.sql import types as T

from jobs.silver.events_base import base_track_columns, with_payload
from jobs.silver.mappings import push_commit_cols, push_event_cols
from jobs.silver.schema import PUSH_PAYLOAD_SCHEMA


def _payload_keyset_hash(*, payload_raw_col):
    payload_map = F.from_json(
        payload_raw_col, T.MapType(T.StringType(), T.StringType())
    )
    payload_keys = F.sort_array(F.array_distinct(F.map_keys(payload_map)))
    return (
        F.when(payload_raw_col.isNull(), F.lit("parse_fail"))
        .when(payload_map.isNull(), F.lit("parse_fail"))
        .otherwise(F.sha2(F.concat_ws(",", payload_keys), 256))
    )


def _simple_track(
    *,
    base_df,
    event_type: str,
    payload_schema,
    payload_cols_fn,
):
    payload_cols = payload_cols_fn()
    df = with_payload(
        base_df=base_df,
        event_type=event_type,
        payload_schema=payload_schema,
    )
    payload_variant = _payload_keyset_hash(payload_raw_col=F.col("payload_raw"))
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        payload_variant.alias("payload_variant"),
        *payload_cols,
    )


def make_simple_builder(
    event_type: str,
    payload_schema,
    payload_cols_fn,
):
    def _builder(*, base_df):
        return _simple_track(
            base_df=base_df,
            event_type=event_type,
            payload_schema=payload_schema,
            payload_cols_fn=payload_cols_fn,
        )

    return _builder


def transform_push_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PushEvent",
        payload_schema=PUSH_PAYLOAD_SCHEMA,
    )
    commit_count_col = F.coalesce(F.size(F.col("payload.commits")), F.lit(0))
    variant_col = _payload_keyset_hash(payload_raw_col=F.col("payload_raw"))
    push_cols = push_event_cols(commit_count_col=commit_count_col)
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        variant_col.alias("payload_variant"),
        *push_cols,
    )


def transform_push_commits(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PushEvent",
        payload_schema=PUSH_PAYLOAD_SCHEMA,
    )
    variant_col = _payload_keyset_hash(payload_raw_col=F.col("payload_raw"))
    return (
        df.withColumn("payload_parse_ok", F.col("payload").isNotNull())
        .withColumn("commit", F.explode_outer(F.col("payload.commits")))
        .select(
            *base_track_columns(),
            F.col("payload_parse_ok"),
            variant_col.alias("payload_variant"),
            *push_commit_cols(),
        )
        .filter(F.col("commit_sha").isNotNull())
    )


def transform_public_events(*, base_df):
    return base_df.filter(F.col("event_type") == F.lit("PublicEvent")).select(
        *base_track_columns(),
        F.lit(True).alias("payload_parse_ok"),
        F.lit("no_payload").alias("payload_variant"),
    )
