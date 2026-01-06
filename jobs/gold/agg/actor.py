"""
액터 기반 골드 집계
"""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from jobs.gold.metrics import (
    attach_date_range,
    build_actor_dim,
    compute_actor_counts,
    compute_actor_distinct_repo_counts,
    compute_actor_event_type_counts,
)


def build_actor_marts(
    *,
    events_df: DataFrame,
    args,
    start_date,
    end_date,
    explain: bool,
) -> dict[str, DataFrame]:
    actor_events_df = events_df.filter(F.col("actor_login").isNotNull())
    actor_metric_col = (
        "actor_repo_count"
        if args.actor_metric == "distinct_repo"
        else "actor_event_count"
    )
    if args.actor_metric == "distinct_repo":
        actor_counts = compute_actor_distinct_repo_counts(df=actor_events_df)
    else:
        actor_counts = compute_actor_counts(df=actor_events_df)
    if explain:
        actor_counts.explain(True)

    top_actors = attach_date_range(
        df=actor_counts.limit(max(1, args.top_actors)),
        start_date=start_date,
        end_date=end_date,
    )

    actor_dim = build_actor_dim(top_actors=top_actors, metric_col=actor_metric_col)
    actor_events = events_df.join(actor_dim, on="actor_login", how="inner")
    if explain:
        actor_events.explain(True)

    actor_event_type_counts = attach_date_range(
        df=compute_actor_event_type_counts(
            actor_events=actor_events,
            metric_col=actor_metric_col,
        ),
        start_date=start_date,
        end_date=end_date,
    )

    return {
        "top_actors": top_actors,
        "top_actor_event_types": actor_event_type_counts,
    }
