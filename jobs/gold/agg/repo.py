"""
레포지토리 기반 골드 집계
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from jobs.gold.metrics import (
    attach_date_range,
    build_repo_dim,
    compute_event_type_counts,
    compute_repo_counts,
    compute_repo_event_type_counts,
)


def build_repo_marts(
    *,
    events_df: DataFrame,
    args,
    start_date,
    end_date,
    explain: bool,
) -> tuple[DataFrame, dict[str, DataFrame]]:
    repo_counts = compute_repo_counts(df=events_df)
    if explain:
        repo_counts.explain(True)
    top_repos = attach_date_range(
        df=repo_counts.limit(max(1, args.top_n)),
        start_date=start_date,
        end_date=end_date,
    )

    event_type_counts = attach_date_range(
        df=compute_event_type_counts(df=events_df).limit(max(1, args.top_event_types)),
        start_date=start_date,
        end_date=end_date,
    )

    repo_dim = build_repo_dim(top_repos=top_repos)
    repo_events = events_df.join(repo_dim, on="repo_name", how="inner")
    if explain:
        repo_events.explain(True)

    repo_event_type_counts = attach_date_range(
        df=compute_repo_event_type_counts(repo_events=repo_events),
        start_date=start_date,
        end_date=end_date,
    )

    marts = {
        "top_repos": top_repos,
        "event_type_counts": event_type_counts,
        "top_repo_event_types": repo_event_type_counts,
    }
    return events_df, marts
