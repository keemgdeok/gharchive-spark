"""
일별 집계
"""

from __future__ import annotations

from pyspark.sql import DataFrame

from jobs.gold.metrics import (
    attach_date_range,
    compute_daily_repo_counts,
    compute_daily_topn,
)


def build_daily_top_repos(
    *,
    events_df: DataFrame,
    args,
    start_date,
    end_date,
    explain: bool,
) -> DataFrame | None:
    daily_top_n = max(0, args.daily_top_n)
    if daily_top_n <= 0:
        return None
    daily_repo_counts = compute_daily_repo_counts(df=events_df)
    daily_top_repos = attach_date_range(
        df=compute_daily_topn(
            df=daily_repo_counts,
            metric_col="event_count",
            top_n=daily_top_n,
        ),
        start_date=start_date,
        end_date=end_date,
    )
    if explain:
        daily_top_repos.explain(True)
    return daily_top_repos
