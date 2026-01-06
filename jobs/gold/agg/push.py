"""
푸시 이벤트 집계
"""

from __future__ import annotations

from jobs.gold.io import build_silver_path, read_events_push
from jobs.gold.metrics import attach_date_range, compute_push_branch_ratio


def build_push_branch_ratio(
    *,
    spark,
    args,
    start_date,
    end_date,
    explain: bool,
):
    push_events_path = build_silver_path(
        args.bucket,
        args.silver_prefix,
        "events_push",
    )
    push_events_df = read_events_push(
        spark=spark,
        base_path=push_events_path,
        start_date=start_date,
        end_date=end_date,
    )
    push_branch_ratio = attach_date_range(
        df=compute_push_branch_ratio(df=push_events_df),
        start_date=start_date,
        end_date=end_date,
    )
    if explain:
        push_branch_ratio.explain(True)
    return push_branch_ratio
