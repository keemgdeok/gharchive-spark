"""
PR 라이프사이클 집계
"""

from __future__ import annotations

from jobs.gold.io import (
    build_silver_path,
    read_events_pull_request,
    read_events_pull_request_review,
    read_events_pull_request_review_comment,
)
from jobs.gold.metrics import (
    attach_date_range,
    build_pull_request_base,
    compute_pr_review_comment_stats,
    compute_pr_review_latency_metrics,
    compute_pr_review_stats,
)


def build_pr_review_latency(
    *,
    spark,
    args,
    start_date,
    end_date,
    explain: bool,
):
    pr_events_path = build_silver_path(
        args.bucket,
        args.silver_prefix,
        "events_pull_request",
    )
    pr_reviews_path = build_silver_path(
        args.bucket,
        args.silver_prefix,
        "events_pull_request_review",
    )
    pr_review_comments_path = build_silver_path(
        args.bucket,
        args.silver_prefix,
        "events_pull_request_review_comment",
    )
    pr_events_df = read_events_pull_request(
        spark=spark,
        base_path=pr_events_path,
        start_date=start_date,
        end_date=end_date,
    )
    pr_reviews_df = read_events_pull_request_review(
        spark=spark,
        base_path=pr_reviews_path,
        start_date=start_date,
        end_date=end_date,
    )
    pr_review_comments_df = read_events_pull_request_review_comment(
        spark=spark,
        base_path=pr_review_comments_path,
        start_date=start_date,
        end_date=end_date,
    )
    pr_base = build_pull_request_base(pr_events=pr_events_df)
    pr_review_stats = compute_pr_review_stats(pr_reviews=pr_reviews_df)
    pr_review_comment_stats = compute_pr_review_comment_stats(
        pr_review_comments=pr_review_comments_df
    )
    pr_review_latency = attach_date_range(
        df=compute_pr_review_latency_metrics(
            pr_base=pr_base,
            review_stats=pr_review_stats,
            review_comment_stats=pr_review_comment_stats,
        ),
        start_date=start_date,
        end_date=end_date,
    )
    if explain:
        pr_review_latency.explain(True)
    return pr_review_latency
