from __future__ import annotations

from pyspark.sql import functions as F

from jobs.silver.events_base import base_track_columns, with_payload
from jobs.silver.schema import (
    COMMIT_COMMENT_PAYLOAD_SCHEMA,
    CREATE_PAYLOAD_SCHEMA,
    DELETE_PAYLOAD_SCHEMA,
    DISCUSSION_PAYLOAD_SCHEMA,
    FORK_PAYLOAD_SCHEMA,
    GOLLUM_PAYLOAD_SCHEMA,
    ISSUE_COMMENT_PAYLOAD_SCHEMA,
    ISSUES_PAYLOAD_SCHEMA,
    MEMBER_PAYLOAD_SCHEMA,
    PULL_REQUEST_PAYLOAD_SCHEMA,
    PULL_REQUEST_REVIEW_COMMENT_PAYLOAD_SCHEMA,
    PULL_REQUEST_REVIEW_PAYLOAD_SCHEMA,
    PUSH_PAYLOAD_SCHEMA,
    RELEASE_PAYLOAD_SCHEMA,
    WATCH_PAYLOAD_SCHEMA,
)


def transform_push_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PushEvent",
        payload_schema=PUSH_PAYLOAD_SCHEMA,
    )
    commit_count_raw = F.size(F.col("payload.commits"))
    commit_count_col = F.when(commit_count_raw < 0, F.lit(0)).otherwise(
        commit_count_raw
    )
    variant_col = (
        F.when(F.col("payload").isNull(), F.lit("parse_fail"))
        .when(
            F.col("payload.size").isNotNull()
            | F.col("payload.distinct_size").isNotNull()
            | F.col("payload.commits").isNotNull(),
            F.lit("v1"),
        )
        .otherwise(F.lit("v2"))
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        variant_col.alias("push_payload_variant"),
        F.col("payload.repository_id").alias("repository_id"),
        F.col("payload.push_id").alias("push_id"),
        F.col("payload.ref").alias("ref"),
        F.col("payload.head").alias("head"),
        F.col("payload.before").alias("before"),
        F.coalesce(F.col("payload.size"), F.lit(0)).alias("push_size"),
        F.coalesce(F.col("payload.distinct_size"), F.lit(0)).alias(
            "push_distinct_size"
        ),
        commit_count_col.alias("commit_count"),
    )


def transform_push_commits(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PushEvent",
        payload_schema=PUSH_PAYLOAD_SCHEMA,
    )
    return (
        df.withColumn("payload_parse_ok", F.col("payload").isNotNull())
        .withColumn("commit", F.explode_outer(F.col("payload.commits")))
        .select(
            *base_track_columns(),
            F.col("payload_parse_ok"),
            F.col("payload.repository_id").alias("repository_id"),
            F.col("payload.push_id").alias("push_id"),
            F.col("payload.size").alias("push_size"),
            F.col("payload.distinct_size").alias("push_distinct_size"),
            F.col("commit.sha").alias("commit_sha"),
            F.col("commit.author.email").alias("commit_author_email"),
            F.col("commit.author.name").alias("commit_author_name"),
            F.col("commit.message").alias("commit_message"),
            F.col("commit.distinct").alias("commit_distinct"),
            F.col("commit.url").alias("commit_url"),
        )
        .filter(F.col("commit_sha").isNotNull())
    )


def transform_pull_request_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PullRequestEvent",
        payload_schema=PULL_REQUEST_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.coalesce(
            F.col("payload.number"),
            F.col("payload.pull_request.number"),
        ).alias("pull_request_number"),
        F.col("payload.pull_request.id").alias("pull_request_id"),
        F.col("payload.pull_request.state").alias("pull_request_state"),
        F.col("payload.pull_request.title").alias("pull_request_title"),
        F.col("payload.pull_request.user.login").alias("pull_request_user_login"),
        F.col("payload.pull_request.merged").alias("pull_request_merged"),
        F.col("payload.pull_request.merged_at").alias("pull_request_merged_at"),
        F.col("payload.pull_request.merge_commit_sha").alias(
            "pull_request_merge_commit_sha"
        ),
        F.col("payload.pull_request.draft").alias("pull_request_draft"),
        F.col("payload.pull_request.base.ref").alias("base_ref"),
        F.col("payload.pull_request.head.ref").alias("head_ref"),
        F.col("payload.pull_request.base.repo.full_name").alias("base_repo_full_name"),
        F.col("payload.pull_request.head.repo.full_name").alias("head_repo_full_name"),
        F.col("payload.pull_request.commits").alias("commit_count"),
        F.col("payload.pull_request.additions").alias("additions"),
        F.col("payload.pull_request.deletions").alias("deletions"),
        F.col("payload.pull_request.changed_files").alias("changed_files"),
    )


def transform_issues_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="IssuesEvent",
        payload_schema=ISSUES_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.issue.id").alias("issue_id"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.title").alias("issue_title"),
        F.col("payload.issue.state").alias("issue_state"),
        F.col("payload.issue.user.login").alias("issue_user_login"),
        F.col("payload.issue.comments").alias("issue_comments"),
        F.col("payload.issue.created_at").alias("issue_created_at"),
        F.col("payload.issue.closed_at").alias("issue_closed_at"),
    )


def transform_issue_comment_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="IssueCommentEvent",
        payload_schema=ISSUE_COMMENT_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.issue.id").alias("issue_id"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.title").alias("issue_title"),
        F.col("payload.issue.state").alias("issue_state"),
        F.col("payload.comment.id").alias("comment_id"),
        F.col("payload.comment.user.login").alias("comment_user_login"),
        F.col("payload.comment.created_at").alias("comment_created_at"),
    )


def transform_pull_request_review_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PullRequestReviewEvent",
        payload_schema=PULL_REQUEST_REVIEW_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.pull_request.id").alias("pull_request_id"),
        F.col("payload.pull_request.number").alias("pull_request_number"),
        F.col("payload.pull_request.title").alias("pull_request_title"),
        F.col("payload.pull_request.user.login").alias("pull_request_user_login"),
        F.col("payload.review.id").alias("review_id"),
        F.col("payload.review.state").alias("review_state"),
        F.col("payload.review.user.login").alias("review_user_login"),
        F.col("payload.review.submitted_at").alias("review_submitted_at"),
    )


def transform_pull_request_review_comment_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="PullRequestReviewCommentEvent",
        payload_schema=PULL_REQUEST_REVIEW_COMMENT_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.pull_request.id").alias("pull_request_id"),
        F.col("payload.pull_request.number").alias("pull_request_number"),
        F.col("payload.pull_request.title").alias("pull_request_title"),
        F.col("payload.comment.id").alias("comment_id"),
        F.col("payload.comment.user.login").alias("comment_user_login"),
        F.col("payload.comment.created_at").alias("comment_created_at"),
        F.col("payload.comment.path").alias("comment_path"),
        F.col("payload.comment.position").alias("comment_position"),
        F.col("payload.comment.commit_id").alias("comment_commit_id"),
    )


def transform_commit_comment_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="CommitCommentEvent",
        payload_schema=COMMIT_COMMENT_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.comment.id").alias("comment_id"),
        F.col("payload.comment.commit_id").alias("commit_id"),
        F.col("payload.comment.user.login").alias("comment_user_login"),
        F.col("payload.comment.created_at").alias("comment_created_at"),
    )


def transform_create_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="CreateEvent",
        payload_schema=CREATE_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.ref").alias("ref"),
        F.col("payload.ref_type").alias("ref_type"),
        F.col("payload.master_branch").alias("master_branch"),
        F.col("payload.description").alias("description"),
        F.col("payload.pusher_type").alias("pusher_type"),
    )


def transform_delete_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="DeleteEvent",
        payload_schema=DELETE_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.ref").alias("ref"),
        F.col("payload.ref_type").alias("ref_type"),
        F.col("payload.pusher_type").alias("pusher_type"),
    )


def transform_discussion_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="DiscussionEvent",
        payload_schema=DISCUSSION_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.discussion.id").alias("discussion_id"),
        F.col("payload.discussion.title").alias("discussion_title"),
        F.col("payload.discussion.category.name").alias("discussion_category"),
    )


def transform_fork_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="ForkEvent",
        payload_schema=FORK_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.forkee.id").alias("forkee_id"),
        F.col("payload.forkee.full_name").alias("forkee_full_name"),
        F.col("payload.forkee.owner.login").alias("forkee_owner_login"),
        F.col("payload.forkee.created_at").alias("forkee_created_at"),
    )


def transform_gollum_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="GollumEvent",
        payload_schema=GOLLUM_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.size(F.col("payload.pages")).alias("page_count"),
        F.col("payload.pages").alias("pages"),
    )


def transform_member_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="MemberEvent",
        payload_schema=MEMBER_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.member.id").alias("member_id"),
        F.col("payload.member.login").alias("member_login"),
    )


def transform_public_events(*, base_df):
    return base_df.filter(F.col("event_type") == F.lit("PublicEvent")).select(
        *base_track_columns(),
        F.lit(True).alias("payload_parse_ok"),
    )


def transform_release_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="ReleaseEvent",
        payload_schema=RELEASE_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
        F.col("payload.release.id").alias("release_id"),
        F.col("payload.release.tag_name").alias("release_tag_name"),
        F.col("payload.release.name").alias("release_name"),
        F.col("payload.release.draft").alias("release_draft"),
        F.col("payload.release.prerelease").alias("release_prerelease"),
        F.col("payload.release.created_at").alias("release_created_at"),
        F.col("payload.release.published_at").alias("release_published_at"),
        F.col("payload.release.author.login").alias("release_author_login"),
    )


def transform_watch_events(*, base_df):
    df = with_payload(
        base_df=base_df,
        event_type="WatchEvent",
        payload_schema=WATCH_PAYLOAD_SCHEMA,
    )
    return df.withColumn("payload_parse_ok", F.col("payload").isNotNull()).select(
        *base_track_columns(),
        F.col("payload_parse_ok"),
        F.col("payload.action").alias("action"),
    )


TRACK_BUILDERS = [
    ("events_push", transform_push_events),
    ("push_commits", transform_push_commits),
    ("events_pull_request", transform_pull_request_events),
    ("events_issues", transform_issues_events),
    ("events_issue_comment", transform_issue_comment_events),
    ("events_pull_request_review", transform_pull_request_review_events),
    (
        "events_pull_request_review_comment",
        transform_pull_request_review_comment_events,
    ),
    ("events_commit_comment", transform_commit_comment_events),
    ("events_create", transform_create_events),
    ("events_delete", transform_delete_events),
    ("events_discussion", transform_discussion_events),
    ("events_fork", transform_fork_events),
    ("events_gollum", transform_gollum_events),
    ("events_member", transform_member_events),
    ("events_public", transform_public_events),
    ("events_release", transform_release_events),
    ("events_watch", transform_watch_events),
]


def build_tracks(*, base_df):
    return [(label, builder(base_df=base_df)) for label, builder in TRACK_BUILDERS]
