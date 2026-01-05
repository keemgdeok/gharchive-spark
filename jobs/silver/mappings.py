from __future__ import annotations

from pyspark.sql import functions as F


def push_event_cols(*, commit_count_col):
    return [
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
    ]


def push_commit_cols():
    return [
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
    ]


def pull_request_cols():
    return [
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
    ]


def issues_cols():
    return [
        F.col("payload.action").alias("action"),
        F.col("payload.issue.id").alias("issue_id"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.title").alias("issue_title"),
        F.col("payload.issue.state").alias("issue_state"),
        F.col("payload.issue.user.login").alias("issue_user_login"),
        F.col("payload.issue.comments").alias("issue_comments"),
        F.col("payload.issue.created_at").alias("issue_created_at"),
        F.col("payload.issue.closed_at").alias("issue_closed_at"),
    ]


def issue_comment_cols():
    return [
        F.col("payload.action").alias("action"),
        F.col("payload.issue.id").alias("issue_id"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.title").alias("issue_title"),
        F.col("payload.issue.state").alias("issue_state"),
        F.col("payload.comment.id").alias("comment_id"),
        F.col("payload.comment.user.login").alias("comment_user_login"),
        F.col("payload.comment.created_at").alias("comment_created_at"),
    ]


def pull_request_review_cols():
    return [
        F.col("payload.action").alias("action"),
        F.col("payload.pull_request.id").alias("pull_request_id"),
        F.col("payload.pull_request.number").alias("pull_request_number"),
        F.col("payload.pull_request.title").alias("pull_request_title"),
        F.col("payload.pull_request.user.login").alias("pull_request_user_login"),
        F.col("payload.review.id").alias("review_id"),
        F.col("payload.review.state").alias("review_state"),
        F.col("payload.review.user.login").alias("review_user_login"),
        F.col("payload.review.submitted_at").alias("review_submitted_at"),
    ]


def pull_request_review_comment_cols():
    return [
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
    ]


def commit_comment_cols():
    return [
        F.col("payload.comment.id").alias("comment_id"),
        F.col("payload.comment.commit_id").alias("commit_id"),
        F.col("payload.comment.user.login").alias("comment_user_login"),
        F.col("payload.comment.created_at").alias("comment_created_at"),
    ]


def create_cols():
    return [
        F.col("payload.ref").alias("ref"),
        F.col("payload.ref_type").alias("ref_type"),
        F.col("payload.master_branch").alias("master_branch"),
        F.col("payload.description").alias("description"),
        F.col("payload.pusher_type").alias("pusher_type"),
    ]


def delete_cols():
    return [
        F.col("payload.ref").alias("ref"),
        F.col("payload.ref_type").alias("ref_type"),
        F.col("payload.pusher_type").alias("pusher_type"),
    ]


def discussion_cols():
    return [
        F.col("payload.action").alias("action"),
        F.col("payload.discussion.id").alias("discussion_id"),
        F.col("payload.discussion.title").alias("discussion_title"),
        F.col("payload.discussion.category.name").alias("discussion_category"),
    ]


def fork_cols():
    return [
        F.col("payload.forkee.id").alias("forkee_id"),
        F.col("payload.forkee.full_name").alias("forkee_full_name"),
        F.col("payload.forkee.owner.login").alias("forkee_owner_login"),
        F.col("payload.forkee.created_at").alias("forkee_created_at"),
    ]


def gollum_cols():
    return [
        F.size(F.col("payload.pages")).alias("page_count"),
        F.col("payload.pages").alias("pages"),
    ]


def member_cols():
    return [
        F.col("payload.action").alias("action"),
        F.col("payload.member.id").alias("member_id"),
        F.col("payload.member.login").alias("member_login"),
    ]


def release_cols():
    return [
        F.col("payload.action").alias("action"),
        F.col("payload.release.id").alias("release_id"),
        F.col("payload.release.tag_name").alias("release_tag_name"),
        F.col("payload.release.name").alias("release_name"),
        F.col("payload.release.draft").alias("release_draft"),
        F.col("payload.release.prerelease").alias("release_prerelease"),
        F.col("payload.release.created_at").alias("release_created_at"),
        F.col("payload.release.published_at").alias("release_published_at"),
        F.col("payload.release.author.login").alias("release_author_login"),
    ]


def watch_cols():
    return [
        F.col("payload.action").alias("action"),
    ]
