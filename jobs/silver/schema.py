"""
Silver 변환에 필요한 입력 스키마 정의
- Challenge #3: 중첩/드리프트 대응을 위한 Superset 스키마
"""

from __future__ import annotations

from pyspark.sql import types as T

# 공통 이벤트 스키마(이벤트 API 공통 속성 기준)
EVENT_ACTOR_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("login", T.StringType(), True),
        T.StructField("display_login", T.StringType(), True),
        T.StructField("gravatar_id", T.StringType(), True),
        T.StructField("url", T.StringType(), True),
        T.StructField("avatar_url", T.StringType(), True),
    ]
)

EVENT_REPO_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("url", T.StringType(), True),
    ]
)

EVENT_ORG_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("login", T.StringType(), True),
        T.StructField("gravatar_id", T.StringType(), True),
        T.StructField("url", T.StringType(), True),
        T.StructField("avatar_url", T.StringType(), True),
    ]
)

EVENT_BASE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("type", T.StringType(), True),
        T.StructField("actor", EVENT_ACTOR_SCHEMA, True),
        T.StructField("repo", EVENT_REPO_SCHEMA, True),
        T.StructField("public", T.BooleanType(), True),
        T.StructField("created_at", T.StringType(), True),
        T.StructField("org", EVENT_ORG_SCHEMA, True),
    ]
)

# 타입별 payload 스키마(전용 트랙용)
PAYLOAD_USER_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("login", T.StringType(), True),
    ]
)

PUSH_COMMIT_SCHEMA = T.StructType(
    [
        T.StructField("sha", T.StringType(), True),
        T.StructField(
            "author",
            T.StructType(
                [
                    T.StructField("email", T.StringType(), True),
                    T.StructField("name", T.StringType(), True),
                ]
            ),
            True,
        ),
        T.StructField("message", T.StringType(), True),
        T.StructField("distinct", T.BooleanType(), True),
        T.StructField("url", T.StringType(), True),
    ]
)

PUSH_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("repository_id", T.LongType(), True),
        T.StructField("push_id", T.LongType(), True),
        T.StructField("size", T.IntegerType(), True),
        T.StructField("distinct_size", T.IntegerType(), True),
        T.StructField("ref", T.StringType(), True),
        T.StructField("head", T.StringType(), True),
        T.StructField("before", T.StringType(), True),
        T.StructField("commits", T.ArrayType(PUSH_COMMIT_SCHEMA), True),
    ]
)

PULL_REQUEST_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("number", T.LongType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("user", PAYLOAD_USER_SCHEMA, True),
        T.StructField("merged", T.BooleanType(), True),
        T.StructField("merged_at", T.StringType(), True),
        T.StructField("merge_commit_sha", T.StringType(), True),
        T.StructField("draft", T.BooleanType(), True),
        T.StructField(
            "base",
            T.StructType(
                [
                    T.StructField("ref", T.StringType(), True),
                    T.StructField(
                        "repo",
                        T.StructType(
                            [
                                T.StructField("full_name", T.StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        T.StructField(
            "head",
            T.StructType(
                [
                    T.StructField("ref", T.StringType(), True),
                    T.StructField(
                        "repo",
                        T.StructType(
                            [
                                T.StructField("full_name", T.StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        T.StructField("commits", T.IntegerType(), True),
        T.StructField("additions", T.IntegerType(), True),
        T.StructField("deletions", T.IntegerType(), True),
        T.StructField("changed_files", T.IntegerType(), True),
    ]
)

PULL_REQUEST_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField("number", T.LongType(), True),
        T.StructField("pull_request", PULL_REQUEST_SCHEMA, True),
    ]
)

ISSUE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("number", T.LongType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("user", PAYLOAD_USER_SCHEMA, True),
        T.StructField("comments", T.IntegerType(), True),
        T.StructField("created_at", T.StringType(), True),
        T.StructField("closed_at", T.StringType(), True),
    ]
)

ISSUES_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField("issue", ISSUE_SCHEMA, True),
    ]
)

ISSUE_COMMENT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("user", PAYLOAD_USER_SCHEMA, True),
        T.StructField("created_at", T.StringType(), True),
    ]
)

ISSUE_COMMENT_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField("issue", ISSUE_SCHEMA, True),
        T.StructField("comment", ISSUE_COMMENT_SCHEMA, True),
    ]
)

PULL_REQUEST_REVIEW_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("state", T.StringType(), True),
        T.StructField("user", PAYLOAD_USER_SCHEMA, True),
        T.StructField("submitted_at", T.StringType(), True),
    ]
)

PULL_REQUEST_REVIEW_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField("review", PULL_REQUEST_REVIEW_SCHEMA, True),
        T.StructField("pull_request", PULL_REQUEST_SCHEMA, True),
    ]
)

PULL_REQUEST_REVIEW_COMMENT_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType(), True),
        T.StructField("user", PAYLOAD_USER_SCHEMA, True),
        T.StructField("created_at", T.StringType(), True),
        T.StructField("path", T.StringType(), True),
        T.StructField("position", T.IntegerType(), True),
        T.StructField("commit_id", T.StringType(), True),
    ]
)

PULL_REQUEST_REVIEW_COMMENT_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField("comment", PULL_REQUEST_REVIEW_COMMENT_SCHEMA, True),
        T.StructField("pull_request", PULL_REQUEST_SCHEMA, True),
    ]
)

COMMIT_COMMENT_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField(
            "comment",
            T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                    T.StructField("commit_id", T.StringType(), True),
                    T.StructField("user", PAYLOAD_USER_SCHEMA, True),
                    T.StructField("created_at", T.StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

CREATE_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("ref", T.StringType(), True),
        T.StructField("ref_type", T.StringType(), True),
        T.StructField("master_branch", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("pusher_type", T.StringType(), True),
    ]
)

DELETE_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("ref", T.StringType(), True),
        T.StructField("ref_type", T.StringType(), True),
        T.StructField("pusher_type", T.StringType(), True),
    ]
)

DISCUSSION_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField(
            "discussion",
            T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                    T.StructField("title", T.StringType(), True),
                    T.StructField(
                        "category",
                        T.StructType(
                            [
                                T.StructField("name", T.StringType(), True),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

FORK_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField(
            "forkee",
            T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                    T.StructField("full_name", T.StringType(), True),
                    T.StructField("owner", PAYLOAD_USER_SCHEMA, True),
                    T.StructField("created_at", T.StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

GOLLUM_PAGE_SCHEMA = T.StructType(
    [
        T.StructField("page_name", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("action", T.StringType(), True),
        T.StructField("sha", T.StringType(), True),
    ]
)

GOLLUM_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("pages", T.ArrayType(GOLLUM_PAGE_SCHEMA), True),
    ]
)

MEMBER_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField("member", PAYLOAD_USER_SCHEMA, True),
    ]
)

RELEASE_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
        T.StructField(
            "release",
            T.StructType(
                [
                    T.StructField("id", T.LongType(), True),
                    T.StructField("tag_name", T.StringType(), True),
                    T.StructField("name", T.StringType(), True),
                    T.StructField("draft", T.BooleanType(), True),
                    T.StructField("prerelease", T.BooleanType(), True),
                    T.StructField("created_at", T.StringType(), True),
                    T.StructField("published_at", T.StringType(), True),
                    T.StructField("author", PAYLOAD_USER_SCHEMA, True),
                ]
            ),
            True,
        ),
    ]
)

WATCH_PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType(), True),
    ]
)
