"""
Silver 변환에 필요한 입력 스키마 로더
- Challenge #3: 중첩/드리프트 대응을 위한 Superset 스키마
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pyspark.sql import types as T

_SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"


@lru_cache(maxsize=None)
def _load_schema(name: str) -> T.StructType:
    schema_path = _SCHEMA_DIR / f"{name}.json"
    if not schema_path.exists():
        raise FileNotFoundError(f"스키마 파일이 없습니다: {schema_path}")
    with schema_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return T.StructType.fromJson(payload)


EVENT_ACTOR_SCHEMA = _load_schema("event_actor")
EVENT_REPO_SCHEMA = _load_schema("event_repo")
EVENT_ORG_SCHEMA = _load_schema("event_org")
EVENT_BASE_SCHEMA = _load_schema("event_base")

PAYLOAD_USER_SCHEMA = _load_schema("payload_user")
PUSH_COMMIT_SCHEMA = _load_schema("push_commit")
PUSH_PAYLOAD_SCHEMA = _load_schema("push_payload")
PULL_REQUEST_SCHEMA = _load_schema("pull_request")
PULL_REQUEST_PAYLOAD_SCHEMA = _load_schema("pull_request_payload")
ISSUE_SCHEMA = _load_schema("issue")
ISSUES_PAYLOAD_SCHEMA = _load_schema("issues_payload")
ISSUE_COMMENT_SCHEMA = _load_schema("issue_comment")
ISSUE_COMMENT_PAYLOAD_SCHEMA = _load_schema("issue_comment_payload")
PULL_REQUEST_REVIEW_SCHEMA = _load_schema("pull_request_review")
PULL_REQUEST_REVIEW_PAYLOAD_SCHEMA = _load_schema("pull_request_review_payload")
PULL_REQUEST_REVIEW_COMMENT_SCHEMA = _load_schema("pull_request_review_comment")
PULL_REQUEST_REVIEW_COMMENT_PAYLOAD_SCHEMA = _load_schema(
    "pull_request_review_comment_payload"
)
COMMIT_COMMENT_PAYLOAD_SCHEMA = _load_schema("commit_comment_payload")
CREATE_PAYLOAD_SCHEMA = _load_schema("create_payload")
DELETE_PAYLOAD_SCHEMA = _load_schema("delete_payload")
DISCUSSION_PAYLOAD_SCHEMA = _load_schema("discussion_payload")
FORK_PAYLOAD_SCHEMA = _load_schema("fork_payload")
GOLLUM_PAGE_SCHEMA = _load_schema("gollum_page")
GOLLUM_PAYLOAD_SCHEMA = _load_schema("gollum_payload")
MEMBER_PAYLOAD_SCHEMA = _load_schema("member_payload")
RELEASE_PAYLOAD_SCHEMA = _load_schema("release_payload")
WATCH_PAYLOAD_SCHEMA = _load_schema("watch_payload")
