"""
S3/MinIO 엔드포인트 해석 유틸
- Container First: 기본값은 docker 네트워크(minio)
"""

from __future__ import annotations

import os

DEFAULT_DOCKER_ENDPOINT = "http://minio:9000"


def resolve_s3_endpoint() -> str:
    raw = os.getenv("S3_ENDPOINT", "").strip()
    return raw or DEFAULT_DOCKER_ENDPOINT
