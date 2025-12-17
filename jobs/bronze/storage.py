"""
MinIO/S3A 입출력 유틸
"""

from __future__ import annotations

import asyncio
import logging
import os

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

DEFAULT_DOCKER_S3_ENDPOINT = "http://minio:9000"


def build_s3_client():
    endpoint = os.getenv("S3_ENDPOINT", "").strip() or DEFAULT_DOCKER_S3_ENDPOINT
    region = os.getenv("MINIO_REGION", "us-east-1")
    access_key = os.getenv(
        "AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin")
    )
    secret_key = os.getenv(
        "AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    )
    # MinIO 호환 path-style + v4 서명
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=BotoConfig(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_bucket(client, bucket: str, create_if_missing: bool) -> None:
    try:
        client.head_bucket(Bucket=bucket)
        return
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchBucket", "NotFound"):
            if not create_if_missing:
                raise
            logging.info("버킷 %s 이 없어 생성합니다", bucket)
            client.create_bucket(Bucket=bucket)
            return
        raise


async def object_exists(client, bucket: str, key: str) -> bool:
    def _head() -> bool:
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    return await asyncio.to_thread(_head)


async def upload_bytes(
    client,
    bucket: str,
    key: str,
    data: bytes,
) -> None:
    await asyncio.to_thread(
        client.put_object,
        Bucket=bucket,
        Key=key,
        Body=data,
        ContentType="application/json",
        ContentEncoding="gzip",
        Metadata={"source": "gharchive", "stage": "bronze"},
    )
