"""
Spark 런타임 유틸
- CLI 모드: SparkSession 새로 생성
- @task.pyspark 모드: 외부 주입된 SparkSession 사용
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def validate_runtime(spark: SparkSession) -> None:
    """S3A 설정 및 의존성 검증"""
    endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "").strip()
    if not endpoint:
        raise RuntimeError("spark.hadoop.fs.s3a.endpoint 설정이 없습니다")

    impl = spark.conf.get("spark.hadoop.fs.s3a.impl", "").strip()
    if "S3AFileSystem" not in impl:
        raise RuntimeError("spark.hadoop.fs.s3a.impl 설정이 올바르지 않습니다")

    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        raise RuntimeError(
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 환경 변수가 없습니다"
        )

    try:
        spark._jvm.java.lang.Class.forName(  # pyright: ignore[reportAttributeAccessIssue]
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        spark._jvm.java.lang.Class.forName(  # pyright: ignore[reportAttributeAccessIssue]
            "com.amazonaws.auth.AWSCredentialsProvider"
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("S3A 의존성(JAR) 로딩에 실패했습니다") from exc


def get_spark(app_name: str, spark: SparkSession | None = None) -> SparkSession:
    """
    SparkSession 획득

    Args:
        app_name: 애플리케이션 이름
        spark: 외부 주입된 SparkSession (None이면 새로 생성)

    Returns:
        검증된 SparkSession
    """
    if spark is None:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    validate_runtime(spark)
    return spark
