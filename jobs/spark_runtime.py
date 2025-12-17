"""
Spark 런타임 유틸 (Docker-only)
- 설정은 spark-defaults.conf(SoT)로 고정
- 여기서는 최소 검증만 수행
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def validate_runtime(spark: SparkSession) -> None:
    # S3A 설정/크리덴셜/JAR 로딩 더블 체크
    endpoint = spark.conf.get("spark.hadoop.fs.s3a.endpoint", "").strip()
    if not endpoint:
        raise RuntimeError(
            "spark.hadoop.fs.s3a.endpoint 설정이 없습니다(docker/spark/conf/spark-defaults.conf 확인)"
        )

    impl = spark.conf.get("spark.hadoop.fs.s3a.impl", "").strip()
    if "S3AFileSystem" not in impl:
        raise RuntimeError(
            "spark.hadoop.fs.s3a.impl 설정이 올바르지 않습니다(docker/spark/conf/spark-defaults.conf 확인)"
        )

    if not os.getenv("AWS_ACCESS_KEY_ID") or not os.getenv("AWS_SECRET_ACCESS_KEY"):
        raise RuntimeError(
            "AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY 환경 변수가 없습니다(docker-compose.yaml 확인)"
        )

    try:
        spark._jvm.java.lang.Class.forName(  # pyright: ignore[reportAttributeAccessIssue]
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        spark._jvm.java.lang.Class.forName(  # pyright: ignore[reportAttributeAccessIssue]
            "com.amazonaws.auth.AWSCredentialsProvider"
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "S3A 의존성(JAR) 로딩에 실패했습니다(docker/spark/Dockerfile의 hadoop-aws/aws sdk 버전 확인)"
        ) from exc


def get_spark(app_name: str) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    validate_runtime(spark)
    return spark
