import os
from pyspark.sql import SparkSession


# 한국어 주석: MinIO(S3A) 설정을 묶어 SparkSession을 생성
# 재사용을 위해 모듈화
# TODO: 필요 시 파티션/메모리 파라미터를 인자로 노출


def get_spark(app_name: str = "gh-archive") -> SparkSession:
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
    bucket_region = os.getenv("MINIO_REGION", "us-east-1")
    access_key = os.getenv(
        "AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minioadmin")
    )
    secret_key = os.getenv(
        "AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    )

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.master", os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        )
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.region", bucket_region)
        .config("spark.sql.adaptive.enabled", "true")
    )

    # 자격 증명 주입 (로컬/테스트 편의)
    if access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", access_key)
    if secret_key:
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", secret_key)

    return builder.getOrCreate()
