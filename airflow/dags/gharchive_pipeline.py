"""
GHArchive Daily Pipeline DAG
Bronze → Silver → Gold 메달리온 아키텍처 파이프라인

- Bronze: GHArchive .json.gz 수집 (Airflow 컨테이너에서 실행)
- Silver: DockerOperator 기반 Spark Job (spark-master에서 실행)
- Gold: DockerOperator 기반 Spark Job (spark-master에서 실행)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
import subprocess

from airflow.decorators import dag, task
from airflow.providers.docker.operators.docker import DockerOperator

# DAG 기본 설정
default_args = {
    "owner": "gharchive",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Spark DockerOperator 설정 (환경변수에서 주입)
SPARK_IMAGE = os.getenv("SPARK_IMAGE", "gharchive-spark-spark-master:latest")
SPARK_NETWORK = f"{os.getenv('COMPOSE_PROJECT_NAME', 'gharchive-spark')}_gh-net"

# S3A 인증용 환경변수 (spark-defaults.conf의 EnvironmentVariableCredentialsProvider가 사용)
SPARK_ENV = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
}


def spark_submit_command(script_path: str, date_arg: str) -> str:
    """spark-submit 명령어 (설정은 spark-defaults.conf에서 로드, driver 통신만 런타임 지정)"""
    return f"/bin/bash -c '/opt/spark/bin/spark-submit --conf spark.driver.host=$(hostname -i) --conf spark.driver.bindAddress=0.0.0.0 {script_path} --date {date_arg}'"


@dag(
    dag_id="gharchive_daily",
    default_args=default_args,
    description="GHArchive 일별 Medallion 파이프라인 (Bronze → Silver → Gold)",
    schedule="0 2 * * *",  # 매일 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gharchive", "medallion", "spark"],
)
def gharchive_daily():
    @task
    def build_hours_for_date(target_date: str) -> list[str]:
        # 전날(target_date) 기준 24시간 리스트 생성
        return [f"{target_date}-{hour:02d}" for hour in range(24)]

    @task(pool="bronze_pool")
    def bronze_ingest(hour: str) -> None:
        # Airflow 컨테이너에서 시간별 브론즈 수집
        subprocess.run(
            [
                "python3",
                "-m",
                "jobs.bronze.ingest",
                "--hour",
                hour,
                "--concurrency",
                "1",
            ],
            check=True,
        )

    # Bronze JSON → Silver Parquet 변환
    silver_base = DockerOperator(
        task_id="silver_base",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=SPARK_NETWORK,
        hostname="silver_base",
        environment=SPARK_ENV,
        command=spark_submit_command(
            "/opt/gharchive/jobs/silver/base.py",
            "{{ ds }}",
        ),
        mount_tmp_dir=False,
    )

    # Silver 완료 후 스키마 드리프트 감지 (DockerOperator)
    check_schema_drift = DockerOperator(
        task_id="check_schema_drift",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=SPARK_NETWORK,
        hostname="check_schema_drift",
        environment=SPARK_ENV,
        command=spark_submit_command(
            "/opt/gharchive/jobs/silver/schema_drift/cli.py",
            "{{ ds }}",
        ),
        mount_tmp_dir=False,
    )

    @task.branch
    def evaluate_drift(**context) -> str:
        """
        드리프트 여부에 따라 분기
        S3에 저장된 드리프트 결과 JSON을 읽어서 분기 결정
        - 드리프트 감지: log_drift_alert 실행 후 gold_base로
        - 드리프트 없음: gold_base로 직접 진행
        """
        import boto3
        import json

        # S3에서 결과 읽기
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        s3 = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("MINIO_REGION", "us-east-1"),
        )

        target_date = context["ds"]
        key = f"metadata/schema_drift_results/{target_date}.json"

        try:
            response = s3.get_object(Bucket="gharchive", Key=key)
            content = response["Body"].read().decode("utf-8")
            drift_results = json.loads(content)

            if drift_results.get("drift_detected", False):
                return "log_drift_alert"
        except Exception:  # noqa: BLE001
            # 결과 파일이 없거나 읽기 실패 시 Gold로 진행
            pass

        return "gold_base"

    @task
    def log_drift_alert(**context) -> None:
        """드리프트 알림 로깅 및 Airflow Variable 기록"""
        import boto3
        import json
        from jobs.silver.schema_drift.alerter import send_drift_alert
        from jobs.silver.schema_drift.detector import (
            AggregatedDriftResult,
            SchemaDriftResult,
        )

        # S3에서 결과 읽기
        s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        s3 = boto3.client(
            "s3",
            endpoint_url=s3_endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("MINIO_REGION", "us-east-1"),
        )

        target_date = context["ds"]
        key = f"metadata/schema_drift_results/{target_date}.json"

        try:
            response = s3.get_object(Bucket="gharchive", Key=key)
            content = response["Body"].read().decode("utf-8")
            drift_results = json.loads(content)
        except Exception:  # noqa: BLE001
            return

        # dict를 다시 객체로 변환
        track_results = [
            SchemaDriftResult(**tr) for tr in drift_results.get("track_results", [])
        ]
        result = AggregatedDriftResult(
            target_date=drift_results["target_date"],
            track_results=track_results,
            drift_detected=drift_results["drift_detected"],
            total_new_variants=drift_results["total_new_variants"],
            max_failure_rate=drift_results["max_failure_rate"],
        )
        send_drift_alert(result)

    # Silver → Gold 집계 마트 생성
    gold_base = DockerOperator(
        task_id="gold_base",
        image=SPARK_IMAGE,
        api_version="auto",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=SPARK_NETWORK,
        hostname="gold_base",
        environment=SPARK_ENV,
        command=spark_submit_command(
            "/opt/gharchive/jobs/gold/base.py",
            "{{ ds }}",
        ),
        mount_tmp_dir=False,
        trigger_rule="none_failed_min_one_success",
    )

    # DAG 흐름 정의
    hours = build_hours_for_date(target_date="{{ ds }}")
    bronze_tasks = bronze_ingest.expand(hour=hours)

    branch_decision = evaluate_drift()
    alert_task = log_drift_alert()

    bronze_tasks >> silver_base >> check_schema_drift >> branch_decision
    branch_decision >> alert_task >> gold_base
    branch_decision >> gold_base


dag = gharchive_daily()
