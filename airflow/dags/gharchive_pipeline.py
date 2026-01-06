"""
GHArchive Daily Pipeline DAG
Bronze → Silver → Gold 메달리온 아키텍처 파이프라인

- Bronze: GHArchive .json.gz 수집 (Airflow 컨테이너에서 실행)
- Silver: SparkSubmitOperator 기반 Parquet 변환
- Gold: SparkSubmitOperator 기반 집계 마트 생성
"""

from __future__ import annotations

from datetime import datetime, timedelta
import subprocess

from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# DAG 기본 설정
default_args = {
    "owner": "gharchive",
    "depends_on_past": False,
    # Challenge #4: 운영 안정성 - 지수 백오프 재시도
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


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
    def build_hours_for_date(ds: str) -> list[str]:
        # 전날(ds) 기준 24시간 리스트 생성
        target = datetime.strptime(ds, "%Y-%m-%d").date()
        return [f"{target:%Y-%m-%d}-{hour:02d}" for hour in range(24)]

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
    silver_base = SparkSubmitOperator(
        task_id="silver_base",
        application="/opt/gharchive/jobs/silver/base.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}"],
        verbose=True,
    )

    # Silver → Gold 집계 마트 생성
    gold_base = SparkSubmitOperator(
        task_id="gold_base",
        application="/opt/gharchive/jobs/gold/base.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}"],
        verbose=True,
    )

    hours = build_hours_for_date("{{ ds }}")
    bronze_tasks = bronze_ingest.expand(hour=hours)
    bronze_tasks >> silver_base >> gold_base


dag = gharchive_daily()
