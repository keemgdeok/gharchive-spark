"""
GHArchive Daily Pipeline DAG
Bronze → Silver → Gold 메달리온 아키텍처 파이프라인

- Bronze: GHArchive .json.gz 수집 (@task로 실행)
- Silver: @task.pyspark 기반 Spark Job
- Gold: @task.pyspark 기반 Spark Job
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.apache.spark.decorators.pyspark import pyspark_task
from airflow.utils.trigger_rule import TriggerRule

# DAG 기본 설정
default_args = {
    "owner": "gharchive",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# Spark 런타임 설정 (spark-defaults.conf에서 나머지 설정 자동 로드)
# 참고: /opt/spark/conf/spark-defaults.conf는 docker-compose.yaml에서 마운트됨
SPARK_CONF = {
    # spark-defaults.conf의 spark.master를 override (동일하지만 명시적으로)
    "spark.master": "spark://spark-master:7077",
    # 런타임에 동적으로 결정되는 Driver 통신 설정
    "spark.driver.host": os.getenv("HOSTNAME", "airflow-scheduler"),
    "spark.driver.bindAddress": "0.0.0.0",
}


@dag(
    dag_id="gharchive_daily",
    default_args=default_args,
    description="GHArchive 일별 Medallion 파이프라인 (Bronze → Silver → Gold)",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gharchive", "medallion", "spark", "taskflow"],
)
def gharchive_daily():
    """GHArchive Daily Pipeline using @task.pyspark TaskFlow API"""

    @task
    def build_hours_for_date(target_date: str) -> list[str]:
        """대상 날짜의 24시간 리스트 생성"""
        return [f"{target_date}-{hour:02d}" for hour in range(24)]

    @task(pool="bronze_pool")
    def bronze_ingest(hour: str) -> None:
        """시간별 Bronze 데이터 수집"""
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

    @pyspark_task(config_kwargs=SPARK_CONF)
    def silver_base(target_date: str, spark=None) -> None:
        """Bronze → Silver 변환 (events_base + 멀티 트랙)"""
        from jobs.silver.base import process

        process(spark=spark, target_date=target_date)

    @pyspark_task(config_kwargs=SPARK_CONF)
    def check_schema_drift(target_date: str, spark=None) -> dict:
        """Silver 스키마 드리프트 감지"""
        from jobs.silver.schema_drift.cli import process

        return process(spark=spark, target_date=target_date)

    @task.branch
    def evaluate_drift(drift_result: dict) -> str:
        """
        드리프트 여부에 따라 분기
        - 드리프트 감지: log_drift_alert 실행
        - 드리프트 없음: run_gold_base로 직접 진행
        """
        if drift_result.get("drift_detected", False):
            return "log_drift_alert"
        return "run_gold_base"

    @task(task_id="log_drift_alert")
    def log_drift_alert(**context) -> None:
        """드리프트 알림 로깅 (XCom에서 데이터 읽기)"""
        from jobs.silver.schema_drift.alerter import send_drift_alert
        from jobs.silver.schema_drift.detector import (
            AggregatedDriftResult,
            SchemaDriftResult,
        )

        # XCom에서 check_schema_drift 결과 읽기
        ti = context["ti"]
        drift_result = ti.xcom_pull(task_ids="check_schema_drift")

        track_results = [
            SchemaDriftResult(**tr) for tr in drift_result.get("track_results", [])
        ]
        result = AggregatedDriftResult(
            target_date=drift_result["target_date"],
            track_results=track_results,
            drift_detected=drift_result["drift_detected"],
            total_new_variants=drift_result["total_new_variants"],
            max_failure_rate=drift_result["max_failure_rate"],
        )
        send_drift_alert(result)

    @pyspark_task(
        task_id="run_gold_base",
        config_kwargs=SPARK_CONF,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    def gold_base(target_date: str, spark=None) -> None:
        """Silver → Gold 집계 마트 생성"""
        from jobs.gold.base import process

        process(spark=spark, target_date=target_date)

    # DAG 흐름 정의
    ds = "{{ ds }}"
    hours = build_hours_for_date(target_date=ds)
    bronze_tasks = bronze_ingest.expand(hour=hours)

    silver_result = silver_base(target_date=ds)
    drift_result = check_schema_drift(target_date=ds)
    branch = evaluate_drift(drift_result)

    # log_drift_alert는 파라미터 없이 호출 (XCom으로 읽음)
    alert = log_drift_alert()
    gold = gold_base(target_date=ds)

    # 의존성 설정
    # Branch 패턴:
    # - 드리프트 감지: branch → alert → gold
    # - 드리프트 없음: branch → gold (alert skip)
    bronze_tasks >> silver_result >> drift_result >> branch
    branch >> [alert, gold]
    alert >> gold


dag = gharchive_daily()
