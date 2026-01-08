<div align="center">

# <code>gharchive-spark</code>

<em>GHArchive 로그를 Spark + MinIO로 처리하는 Medallion 기반 데이터 레이크 파이프라인</em>

<em>Built with the tools and technologies:</em>

<img src="https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C?style=flat&logo=apachespark&logoColor=white" alt="Apache Spark">
<img src="https://img.shields.io/badge/PySpark-3.5.7-E25A1C?style=flat&logo=apachespark&logoColor=white" alt="PySpark">
<img src="https://img.shields.io/badge/Hadoop-3.3.4-66CCFF?style=flat&logo=apachehadoop&logoColor=black" alt="Hadoop">
<img src="https://img.shields.io/badge/MinIO-S3%20Compatible-000000?style=flat&logo=minio&logoColor=white" alt="MinIO">
<img src="https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?style=flat&logo=apacheairflow&logoColor=white" alt="Apache Airflow">
<br>
<img src="https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white" alt="Docker">
<img src="https://img.shields.io/badge/Python-3.10%2B-3776AB?style=flat&logo=python&logoColor=white" alt="Python">
<img src="https://img.shields.io/badge/pre--commit-FAB040?style=flat&logo=pre-commit&logoColor=black" alt="pre-commit">
<img src="https://img.shields.io/badge/Ruff-000000?style=flat&logo=ruff&logoColor=white" alt="Ruff">
<img src="https://img.shields.io/badge/Mypy-2A6DB2?style=flat&logo=python&logoColor=white" alt="Mypy">

</div>
<br>

______________________________________________________________________

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Key Directories](#key-directories)
- [Quick Start](#quick-start)
- [Common Commands](#common-commands)
- [Troubleshooting Log](#troubleshooting-log)
- [Testing & Quality Gates](#testing--quality-gates)

<br>

______________________________________________________________________

## Architecture


![GHArchive Spark Architecture](assets/gharchive-spark.svg)


**Flow** <br>
Bronze(Raw JSON) → Silver(Curated Parquet) → Gold(Aggregated Marts)


### Technical concerns

설계 결정 및 트러블슈팅 상세 내용 **[Technical Concern](https://versed-racer-357.notion.site/Technical-Concern-2c6cd94d4b5e80f6bd39f19fc5e1be53?source=copy_link)** 참고

<br>

______________________________________________________________________

## Features

| Component | Details |
| :-- | :-- |
| Architecture | <ul><li>Spark/MinIO/History Server</li><li>Medallion Architecture (Bronze/Silver/Gold)</li><li>S3A 연동 로컬 S3 호환 레이크</li></ul> |
| Bronze | <ul><li>aiohttp 비동기 다운로드</li><li>재시도/타임아웃/멱등 업로드</li><li>`bronze/YYYY/MM/DD/` 적재</li></ul> |
| Silver | <ul><li>payload_raw 보존 + Superset 스키마로 드리프트 대응</li><li>explode/col 중첩 스키마 해제</li><li>`partitionBy("dt")` Parquet 저장</li></ul> |
| Performance | <ul><li>AQE 활성화</li><li>`spark.sql.shuffle.partitions` 튜닝</li><li>Small File: coalesce/repartition</li><li>Data Skew: salting/broadcast join</li></ul> |
| Gold | <ul><li>top_repos/event_type/top_repo_event_types 집계</li><li>daily_top_repos(window), push_branch_ratio(master/main 비율) 추가</li><li>parquet/csv 출력</li><li>coalesce로 단일 파일 생성</li></ul> |
| Observability | <ul><li>Spark UI(4040-4050)</li><li>Spark History Server(18080)</li><li>Event Log 보존 정책</li></ul> |
| Orchestration | <ul><li>Airflow 2.10 (8088)</li><li>`@task.pyspark` TaskFlow API</li><li>Bronze→Silver→Gold DAG</li><li>Schema Drift 감지 및 분기</li><li>Backfill/Retry/Alerting</li></ul> |
| Quality | <ul><li>ruff + mypy + pre-commit</li><li>Docker 기반 재현</li></ul> |

<br>

______________________________________________________________________

## Key Directories

| Path | Purpose |
| :-- | :-- |
| `docker-compose.yaml` | Spark/MinIO/History Server/Airflow 인프라 정의 |
| `docker/spark/` | Spark 이미지 빌드 및 S3A JAR 포함 |
| `docker/spark/conf/` | `spark-defaults.conf`, `spark-env.sh`, `log4j.properties` |
| `docker/airflow/` | Airflow 이미지 (Spark Provider + PySpark, `@task.pyspark` 사용) |
| `airflow/dags/` | Airflow DAG 파일 |
| `jobs/bronze/` | GHArchive .json.gz 수집<br>비동기 다운로드/재시도/멱등 업로드<br>bronze 경로 적재 |
| `jobs/silver/` | 이벤트 정규화<br>중첩 해제/트랙 변환<br>스키마 드리프트 대응 |
| `jobs/gold/` | Gold 집계 마트 생성<br>Skew/Broadcast 시나리오 포함<br>parquet/csv 출력 |
| `jobs/spark_runtime.py` | S3A 설정/크리덴셜/JAR 검증<br>외부 SparkSession 주입 지원 (`@task.pyspark`) |
| `data/samples/schema-drift/` | 스키마 드리프트 샘플 |

<br>

______________________________________________________________________

## Quick Start

### Prerequisites

- Docker Engine + Docker Compose v2

### Environment setup

1. **.env 생성**
   ```bash
   cat <<'EOF' > .env
   SPARK_VERSION=3.5.7
   HADOOP_VERSION=3
   HADOOP_AWS_VERSION=3.3.4
   AWS_SDK_VERSION=1.12.262
   PYTHON_VERSION=3.10

   SPARK_MASTER_HOST=spark-master
   SPARK_MASTER_PORT=7077
   SPARK_MASTER_WEBUI=8080
   SPARK_WORKER_CORES=2
   SPARK_WORKER_MEMORY=3G
   SPARK_HISTORY_PORT=18080
   SPARK_DRIVER_MEMORY=2G
   SPARK_EXECUTOR_MEMORY=2G
   SPARK_EXECUTOR_CORES=2
   SPARK_LOCAL_DIR=/opt/spark/work-dir

   MINIO_ROOT_USER=minioadmin
   MINIO_ROOT_PASSWORD=minioadmin
   MINIO_REGION=us-east-1
   MINIO_PORT=9000
   MINIO_CONSOLE_PORT=9090
   MINIO_BUCKET=gharchive
   S3_ENDPOINT=http://minio:9000

   COMPOSE_PROJECT_NAME=gharchive-spark
   EOF
   ```

2. **Docker Compose 실행**
   ```bash
   docker compose up -d --build
   docker compose ps
   ```

3. **Bronze 수집**
   ```bash
   # 기본 S3 엔드포인트 http://minio:9000
   docker compose exec -T spark-master \
     python3 -m jobs.bronze.ingest \
     --hour 2024-05-21-00 --concurrency 1
   ```

4. **Silver 변환**
   ```bash
   docker compose exec -T spark-master \
     /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /opt/gharchive/jobs/silver/base.py \
     --hour 2024-05-21-00
   ```

5. **Gold 집계**
   ```bash
   docker compose exec -T spark-master \
     /opt/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     /opt/gharchive/jobs/gold/base.py \
     --date 2024-05-21
   ```

6. **Airflow DAG 실행 (권장)**
   ```bash
   # DAG 트리거
   docker compose exec airflow-scheduler \
     airflow dags trigger gharchive_daily --exec-date "2024-05-21"

   # 태스크 상태 확인
   docker compose exec airflow-scheduler \
     airflow tasks states-for-dag-run gharchive_daily "manual__2024-05-21T00:00:00+00:00"
   ```

7. **UI 접근**
    ```text
    Airflow UI: http://localhost:8088 (ID/PW: airflow/airflow)
    Spark Master: http://localhost:8080
    Spark Worker: http://localhost:8081, http://localhost:8082, http://localhost:8083
    Spark UI(실행 중 Driver): http://localhost:4040 (점유 시 4041, 4042 ...)
    Spark History Server: http://localhost:18080/?showIncomplete=true
    MinIO Console: http://localhost:9090
    ```

<br>

______________________________________________________________________

## Common Commands

#### Infra

```bash

docker compose up -d --build # 빌드/기동
docker compose ps # 상태 확인
docker compose logs -f spark-master # 로그 확인
docker compose down # 종료
```

#### S3A smoke test

```bash
# pyspark 접속
docker compose exec -T spark-master /opt/spark/bin/pyspark \
  --master spark://spark-master:7077
```

```python
# S3A read/write 확인
spark.range(5).write.mode("overwrite").parquet("s3a://gharchive/_smoketest/spark-3.5.7/")
spark.read.parquet("s3a://gharchive/_smoketest/spark-3.5.7/").count()
```

#### Bronze ingestion (Airflow 컨테이너 기준)

```bash
# --date 24시간 전체 처리
docker compose exec -T airflow-webserver \
  python3 -m jobs.bronze.ingest --date 2024-05-21 --concurrency 8

# --hour 단일 시간 처리
docker compose exec -T airflow-webserver \
  python3 -m jobs.bronze.ingest --hour 2024-05-21-00 --concurrency 1
```

#### Silver base

```bash
# --verbose DEBUG 로그 출력
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/gharchive/jobs/silver/base.py --date 2024-05-21 --verbose
```

#### Gold base

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/gharchive/jobs/gold/base.py --date 2024-05-21 \
  --daily-top-n 10
```

<br>

______________________________________________________________________

## Troubleshooting Log

<details>
  <summary>Schema Drift (스키마 드리프트)</summary>

  | Key | Value |
  | :-- | :-- |
  | Problem | 2025-10-09 이후 PushEvent payload 필드(size/distinct_size/commits) 누락 발생 |
  | Cause | GHArchive 업스트림에서 PushEvent 스키마 변경 (v1→v2) |
  | Solution | `payload_raw` 보존 + Superset 스키마(`jobs/silver/schemas/*.json`)로 `from_json` 파싱 |
  | Validation | `payload_parse_ok`/`payload_variant` 분포로 파싱 성공률 확인 |
</details>

<details>
  <summary>Nested Schema (중첩 스키마)</summary>

  | Key | Value |
  | :-- | :-- |
  | Problem | payload 중첩 구조(배열/오브젝트)로 직접 분석 어려움 |
  | Cause | GHArchive JSON 구조가 이벤트 타입별로 다른 중첩 깊이 보유 |
  | Solution | `with_payload`로 구조체 파싱 → `col("payload.*")` 전개, commits는 `explode_outer` 적용 |
  | Validation | `commit_sha` null 필터, `--verbose` `explain(True)`로 변환 확인 |
</details>

<details>
  <summary>Small File (작은 파일)</summary>

  | Key | Value |
  | :-- | :-- |
  | Problem | `partitionBy("dt")` 저장 시 작은 파일이 다량 생성됨 |
  | Cause | 기본 `spark.sql.shuffle.partitions`(200)이 데이터 크기 대비 과다 |
  | Solution | AQE `coalescePartitions` 활성화 + Silver/Gold에서 `--coalesce` 옵션 사용 |
  | Validation | `write partitions` 로그로 파일 수 전후 비교 |
</details>

<details>
  <summary>Remote Driver OOM (@task.pyspark)</summary>

  | Key | Value |
  | :-- | :-- |
  | Problem | `@task.pyspark` 마이그레이션 시 Executor `OutOfMemoryError` 발생 |
  | Cause | 원격 Driver 아키텍처(Airflow↔Worker)에서 Java 직렬화 오버헤드 발생 |
  | Solution | 1) Java 직렬화 사용<br>2) `spark.executor.memory` 1G→2G<br>3) `SPARK_WORKER_MEMORY` 3G 이상<br>4) `spark.sql.shuffle.partitions` 18→64 |
  | Validation | Silver/Gold 전체 파이프라인 테스트 성공 |
</details>

<br>

______________________________________________________________________

## Testing & Quality Gates

```bash
pre-commit run --all-files
ruff check jobs
ruff format jobs
mypy jobs
```

<br>
