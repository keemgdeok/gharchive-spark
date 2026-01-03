<div align="center">

# <code>gharchive-spark</code>

<em>GHArchive 로그를 Spark + MinIO로 처리하는 Medallion 기반 데이터 레이크 파이프라인</em>

<em>Built with the tools and technologies:</em>

<img src="https://img.shields.io/badge/Apache%20Spark-3.5.7-E25A1C?style=flat&logo=apachespark&logoColor=white" alt="Apache Spark">
<img src="https://img.shields.io/badge/PySpark-3.5.7-E25A1C?style=flat&logo=apachespark&logoColor=white" alt="PySpark">
<img src="https://img.shields.io/badge/Hadoop-3.3.4-66CCFF?style=flat&logo=apachehadoop&logoColor=black" alt="Hadoop">
<img src="https://img.shields.io/badge/MinIO-S3%20Compatible-000000?style=flat&logo=minio&logoColor=white" alt="MinIO">
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

\#

### End-to-end flow

\#

**Flow** <br>
Bronze(원시 JSON) → Silver(중첩 해제/정제 Parquet) → Gold(집계 마트)

**Storage** <br>
MinIO `bronze/`, `silver/`, `gold/` 경로 사용

**Observability** <br>
Spark UI(Driver) + Spark History Server 이벤트 로그

### Technical concerns

\#

<br>

______________________________________________________________________

## Features

| Component | Details |
| :-- | :-- |
| Architecture | <ul><li>Docker Compose 기반 Spark Cluster + MinIO + History Server</li><li>Medallion Architecture (Bronze/Silver/Gold)</li><li>S3A 연동 로컬 S3 호환 레이크</li></ul> |
| Bronze | <ul><li>aiohttp 비동기 다운로드</li><li>재시도/타임아웃/멱등 업로드</li><li>`bronze/YYYY/MM/DD/` 적재</li></ul> |
| Silver | <ul><li>명시적 Superset 스키마 적용</li><li>explode/col("a.b.c")로 중첩 해제</li><li>`partitionBy("dt")` Parquet 저장</li></ul> |
| Performance | <ul><li>AQE 활성화</li><li>`spark.sql.shuffle.partitions` 튜닝</li><li>Small File/Data Skew 시나리오 재현/개선</li></ul> |
| Observability | <ul><li>Spark UI(4040-4050)</li><li>Spark History Server(18080)</li><li>Event Log 보존 정책</li></ul> |
| Quality | <ul><li>ruff + mypy + pre-commit</li><li>Docker 기반 재현</li></ul> |

<br>

______________________________________________________________________

## Key Directories

| Path | Purpose |
| :-- | :-- |
| `docker-compose.yaml` | Spark/MinIO/History Server 인프라 정의 |
| `docker/spark/` | Spark 이미지 빌드 및 S3A JAR 포함 |
| `docker/spark/conf/` | `spark-defaults.conf`, `spark-env.sh`, `log4j.properties` |
| `jobs/bronze/` | GHArchive 비동기 수집 파이프라인 |
| `jobs/silver/` | 스키마 정의 및 events_base/멀티 트랙 변환 |
| `jobs/gold/` | Gold 집계 파이프라인 (TODO) |
| `jobs/spark_runtime.py` | S3A/JAR/환경 변수 검증 |
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

   SPARK_MASTER_HOST=spark-master
   SPARK_MASTER_PORT=7077
   SPARK_MASTER_WEBUI=8080
   SPARK_WORKER_CORES=2
   SPARK_WORKER_MEMORY=4G
   SPARK_HISTORY_PORT=18080

   MINIO_ROOT_USER=minioadmin
   MINIO_ROOT_PASSWORD=minioadmin
   MINIO_REGION=us-east-1
   MINIO_PORT=9000
   MINIO_CONSOLE_PORT=9090
   MINIO_BUCKET=gharchive
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
     --hour 2024-05-21-00
   ```

6. **UI 접근**
    ```text
    Spark Master: http://localhost:8080
    Spark Worker: http://localhost:8081, http://localhost:8082
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

#### Bronze ingestion

```bash
# --date 24시간 전체 처리
docker compose exec -T spark-master \
  python3 -m jobs.bronze.ingest --date 2024-05-21 --concurrency 8

# --hour 단일 시간 처리
docker compose exec -T spark-master \
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
  /opt/gharchive/jobs/gold/base.py --date 2024-05-21
```

<br>

<!--
______________________________________________________________________

 ## Troubleshooting Log

| Problem | Before | After | Evidence | Notes |
| :-- | :-- | :-- | :-- | :-- |
| Small File | TBD | TBD | Spark UI 캡처 경로 | coalesce(N), AQE 설정 |
| Data Skew | TBD | TBD | Spark UI 캡처 경로 | salting/broadcast 적용 |
| Nested Schema | TBD | TBD | explain() 로그 | 스키마 드리프트 대응 | -->

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
