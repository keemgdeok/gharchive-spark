#!/usr/bin/env bash
# 실행 시 주입될 핵심 자원 설정
export SPARK_MASTER_HOST=${SPARK_MASTER_HOST:-spark-master}
export SPARK_MASTER_PORT=${SPARK_MASTER_PORT:-7077}
export SPARK_MASTER_WEBUI_PORT=${SPARK_MASTER_WEBUI:-8080}
export SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-2}
export SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-4G}
export SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-2G}
export SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-2G}
export SPARK_LOCAL_DIRS=${SPARK_LOCAL_DIR:-/opt/spark/work-dir}
export PYSPARK_PYTHON=python3

# Hadoop 클라이언트가 있을 때만 CLASSPATH 확장
if [ -x "${SPARK_HOME}/bin/hadoop" ]; then
  export SPARK_DIST_CLASSPATH=$(${SPARK_HOME}/bin/hadoop classpath)
fi

# History Server 이벤트 로그 경로 고정
export SPARK_HISTORY_OPTS=${SPARK_HISTORY_OPTS:--Dspark.history.fs.logDirectory=file:///opt/spark/event-logs -Dspark.history.ui.port=${SPARK_HISTORY_PORT:-18080}}
