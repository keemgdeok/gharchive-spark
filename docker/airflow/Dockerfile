# Airflow 2.10 + Spark Provider + Spark 바이너리
FROM apache/airflow:2.10.4-python3.10

USER root

# Spark/Hadoop 버전 (프로젝트와 일치)
ARG SPARK_VERSION=3.5.7
ARG HADOOP_VERSION=3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${PATH}

# Java 및 Docker CLI 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl procps ca-certificates docker.io \
    && rm -rf /var/lib/apt/lists/*

# Docker 그룹에 airflow 사용자 추가 (호스트 docker 소켓 접근용)
RUN groupadd -g 984 docker || true \
    && usermod -aG docker airflow || true

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark 바이너리 다운로드
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# S3A 의존성 추가 (MinIO 연동)
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Spark 작업 디렉토리 생성 (쓰기 권한 필요)
RUN mkdir -p ${SPARK_HOME}/work-dir ${SPARK_HOME}/event-logs \
    && chmod -R 777 ${SPARK_HOME}/work-dir ${SPARK_HOME}/event-logs

USER airflow

# Airflow Provider 설치
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.1 \
    apache-airflow-providers-amazon==9.1.0 \
    aiohttp \
    boto3 \
    requests
