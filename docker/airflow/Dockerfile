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

# Java 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl procps ca-certificates \
    && rm -rf /var/lib/apt/lists/*

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

USER airflow

# Airflow Provider 설치
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.11.1 \
    apache-airflow-providers-amazon==9.1.0 \
    aiohttp \
    boto3 \
    requests
