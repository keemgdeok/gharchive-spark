FROM eclipse-temurin:11-jre-jammy

ARG SPARK_VERSION=3.4.1
ARG HADOOP_VERSION=3
ARG HADOOP_AWS_VERSION=3.3.2
ARG AWS_SDK_VERSION=1.11.1026

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}

# 기본 도구와 Python 패키지 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates python3 python3-pip procps tini \
    && rm -rf /var/lib/apt/lists/*

# Spark 바이너리 설치
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

# S3A 의존성 추가 (Spark-Hadoop 버전과 일치)
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    && curl -fsSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Python 라이브러리 설치
RUN pip3 install --no-cache-dir pyspark==${SPARK_VERSION} pandas boto3 requests aiohttp

# 로그 및 이벤트 디렉토리 생성
RUN mkdir -p ${SPARK_HOME}/conf ${SPARK_HOME}/logs ${SPARK_HOME}/event-logs ${SPARK_HOME}/work-dir

# 설정 복사
COPY conf/ ${SPARK_HOME}/conf/

WORKDIR ${SPARK_HOME}

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/bin/bash"]
