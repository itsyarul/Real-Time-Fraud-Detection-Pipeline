ARG SPARK_VERSION
ARG SPARK_KAFKA_CONNECTOR_VERSION
ARG DELTA_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_V2_VERSION

FROM apache/spark:${SPARK_VERSION}-python3

ARG SPARK_KAFKA_CONNECTOR_VERSION
ARG DELTA_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_V2_VERSION
ARG KAFKA_CLIENT_VERSION=3.9.1
ARG COMMONS_POOL_VERSION=2.12.0

USER root

# --------------------------------------------------
# 1. OS dependencies
# --------------------------------------------------

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        wget \
        procps && \
    rm -rf /var/lib/apt/lists/*


# --------------------------------------------------
# 2. Python dependencies
# --------------------------------------------------

COPY apps/spark/requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt


# --------------------------------------------------
# 3. Spark / Kafka / Delta / S3 dependencies
# --------------------------------------------------

RUN mkdir -p /opt/spark/jars && \
    curl -fL \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-sql-kafka-0-10_2.13-${SPARK_KAFKA_CONNECTOR_VERSION}.jar" \
      -o "/opt/spark/jars/spark-sql-kafka-0-10_2.13-${SPARK_KAFKA_CONNECTOR_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/${SPARK_KAFKA_CONNECTOR_VERSION}/spark-token-provider-kafka-0-10_2.13-${SPARK_KAFKA_CONNECTOR_VERSION}.jar" \
      -o "/opt/spark/jars/spark-token-provider-kafka-0-10_2.13-${SPARK_KAFKA_CONNECTOR_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/${KAFKA_CLIENT_VERSION}/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" \
      -o "/opt/spark/jars/kafka-clients-${KAFKA_CLIENT_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/${COMMONS_POOL_VERSION}/commons-pool2-${COMMONS_POOL_VERSION}.jar" \
      -o "/opt/spark/jars/commons-pool2-${COMMONS_POOL_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/io/delta/delta-spark_2.13/${DELTA_VERSION}/delta-spark_2.13-${DELTA_VERSION}.jar" \
      -o "/opt/spark/jars/delta-spark_2.13-${DELTA_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar" \
      -o "/opt/spark/jars/delta-storage-${DELTA_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
      -o "/opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fL \
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_V2_VERSION}/bundle-${AWS_SDK_V2_VERSION}.jar" \
      -o "/opt/spark/jars/bundle-${AWS_SDK_V2_VERSION}.jar"


# --------------------------------------------------
# 4. Directories
# --------------------------------------------------

RUN mkdir -p \
      /opt/spark/work-dir \
      /opt/spark/work \
      /opt/spark/tmp \
      /opt/spark/events \
      /opt/spark/logs \
      /opt/spark/warehouse \
      /opt/spark/checkpoints


# --------------------------------------------------
# 5. Application files
# These change frequently, so keep them late.
# --------------------------------------------------

COPY configs/spark/ /opt/spark/conf/

COPY apps/spark/entrypoint.sh /opt/spark/entrypoint.sh

COPY apps/spark/scripts/ /opt/spark/scripts/


# --------------------------------------------------
# 6. Permissions
# --------------------------------------------------

RUN chmod +x /opt/spark/entrypoint.sh && \
    chmod +x /opt/spark/scripts/*.sh && \
    chown -R spark:spark \
        /opt/spark/work-dir \
        /opt/spark/work \
        /opt/spark/tmp \
        /opt/spark/events \
        /opt/spark/logs \
        /opt/spark/warehouse \
        /opt/spark/checkpoints \
        /opt/spark/conf \
        /opt/spark/jars

USER spark

WORKDIR /opt/spark/work-dir

ENTRYPOINT ["/opt/spark/entrypoint.sh"]