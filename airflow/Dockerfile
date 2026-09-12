ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.12

FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

USER root

# Basic utilities used by orchestration/debugging tasks
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        wget \
        procps \
        default-jre-headless \
        docker.io \
    && rm -rf /var/lib/apt/lists/*

# Install Docker Compose v2
RUN curl -L "https://github.com/docker/compose/releases/download/v2.29.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose \
    && mkdir -p /usr/local/lib/docker/cli-plugins \
    && ln -s /usr/local/bin/docker-compose /usr/local/lib/docker/cli-plugins/docker-compose

USER airflow

# Python dependencies required by Airflow tasks.
#
# Keep Spark, Delta Lake, Kafka, and ML dependencies
# in the Spark image. Airflow only orchestrates them.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-postgres \
    apache-airflow-providers-docker \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

WORKDIR /opt/airflow
