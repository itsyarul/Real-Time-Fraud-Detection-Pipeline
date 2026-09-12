#!/usr/bin/env bash

# ======================================
# Java
# ======================================

export JAVA_HOME=/opt/java/openjdk

# ======================================
# Spark
# ======================================

export SPARK_HOME=/opt/spark

export SPARK_NO_DAEMONIZE=true

export SPARK_LOG_DIR=/opt/spark/logs

export SPARK_WORKER_DIR=/opt/spark/work

export SPARK_LOCAL_DIRS=/opt/spark/tmp

# ======================================
# Python
# ======================================

export PYSPARK_PYTHON=python3

export PYSPARK_DRIVER_PYTHON=python3

# ======================================
# History Server
# ======================================

export SPARK_HISTORY_OPTS="
-Dspark.history.fs.logDirectory=file:/opt/spark/events
"