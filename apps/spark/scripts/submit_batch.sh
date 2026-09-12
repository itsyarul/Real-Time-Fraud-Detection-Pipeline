#!/bin/bash

set -e

APP_PATH="$1"

if [ -z "$APP_PATH" ]; then
    echo "Usage: submit_batch.sh <application.py>"
    exit 1
fi

/opt/spark/bin/spark-submit \
    --master "spark://spark-master:7077" \
    --deploy-mode client \
    --conf "spark.cores.max=${SPARK_BATCH_CORES}" \
    --conf "spark.executor.cores=${SPARK_BATCH_CORES}" \
    --conf "spark.executor.memory=${SPARK_BATCH_MEMORY}" \
    "$APP_PATH"