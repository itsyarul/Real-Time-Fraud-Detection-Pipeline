#!/bin/bash

set -e

APP_PATH="$1"

if [ -z "$APP_PATH" ]; then
    echo "Usage: submit_ml.sh <application.py>"
    exit 1
fi

spark-submit \
    --conf "spark.cores.max=${ML_TRAINING_CORES}" \
    --conf "spark.executor.cores=${ML_TRAINING_CORES}" \
    --conf "spark.executor.memory=${ML_TRAINING_MEMORY}" \
    "$APP_PATH"