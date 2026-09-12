#!/bin/bash

set -e

APP_PATH="$1"

if [ -z "$APP_PATH" ]; then
    echo "Usage: submit_inference.sh <application.py>"
    exit 1
fi

spark-submit \
    --conf "spark.cores.max=${ML_INFERENCE_CORES}" \
    --conf "spark.executor.cores=${ML_INFERENCE_CORES}" \
    --conf "spark.executor.memory=${ML_INFERENCE_MEMORY}" \
    "$APP_PATH"