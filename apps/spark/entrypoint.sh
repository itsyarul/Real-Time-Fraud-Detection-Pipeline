#!/usr/bin/env bash

set -e

case "${SPARK_MODE:-}" in

    master)
        exec /opt/spark/bin/spark-class \
            org.apache.spark.deploy.master.Master
        ;;

    worker)
        exec /opt/spark/bin/spark-class \
            org.apache.spark.deploy.worker.Worker \
            spark://spark-master:7077
        ;;

    history)
        exec /opt/spark/sbin/start-history-server.sh
        ;;

    submit)
        exec "$@"
        ;;

    *)
        echo "ERROR: Unknown SPARK_MODE='${SPARK_MODE:-}'"
        echo "Valid modes: master, worker, history, submit"
        exit 1
        ;;

esac