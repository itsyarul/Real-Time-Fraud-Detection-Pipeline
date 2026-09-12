#!/usr/bin/env python3
"""
push_metrics.py
---------------
Reads the ML inference monitoring Delta table from MinIO and pushes
the latest batch metrics to the Prometheus Pushgateway.

Runs on a 60-second cron schedule inside the metrics-pusher container.
"""

import os
import logging
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

# -------------------------------------------------------
# Logging
# -------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("push_metrics")

# -------------------------------------------------------
# Config (all overridable via env)
# -------------------------------------------------------
PUSHGATEWAY_URL   = os.getenv("PUSHGATEWAY_URL",   "http://pushgateway:9091")
MINIO_ENDPOINT    = os.getenv("MINIO_S3_ENDPOINT",  "http://minio:9000")
MINIO_ACCESS_KEY  = os.getenv("MINIO_ROOT_USER",    "minio")
MINIO_SECRET_KEY  = os.getenv("MINIO_ROOT_PASSWORD","MinioAdmin2026")
MONITORING_BUCKET = os.getenv("MONITORING_BUCKET",  "monitoring")
MONITORING_PREFIX = os.getenv("MONITORING_PREFIX",  "ml-inference")
JOB_NAME          = "fraud_detection_ml"

# -------------------------------------------------------
# Metric definitions
# -------------------------------------------------------
def build_registry():
    reg = CollectorRegistry()

    metrics = {
        "total_batches": Gauge(
            "fraud_detection_total_batches",
            "Total number of ML inference micro-batches processed",
            registry=reg,
        ),
        "input_records_total": Gauge(
            "fraud_detection_input_records_total",
            "Cumulative input transaction records processed by ML inference",
            registry=reg,
        ),
        "predictions_total": Gauge(
            "fraud_detection_predictions_total",
            "Cumulative prediction records produced by ML inference",
            registry=reg,
        ),
        "fraud_predictions_total": Gauge(
            "fraud_detection_fraud_predictions_total",
            "Cumulative fraud predictions flagged by ML inference",
            registry=reg,
        ),
        "fraud_rate": Gauge(
            "fraud_detection_fraud_rate",
            "Fraud rate of the most recent inference batch (fraud / predictions)",
            registry=reg,
        ),
        "avg_fraud_probability": Gauge(
            "fraud_detection_avg_fraud_probability",
            "Average fraud probability score of the most recent inference batch",
            registry=reg,
        ),
        "max_fraud_probability": Gauge(
            "fraud_detection_max_fraud_probability",
            "Maximum fraud probability score of the most recent inference batch",
            registry=reg,
        ),
        "decision_threshold": Gauge(
            "fraud_detection_model_decision_threshold",
            "Decision threshold used by the ML model in the most recent batch",
            registry=reg,
        ),
        "last_batch_id": Gauge(
            "fraud_detection_last_batch_id",
            "ID of the most recently processed ML inference batch",
            registry=reg,
        ),
        "last_batch_timestamp_seconds": Gauge(
            "fraud_detection_last_batch_timestamp_seconds",
            "Unix timestamp of the most recently processed ML inference batch",
            registry=reg,
        ),
    }
    return reg, metrics


# -------------------------------------------------------
# Read monitoring summary from Delta Lake via MinIO
# (reads Parquet files directly — no Spark needed)
# -------------------------------------------------------
def read_monitoring_summary():
    """
    Reads the Delta Lake monitoring table by listing and downloading
    Parquet data files directly from MinIO (no Spark required).

    Returns a dict with aggregate + latest-batch metrics, or None on failure.
    """
    try:
        import pyarrow.parquet as pq
        import pyarrow as pa
        import io
    except ImportError:
        log.error("pyarrow is required. Install it in the metrics-pusher container.")
        return None

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )

    # List all Parquet files under the monitoring prefix
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=MONITORING_BUCKET,
            Prefix=MONITORING_PREFIX,
        )
        parquet_keys = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if (
                    key.endswith(".parquet")
                    and "_delta_log" not in key   # exclude Delta transaction log checkpoints
                ):
                    parquet_keys.append(key)
    except ClientError as e:
        log.error("Failed to list monitoring files in MinIO: %s", e)
        return None

    if not parquet_keys:
        log.warning("No Parquet files found under s3://%s/%s", MONITORING_BUCKET, MONITORING_PREFIX)
        return None

    log.info("Found %d Parquet file(s) in monitoring table", len(parquet_keys))

    # Download and concatenate all Parquet files
    tables = []
    for key in parquet_keys:
        try:
            resp = s3.get_object(Bucket=MONITORING_BUCKET, Key=key)
            buf = io.BytesIO(resp["Body"].read())
            tables.append(pq.read_table(buf))
        except Exception as e:
            log.warning("Could not read %s: %s", key, e)

    if not tables:
        log.error("No readable Parquet files found.")
        return None

    table = pa.concat_tables(tables)
    df = table.to_pydict()

    # Aggregate totals
    batch_ids           = df.get("batch_id", [])
    input_records_col   = df.get("input_records", [])
    prediction_col      = df.get("prediction_records", [])
    fraud_preds_col     = df.get("fraud_predictions", [])
    fraud_rate_col      = df.get("fraud_rate", [])
    avg_prob_col        = df.get("avg_fraud_probability", [])
    max_prob_col        = df.get("max_fraud_probability", [])
    threshold_col       = df.get("decision_threshold", [])
    timestamp_col       = df.get("metric_timestamp", [])

    if not batch_ids:
        log.warning("Monitoring table is empty.")
        return None

    # Find the most recent batch by batch_id
    latest_idx = batch_ids.index(max(batch_ids))

    # Parse last batch timestamp to Unix seconds
    last_ts = timestamp_col[latest_idx] if timestamp_col else None
    if isinstance(last_ts, datetime):
        last_ts_seconds = last_ts.replace(tzinfo=timezone.utc).timestamp()
    elif isinstance(last_ts, (int, float)):
        last_ts_seconds = float(last_ts)
    else:
        last_ts_seconds = datetime.now(timezone.utc).timestamp()

    summary = {
        "total_batches":           len(set(batch_ids)),
        "input_records_total":     sum(v for v in input_records_col if v),
        "predictions_total":       sum(v for v in prediction_col if v),
        "fraud_predictions_total": sum(v for v in fraud_preds_col if v),
        "fraud_rate":              fraud_rate_col[latest_idx] if fraud_rate_col else 0.0,
        "avg_fraud_probability":   avg_prob_col[latest_idx]   if avg_prob_col   else 0.0,
        "max_fraud_probability":   max_prob_col[latest_idx]   if max_prob_col   else 0.0,
        "decision_threshold":      threshold_col[latest_idx]  if threshold_col  else 0.0,
        "last_batch_id":           max(batch_ids),
        "last_batch_timestamp_seconds": last_ts_seconds,
    }

    log.info(
        "Summary — batches=%d | total_input=%d | fraud_rate=%.4f%%",
        summary["total_batches"],
        summary["input_records_total"],
        summary["fraud_rate"] * 100,
    )
    return summary


# -------------------------------------------------------
# Push to Prometheus Pushgateway
# -------------------------------------------------------
def push_metrics(summary: dict) -> bool:
    reg, metrics = build_registry()

    metrics["total_batches"].set(summary["total_batches"])
    metrics["input_records_total"].set(summary["input_records_total"])
    metrics["predictions_total"].set(summary["predictions_total"])
    metrics["fraud_predictions_total"].set(summary["fraud_predictions_total"])
    metrics["fraud_rate"].set(summary["fraud_rate"])
    metrics["avg_fraud_probability"].set(summary["avg_fraud_probability"])
    metrics["max_fraud_probability"].set(summary["max_fraud_probability"])
    metrics["decision_threshold"].set(summary["decision_threshold"])
    metrics["last_batch_id"].set(summary["last_batch_id"])
    metrics["last_batch_timestamp_seconds"].set(summary["last_batch_timestamp_seconds"])

    try:
        push_to_gateway(PUSHGATEWAY_URL, job=JOB_NAME, registry=reg)
        log.info("✅ Metrics pushed to Pushgateway at %s", PUSHGATEWAY_URL)
        return True
    except Exception as e:
        log.error("❌ Failed to push metrics to Pushgateway: %s", e)
        return False


# -------------------------------------------------------
# Entry point
# -------------------------------------------------------
def main():
    log.info("=" * 60)
    log.info("Fraud Detection — Metrics Pusher")
    log.info("Pushgateway : %s", PUSHGATEWAY_URL)
    log.info("MinIO       : %s", MINIO_ENDPOINT)
    log.info("Bucket      : s3://%s/%s", MONITORING_BUCKET, MONITORING_PREFIX)
    log.info("=" * 60)

    summary = read_monitoring_summary()
    if summary is None:
        log.warning("No monitoring data available yet — skipping push.")
        return

    push_metrics(summary)


if __name__ == "__main__":
    main()
