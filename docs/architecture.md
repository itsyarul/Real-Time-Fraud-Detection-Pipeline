# System Architecture

## Overview

The **Real-Time Fraud Detection Platform** is designed to process, inspect, validate, store, and analyze high-throughput credit card transaction streams with sub-second latency. It combines distributed stream processing, an ACID lakehouse storage layer, automated continuous data quality enforcement, machine learning inference and retraining, and real-time operational observability.

![System Architecture](Architecture_diagram.png)

---

## End-to-End Data Lifecycle

```
                         Transaction Dataset (creditcard.csv)
                                       │
                                       ▼
                             Kafka Producer Service
                                       │
                                       ▼
                       Apache Kafka (topic: transactions)
                                       │
                                       ▼
                          Spark Structured Streaming
                                       │
                         ┌─────────────┴─────────────┐
                         ▼                           ▼
                MinIO Bronze Delta           Spark Checkpoints
                         │
                         ▼
             Spark Clean & Enrichment (bronze_to_silver.py)
                         │
        ┌────────────────┼───────────────────────────┐
        ▼                ▼                           ▼
  Silver Delta    Quarantine Delta          Great Expectations
        │                                    (Data Quality)
        │
   ┌────┴────────────────────────┐
   ▼                             ▼
ML Retraining (Airflow)    Real-Time Inference (predict_fraud.py)
   │                             │
   ▼                             ├───────────────────────────┐
Model Registry (MinIO)           ▼                           ▼
                          Predictions Delta           Fraud Alerts Delta
                                 │                           │
                                 ▼                           ▼
                        Metrics Pusher / Pushgateway  Slack Webhook
                                 │
                                 ▼
                        Prometheus & Grafana
```

---

## Component Breakdown

### 1. Ingestion Layer: Apache Kafka & Producer
- **Engine**: Apache Kafka 4.3.1 operating in **KRaft** (Kafka Raft Metadata) mode, eliminating ZooKeeper dependency.
- **Topic**: `transactions` (single partition in local dev, horizontally partitionable in production).
- **Producer**: Python-based streaming service simulating real-world credit card swipe activity. It streams rows from the Kaggle Credit Card dataset at configurable rates, injecting realistic delays, looping options, and optional schema corruption for quarantine testing.
- **Inspection**: **Kafka UI** on port `8085` provides real-time visibility into topic offsets, consumer group lag, and message payloads.

### 2. Stream Processing Layer: Apache Spark Structured Streaming
- **Engine**: Apache Spark 4.0.1 (Master + Worker cluster).
- **Connector**: Spark-Kafka connector `4.0.1` and AWS SDK v2 + Hadoop-AWS `3.4.1` for S3A protocol access.
- **Micro-batch Execution**: Ingests records from Kafka using micro-batch triggers, ensuring at-least-once streaming delivery coupled with idempotent Delta Lake sinks.
- **Fault-Tolerance**: Distributed checkpointing stored directly on MinIO S3 (`s3a://spark-checkpoints/`).

### 3. Lakehouse Storage: MinIO & Delta Lake (Medallion Architecture)
- **Object Storage**: MinIO (S3-compatible, high-performance object store) with pre-provisioned buckets:
  - `bronze`: Raw immutable streaming events directly from Kafka.
  - `silver`: Cleaned, typed, feature-engineered, and deduplicated records.
  - `gold`: Analytical aggregates, business KPIs, and risk summaries.
  - `quarantine`: Corrupt, invalid, or schema-violating records rejected during processing.
  - `models`: Serialized ML models, performance metrics, and production pointers.
  - `predictions`: Real-time scored records with inference probabilities.
  - `fraud-alerts`: High-confidence fraud transactions exceeding decision thresholds.
  - `monitoring`: Inference telemetry and latency tracking.
- **Storage Format**: **Delta Lake 4.0.1** providing ACID transactions, time travel, schema enforcement, and atomic `MERGE` upserts.

### 4. Data Quality: Great Expectations (GX 1.19.1)
- Validates data at the Silver boundary before analytical modeling or ML consumption.
- Evaluates 37 distinct validation rules spanning schema conformance, null checks, numerical bounds, and business constraints.
- Any invalid batch or record is flagged and diverted to the quarantine layer, preventing pipeline poison pills.

### 5. Machine Learning Lifecycle: Training, Registry & Real-Time Scoring
- **Algorithm**: Scikit-Learn `LogisticRegression` with class reweighting (`balanced`) and standard feature scaling to tackle severe class imbalance (0.172% fraud rate).
- **Threshold Optimization**: Dynamically selects decision thresholds (production threshold: `0.99`) to optimize precision, recall, and F1 rather than defaulting to arbitrary `0.5`.
- **Model Registry**: MinIO-backed versioned artifact store tracking model binaries (`model.joblib`), evaluation metrics (`metadata.json`), and atomic `production` symlink promotion.
- **Inference Engine**: Spark Structured Streaming micro-batch job scoring live Silver transactions with real-time probability outputs.
- **Alerting**: Automated filtering that routes transactions with `fraud_probability >= 0.99` directly to the `fraud-alerts` Delta table and sends Slack notifications.

### 6. Workflow Orchestration: Apache Airflow 2.9.3
- Manages the offline and continuous ML lifecycle DAG (`fraud_ml_pipeline`).
- Pipeline tasks:
  1. `validate_silver`: Execute Great Expectations suite against the Silver Delta table.
  2. `build_ml_dataset`: Extract feature matrices and train/test splits.
  3. `train_model`: Train model with balanced loss and compute full metric suite.
  4. `evaluate_model`: Perform precision-recall sweep across threshold ranges.
  5. `validate_model`: Verify candidate model meets minimum recall constraints.
  6. `promote_model`: Compare candidate against active production model and atomically update pointer if improved.

### 7. Observability & Monitoring: Prometheus, Pushgateway & Grafana
- **Metrics Pusher**: Dedicated daemon reading inference and pipeline stats and sending them to **Prometheus Pushgateway**.
- **Prometheus 3.5.0**: Scrapes Kafka Exporter, cAdvisor, Node Exporter, and Pushgateway.
- **Alertmanager**: Dispatches alert notifications to Slack when fraud spikes or lag accumulates.
- **Grafana 12.1.1**: Pre-provisioned dashboards visualizing throughput, fraud rate, model inference latency, and cluster health.
