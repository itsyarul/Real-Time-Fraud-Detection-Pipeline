# 🌊 Data Pipeline & Medallion Lakehouse

This document details the transaction lifecycle, streaming ingestion, Delta Lake medallion storage layers, validation quarantine logic, and idempotency guarantees implemented across the platform.

![Kafka and Spark Processing](KafkaSpark%20processing.png)

---

## 🔁 Complete Transaction Lifecycle

```
creditcard.csv (Kaggle Dataset)
       │
       ▼
Streaming Producer (apps/producer/producer.py)
       │ (JSON over TCP / 100ms interval)
       ▼
Apache Kafka Topic ("transactions")
       │
       ▼
Spark Streaming Job 1: kafka_to_bronze.py
       │ (Append-only micro-batches)
       ▼
MinIO Bronze Layer (s3a://bronze/transactions)
       │
       ▼
Spark Streaming Job 2: bronze_to_silver.py
       │
       ├─────────────────────────────────────────┐
       │ (Schema invalid / nulls / negative)     │ (Valid schema & range checks)
       ▼                                         ▼
MinIO Quarantine (s3a://quarantine/transactions)  MinIO Silver (s3a://silver/transactions)
                                                 │
                                                 ├───────────────────────┐
                                                 ▼                       ▼
                                       ML Retraining Pipeline     Inference Engine
                                       (Airflow DAG)              (predict_fraud.py)
```

---

## 1. Streaming Ingestion: Producer & Kafka

### Producer (`apps/producer/producer.py`)
- Reads historical transactions from `data/creditcard.csv`.
- Maps each row to a standardized streaming event contract:
  - Generates a globally unique UUID `event_id`.
  - Attaches `source_row_id` for deterministic tracing back to the raw source.
  - Normalizes PCA features `V1` through `V28`, `Time`, and `Amount`.
- Configurable modes via environment variables:
  - `PRODUCER_MODE=continuous` (streams endlessly in a loop for continuous load testing).
  - `PRODUCER_DELAY_MS=100` (controls ingestion throughput).
  - `PRODUCER_SHUFFLE=true` (avoids temporal bias during streaming simulation).

### Kafka Topic Contract: `transactions`
```json
{
  "event_id": "c1f7b88e-4a6c-4f79-88b1-38e9d6d5a1b2",
  "source_row_id": 1423,
  "Time": 406.0,
  "V1": -2.312226542,
  "V2": 1.951992011,
  "V3": -1.609850732,
  "...": "...",
  "V28": 0.133558377,
  "Amount": 149.62,
  "Class": 0
}
```

---

## 2. Medallion Storage Architecture (MinIO & Delta Lake)

![MinIO Buckets](Minio%20buckets.png)

### 🥉 Bronze Layer (`s3a://bronze/transactions`)
- **Role**: Raw, immutable event store. Preserves the exact payload received from Kafka along with ingestion metadata.
- **Job**: `apps/spark/jobs/kafka_to_bronze.py`
- **Schema**:
  - Raw JSON string parsed into columns: `event_id`, `source_row_id`, `Time`, `V1`..`V28`, `Amount`, `Class`.
  - Metadata column added: `ingestion_timestamp = current_timestamp()`.
- **Write Mode**: Append-only Delta table with Spark streaming checkpointing at `s3a://spark-checkpoints/kafka-to-bronze`.

### 🥈 Silver Layer (`s3a://silver/transactions`)
- **Role**: Cleaned, typed, deduplicated, and validated transactions ready for ML training and live scoring.
- **Job**: `apps/spark/jobs/bronze_to_silver.py`
- **Transformations**:
  1. Casts all features to deterministic Spark SQL data types (`DoubleType`, `LongType`, `StringType`).
  2. Synthesizes `processed_timestamp = current_timestamp()`.
  3. Evaluates 7 core structural data quality rules (null check on `event_id`, positive `source_row_id`, non-negative `Amount`, valid `Time`).
  4. Filters valid records into Silver and diverts corrupt records into Quarantine.
  5. Performs atomic Delta `MERGE` on `event_id` to guarantee zero duplicate records even if Kafka or Spark restarts mid-batch.

### 🚫 Quarantine Layer (`s3a://quarantine/transactions`)
- **Role**: Dead-letter store for corrupt, unparseable, or schema-violating events.
- **Job**: `apps/spark/jobs/bronze_to_silver.py`
- **Error Flagging**:
  - Each invalid record is appended with an `error_reasons` array (e.g., `["NULL_EVENT_ID"]`, `["NEGATIVE_AMOUNT"]`, `["NULL_TIME"]`).
  - Quarantine events can be inspected via `apps/spark/jobs/read_quarantine.py` for root-cause analysis without breaking downstream consumers.

### 🥇 Gold Layer (`s3a://gold/fraud_metrics`)
- **Role**: Aggregated analytical tables and business metrics.
- **Metrics**: Hourly transaction volume, running fraud rates, total fraudulent dollar amount, and high-risk account velocity patterns.

---

## 3. Idempotency & Fault-Tolerance Deep Dive

A critical challenge in distributed streaming is handling container crashes, worker disconnects, and network partitions without producing duplicate records.

```
Without Idempotency:
Transaction A ──► Ingested ──► Spark Crashes ──► Batch Replayed ──► Table has [A, A] (Corrupted Metrics!)

With Our Idempotent Implementation:
Transaction A ──► Ingested ──► Spark Crashes ──► Batch Replayed ──► Delta MERGE (Match on event_id) ──► Table has [A]
```

### Bronze Layer Idempotency
- Uses Spark Structured Streaming **write-ahead logs** and deterministic offset commits in `s3a://spark-checkpoints/kafka-to-bronze`.
- If the streaming engine restarts, it resumes reading from the exact uncommitted Kafka partition offset.

### Silver Layer Idempotency: Delta MERGE
Rather than using `mode("append")`, the Silver writer executes an atomic Delta `MERGE`:
```python
delta_table = DeltaTable.forPath(spark, SILVER_PATH)
(
    delta_table.alias("silver")
    .merge(
        incoming_batch_df.alias("updates"),
        "silver.event_id = updates.event_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)
```
- **Result**: Replaying a batch of 1,000 transactions produces **0 duplicate records** in Silver.

### Prediction Idempotency
- Real-time predictions in `s3a://predictions/fraud` also use Delta Lake checkpoints and upsert semantics on `event_id`. Duplicate streaming events will never trigger multiple fraud alerts for the same transaction.
