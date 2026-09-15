# Real-Time ML Inference & Fraud Alerting

This document describes how the platform executes sub-second machine learning inference on live streaming transactions, records prediction history, and dispatches real-time fraud alerts.

---

## Real-Time Inference Architecture

```
MinIO Silver Delta (s3a://silver/transactions)
                    │
                    ▼
Spark Streaming Inference Job (apps/ml/inference/predict_fraud.py)
                    │
                    ├──► Fetches Model: s3a://models/fraud-detection/.../production/
                    │
                    ▼
           Inference Scoring UDF
                    │
        ┌───────────┴───────────────────────────┐
        ▼                                       ▼
All Scored Transactions (Delta)          Fraud Alerts (Delta)
s3a://predictions/fraud                  s3a://fraud-alerts/fraud (P >= 0.99)
        │                                       │
        ▼                                       ▼
Metrics Pusher / Prometheus             Alert Reader & Slack Webhook
```

---

## 1. Inference Engine (`apps/ml/inference/predict_fraud.py`)

- **Trigger**: Spark Structured Streaming micro-batches reading from `s3a://silver/transactions`.
- **Production Model Resolution**:
  - Dynamically loads `model.joblib` and `metadata.json` from `s3a://models/fraud-detection/fraud-logistic-regression/production/` via the `ModelRegistry` class.
  - Broadcasts the loaded model across all Spark executors to achieve maximum throughput without repeatedly hitting MinIO.
- **Decision Boundary**:
  - Applies `ML_INFERENCE_THRESHOLD` (default: **`0.99`**).
  - Calculates probability: `P = model.predict_proba(features)[:, 1]`.
  - Determines binary flag: `fraud_prediction = 1 if P >= 0.99 else 0`.

---

## 2. Predictions Table Schema (`s3a://predictions/fraud`)

Every transaction processed through the inference pipeline is logged to the Delta predictions table:

| Column | Type | Description |
|---|---|---|
| `event_id` | `StringType` | Unique UUID of the transaction |
| `source_row_id` | `LongType` | Source index from dataset |
| `Time` | `DoubleType` | Elapsed seconds since first transaction |
| `Amount` | `DoubleType` | Transaction amount in USD |
| `fraud_probability` | `DoubleType` | Raw model probability score [0.0000 to 1.0000] |
| `fraud_prediction` | `LongType` | Binary classification: `1` (Fraud) or `0` (Legitimate) |
| `model_name` | `StringType` | Name of model used (`fraud-logistic-regression`) |
| `model_version` | `StringType` | Active version (e.g., `production` or `1.0.1`) |
| `decision_threshold` | `DoubleType` | Threshold applied during scoring (`0.99`) |
| `prediction_timestamp` | `TimestampType` | UTC timestamp when prediction was generated |

---

## 3. Real-Time Fraud Alerting (`s3a://fraud-alerts/fraud`)

### Fraud Alerts Delta Table
Whenever `fraud_prediction == 1`, the record is automatically filtered and appended to `s3a://fraud-alerts/fraud`.

### Alert Reader & Slack Integration (`apps/ml/alerts/read_alerts.py`)
- Continuously polls the `fraud-alerts` Delta table using Delta table version checkpoints.
- Formats incident payloads with critical transaction indicators:
  - High dollar amount
  - Risk probability percentage
  - Model version and timestamp
- Dispatches high-priority webhooks to your team's Slack channel via `SLACK_WEBHOOK`:
```json
{
  "text": "*FRAUD ALERT DETECTED* \n• Event ID: `c1f7b88e-4a6c-4f79-88b1-38e9d6d5a1b2`\n• Amount: `$1,849.20`\n• Fraud Probability: `99.82%`\n• Model: `fraud-logistic-regression:production`"
}
```

---

## 4. Verification & Inspection Commands

Run these inside the project directory:

```bash
# Check current predictions in MinIO
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/ml/inference/verify_predictions.py

# Read active fraud alerts
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/ml/alerts/read_alerts.py
```
