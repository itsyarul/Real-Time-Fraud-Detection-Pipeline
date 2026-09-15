# Operations Runbook

This guide contains step-by-step operational instructions for starting, managing, inspecting, and troubleshooting the platform in local or staging environments.

---

## 1. Platform Lifecycle Commands

### Start Core Platform
```bash
# Build custom images (Spark, Airflow, Producer, Metrics Pusher)
docker compose build

# Start core services in detached background mode
docker compose up -d
```

### Check Service Health
```bash
# View container status and health check state
docker compose ps

# Inspect logs of a specific service
docker compose logs -f kafka
docker compose logs -f spark-master
docker compose logs -f airflow-webserver
```

### Stop or Reset Platform
```bash
# Stop containers without losing data
docker compose down

# Stop containers and remove volumes (Full Reset)
docker compose down -v
```

---

## 2. Running Data Pipeline Jobs

### Step 1: Start Streaming Producer
Launch the transaction generator to stream simulated credit card transactions into Kafka:
```bash
docker compose run --rm producer
```

### Step 2: Ingest from Kafka to Bronze Delta
Submit the streaming ingestion job to Spark:
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/kafka_to_bronze.py
```

### Step 3: Clean and Validate to Silver Delta
Submit the transformation and quarantine job:
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/bronze_to_silver.py
```

### Step 4: Run Real-Time ML Inference
Start the continuous scoring and alert engine:
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/ml/inference/predict_fraud.py
```

---

## 3. Triggering the Airflow ML Retraining Pipeline

### Via Web UI
1. Navigate to `http://localhost:8080`.
2. Login with credentials: `admin` / `admin` (or as set in `.env`).
3. Locate DAG `fraud_ml_pipeline`.
4. Toggle the DAG to **Active** and click **Trigger DAG** (play icon).

### Via CLI
```bash
docker compose exec airflow-webserver airflow dags trigger fraud_ml_pipeline
```

---

## 4. Inspecting Storage & Results

### Read Bronze Transactions
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/read_bronze.py
```

### Read Silver Cleaned Transactions
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/read_silver.py
```

### Read Quarantined Corrupt Records
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/jobs/read_quarantine.py
```

### Verify Scored Predictions
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/ml/inference/verify_predictions.py
```

### Check Real-Time Fraud Alerts
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/ml/alerts/read_alerts.py
```
