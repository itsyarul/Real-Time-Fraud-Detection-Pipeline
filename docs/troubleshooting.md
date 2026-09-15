# 🔍 Troubleshooting Guide

This guide documents common issues encountered during local development, container startup, or streaming execution, along with diagnostic commands and resolutions.

---

## 1. Spark Cannot Connect to Kafka

### Symptoms
- Spark job fails with `org.apache.kafka.common.errors.TimeoutException: Failed to update metadata`.
- Spark logs show `Connection to node -1 (kafka:9092) could not be established`.

### Diagnosis & Fix
1. **Check Kafka Container Health**:
   ```bash
   docker compose ps kafka
   ```
   Ensure Kafka has status `healthy`. During initialization, Kafka KRaft may take 15–20 seconds to elect the controller quorum.
2. **Verify Network Hostname**:
   Inside the Docker backend network, Spark containers must communicate with `kafka:9092`, NOT `localhost:9092`. Ensure `KAFKA_BOOTSTRAP_SERVERS=kafka:9092` in `.env`.
3. **Inspect Kafka Logs**:
   ```bash
   docker compose logs kafka
   ```

---

## 2. Spark Cannot Read or Write to MinIO (S3A Errors)

### Symptoms
- `com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied` (HTTP 403).
- `java.net.ConnectException: Connection refused` to `minio:9000`.
- `org.apache.hadoop.fs.s3a.AWSBadRequestException: The specified bucket does not exist`.

### Diagnosis & Fix
1. **Verify MinIO Service & Buckets**:
   ```bash
   docker compose ps minio mc
   ```
   Ensure the `mc` (MinIO Client) initialization container exited successfully (`exit 0`), which pre-creates all required buckets (`bronze`, `silver`, `gold`, `quarantine`, `models`, `predictions`, `fraud-alerts`).
2. **Check S3A Configuration**:
   Spark requires path-style access for MinIO. Ensure your Spark configuration includes:
   ```properties
   spark.hadoop.fs.s3a.endpoint=http://minio:9000
   spark.hadoop.fs.s3a.path.style.access=true
   spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
   ```
3. **Verify S3 Credentials**:
   Confirm `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env` match `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`.

---

## 3. Airflow DAG Fails or Fails to Trigger Docker Commands

### Symptoms
- Airflow task `validate_silver` or `build_ml_dataset` fails with `permission denied while trying to connect to the Docker daemon socket`.
- Task fails with `FileNotFoundError: /opt/spark/work-dir/...`.

### Diagnosis & Fix
1. **Docker Socket Permissions**:
   The Airflow scheduler executes Docker Compose commands against `/var/run/docker.sock`. On Linux hosts, ensure proper permissions:
   ```bash
   sudo chmod 666 /var/run/docker.sock
   ```
2. **Path Mapping in Windows / WSL**:
   If running Docker Desktop on Windows, ensure the drive where the project resides (e.g. `d:\`) is shared under Docker Desktop **Settings > Resources > File Sharing**.
3. **Check Task Logs in Web UI**:
   Navigate to `http://localhost:8080`, click on the failed task instance, and open the **Log** tab for stack traces.

---

## 4. Inference Engine Cannot Find Production Model

### Symptoms
- `predict_fraud.py` raises `FileNotFoundError: Production model not found at s3a://models/.../production/model.joblib`.

### Diagnosis & Fix
1. **First-Time Bootstrapping**:
   The real-time inference engine requires an initial trained production model. Run the ML pipeline once to generate and promote model version 1.0.0:
   ```bash
   # Trigger the full ML pipeline
   docker compose exec airflow-webserver airflow dags trigger fraud_ml_pipeline
   ```
2. **Verify Model via MinIO Console**:
   Open `http://localhost:9001`, log in, navigate to bucket `models`, and ensure folder `fraud-detection/fraud-logistic-regression/production/` contains `model.joblib` and `metadata.json`.

---

## 5. Missing Dataset (`creditcard.csv`)

### Symptoms
- Producer logs show `FileNotFoundError: [Errno 2] No such file or directory: '/app/data/creditcard.csv'`.

### Diagnosis & Fix
1. Download the dataset from [Kaggle Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud).
2. Unzip and place `creditcard.csv` inside the `data/` folder:
   ```bash
   # Verify file exists and is around ~150MB
   ls -lh data/creditcard.csv
   ```
