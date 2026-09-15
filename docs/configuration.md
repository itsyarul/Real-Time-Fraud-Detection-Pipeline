# ⚙️ Configuration Reference

All platform services are configured via environment variables defined in `.env`. A complete, sanitized template is provided in `.env.example`.

> [!CAUTION]
> **Never commit `.env` to Git.** Keep real production credentials, passwords, and webhook secrets restricted to your local `.env` or CI secret management.

---

## Environment Variables Breakdown

### Core Platform & Network
| Variable | Default Value | Description |
|---|---|---|
| `PROJECT_NAME` | `fraud-detection-platform` | Prefix for Docker container names and networks |
| `NETWORK_BACKEND` | `backend` | Internal bridge network for inter-service communication |
| `NETWORK_FRONTEND` | `frontend` | External-facing network for Web UIs |

### Engine & Component Versions
| Variable | Default Value | Description |
|---|---|---|
| `PYTHON_VERSION` | `3.12` | Base Python runtime version |
| `KAFKA_VERSION` | `4.3.1` | Apache Kafka engine version |
| `SPARK_VERSION` | `4.0.1` | Apache Spark distribution |
| `DELTA_VERSION` | `4.0.1` | Delta Lake storage connector |
| `POSTGRES_VERSION` | `16` | PostgreSQL engine for Airflow metadata |
| `AIRFLOW_VERSION` | `2.9.3` | Apache Airflow orchestrator |
| `PROMETHEUS_VERSION`| `v3.5.0` | Prometheus monitoring engine |
| `GRAFANA_VERSION` | `12.1.1` | Grafana metrics visualization |

### Apache Kafka & Ingestion
| Variable | Default Value | Description |
|---|---|---|
| `KAFKA_PORT` | `9092` | Exposed Kafka broker PLAINTEXT port |
| `KAFKA_UI_PORT` | `8085` | Web port for Kafka UI inspection console |
| `KAFKA_CLUSTER_ID` | `MkU3OEVBNTcwNTJENDM2Qk` | Static KRaft cluster ID |
| `KAFKA_TOPIC` | `transactions` | Default topic for credit card streaming |
| `PRODUCER_MODE` | `continuous` | Streaming mode: `continuous` or `batch` |
| `PRODUCER_DELAY_MS` | `100` | Delay between emitted transaction rows |
| `PRODUCER_SHUFFLE` | `true` | Shuffles dataset to prevent temporal ordering bias |

### Object Storage (MinIO)
| Variable | Default Value | Description |
|---|---|---|
| `MINIO_API_PORT` | `9002` | S3 API endpoint port (mapped to container 9000) |
| `MINIO_CONSOLE_PORT`| `9001` | MinIO web management UI |
| `MINIO_ROOT_USER` | `minio` | S3 Access Key / Root administrator username |
| `MINIO_ROOT_PASSWORD`| `ChangeMeMinioPassword2026` | S3 Secret Access Key / Admin password |
| `MINIO_S3_ENDPOINT`| `http://minio:9000` | Internal S3 endpoint used by Spark and Airflow |

### Medallion Table Paths (Delta Lake)
| Variable | Default Value | Description |
|---|---|---|
| `BRONZE_TRANSACTIONS_PATH` | `s3a://bronze/transactions` | Raw event streaming destination |
| `BRONZE_CHECKPOINT_LOCATION`| `s3a://spark-checkpoints/kafka-to-bronze` | Kafka-to-Bronze write-ahead checkpoint |
| `SILVER_TRANSACTIONS_PATH` | `s3a://silver/transactions` | Cleaned and validated transaction table |
| `SILVER_CHECKPOINT_LOCATION`| `s3a://spark-checkpoints/bronze-to-silver` | Bronze-to-Silver checkpoint |
| `QUARANTINE_TRANSACTIONS_PATH`| `s3a://quarantine/transactions` | Dead-letter table for malformed records |

### Machine Learning & Inference
| Variable | Default Value | Description |
|---|---|---|
| `ML_MODEL_NAME` | `fraud-logistic-regression` | Registered model identifier |
| `ML_DECISION_THRESHOLD` | `0.99` | Optimal threshold for positive fraud classification |
| `ML_MIN_RECALL` | `0.80` | Guardrail requirement for candidate promotion |
| `ML_INFERENCE_MODEL_VERSION`| `production` | Active version pointer queried by inference job |
| `ML_PREDICTIONS_PATH` | `s3a://predictions/fraud` | Delta destination for all scored records |
| `ML_ALERTS_PATH` | `s3a://fraud-alerts/fraud` | Delta destination for positive fraud alerts |
| `SLACK_WEBHOOK` | *(Optional)* | Webhook URL for real-time Slack notifications |
