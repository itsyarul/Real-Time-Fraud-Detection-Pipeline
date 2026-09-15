# Architecture Decision Records (ADRs)

This document records the architectural and technology decisions made in the **Real-Time Fraud Detection Platform**, explaining the rationale, alternatives considered, and trade-offs.

---

## Summary of Decisions

| ADR | Title | Decision | Status |
|---|---|---|---|
| [ADR-001](#adr-001--apache-spark-401-for-stream--batch-processing) | Stream & Batch Processing Engine | Apache Spark 4.0.1 | Accepted |
| [ADR-002](#adr-002--delta-lake-401-as-storage-layer) | Lakehouse Storage Format | Delta Lake 4.0.1 | Accepted |
| [ADR-003](#adr-003--minio-as-s3-compatible-object-storage) | Object Storage Infrastructure | MinIO | Accepted |
| [ADR-004](#adr-004--apache-kafka-in-kraft-mode-for-ingestion) | Message Broker & Ingestion | Apache Kafka 4.3.1 (KRaft) | Accepted |
| [ADR-005](#adr-005--great-expectations-for-automated-data-quality) | Data Quality & Validation | Great Expectations 1.19.1 | Accepted |
| [ADR-006](#adr-006--apache-airflow-for-ml-continuous-training-ctcd) | ML Lifecycle Orchestration | Apache Airflow 2.9.3 | Accepted |
| [ADR-007](#adr-007--github-actions-for-software-cicd) | Software CI/CD Pipeline | GitHub Actions | Accepted |
| [ADR-008](#adr-008--decision-threshold-optimization-at-099) | Decision Threshold Strategy | Precision-Recall Sweep (Threshold = 0.99) | Accepted |
| [ADR-009](#adr-009--scikit-learn-with-balanced-reweighting) | Classification Model Selection | Logistic Regression with Class Reweighting | Accepted |

---

## ADR-001 — Apache Spark 4.0.1 for Stream & Batch Processing

- **Context**: The platform requires continuous micro-batch streaming from Kafka, high-throughput feature transformations, and large-scale data preparation for ML training.
- **Decision**: Adopt **Apache Spark 4.0.1** Structured Streaming.
- **Alternatives Considered**: Apache Flink, Apache Storm, pure Kafka Streams (Java).
- **Rationale**:
  1. Unified API across streaming and batch workflows in Python (PySpark).
  2. Native integration with Delta Lake 4.0.1.
  3. Seamless distributed batch processing for historical dataset generation and validation.
- **Trade-offs**: Slightly higher micro-batch latency (100–500ms) compared to Flink's event-by-event processing (<50ms), but perfectly acceptable for financial fraud scoring while offering far simpler operational overhead in PySpark.

---

## ADR-002 — Delta Lake 4.0.1 as Storage Layer

- **Context**: Standard object storage (raw Parquet/CSV on S3) suffers from lack of ACID transactions, read/write concurrency conflicts, dirty reads during streaming, and no easy way to update/merge records.
- **Decision**: Store all Medallion layers (Bronze, Silver, Gold, Quarantine, Predictions, Alerts) in **Delta Lake 4.0.1**.
- **Alternatives Considered**: Apache Iceberg, Apache Hudi, vanilla Parquet.
- **Rationale**:
  1. **ACID Transactions**: Multiple Spark streaming queries and Airflow batch readers can interact concurrently without corruption.
  2. **Atomic `MERGE` Upserts**: Guarantees end-to-end idempotency when replaying batches or recovering from failures.
  3. **Time Travel**: Enables reproducible ML training by querying exact historical table snapshots (`VERSION AS OF`).
  4. **Schema Enforcement & Evolution**: Prevents silent schema corruption from poison pill messages.

---

## ADR-003 — MinIO as S3-Compatible Object Storage

- **Context**: Cloud development requires S3 access, but local development and integration testing require a 100% self-contained, reproducible, cost-free environment.
- **Decision**: Use **MinIO** as the central object storage engine, accessed via the S3A protocol (`s3a://`).
- **Alternatives Considered**: Local file system paths (`file://`), LocalStack, actual AWS S3.
- **Rationale**:
  1. High performance, light footprint, and complete AWS S3 API fidelity.
  2. Same code and S3A configurations work unchanged when deploying to AWS S3 in production.
  3. Easy bucket initialization via `minio/mc` container during startup.

---

## ADR-004 — Apache Kafka in KRaft Mode for Ingestion

- **Context**: Transactions must be decoupled from downstream consumers so ingestion can handle sudden traffic spikes without dropping events.
- **Decision**: Deploy **Apache Kafka 4.3.1** operating in **KRaft** mode.
- **Alternatives Considered**: Kafka with ZooKeeper, RabbitMQ, AWS Kinesis.
- **Rationale**:
  1. KRaft eliminates the separate ZooKeeper container, drastically reducing memory footprint and operational complexity.
  2. Industry-standard throughput, horizontal partition scalability, and offset replayability.
  3. Seamless integration with Spark Structured Streaming via official Kafka connectors.

---

## ADR-005 — Great Expectations for Automated Data Quality

- **Context**: Data drift, malformed transactions, or missing fields could silently degrade ML model accuracy and fraud alert reliability.
- **Decision**: Embed **Great Expectations 1.19.1** into the pipeline at the Silver layer.
- **Alternatives Considered**: Custom SQL assertions, PyDeequ, dbt tests alone.
- **Rationale**:
  1. Declarative expectation suites that generate auditable validation reports.
  2. Programmatic validation in Python integrated directly into the Airflow DAG (`validate_silver`).
  3. Automatic routing of failing records to the `quarantine` Delta table before ML training.

---

## ADR-006 — Apache Airflow for ML Continuous Training (CT/CD)

- **Context**: ML models degrade over time as fraud patterns evolve. Periodic dataset rebuilding, data quality validation, retraining, threshold evaluation, and model promotion must be coordinated reliably.
- **Decision**: Use **Apache Airflow 2.9.3** with PostgreSQL metadata backend for ML lifecycle orchestration.
- **Alternatives Considered**: Cron jobs, Kubeflow, MLflow Pipelines, manual retraining.
- **Rationale**:
  1. Full task dependency management (`validate_silver >> build_ml_dataset >> train_model >> evaluate_model >> validate_model >> promote_model`).
  2. Automatic retries, execution history, task logs, and dynamic run identification.
  3. Clean separation between **Application CI/CD** (handled by GitHub Actions) and **Data/ML CT/CD** (handled by Airflow).

---

## ADR-007 — GitHub Actions for Software CI/CD

- **Context**: The codebase spans multiple services (Airflow, Spark, Producer, Metrics Pusher) and needs automated linting, unit testing, Docker image building, and vulnerability scanning.
- **Decision**: Implement a multi-workflow **GitHub Actions** CI/CD pipeline.
- **Workflows**:
  - `ci.yml`: Code formatting, flake8, black, isort, docker-compose syntax validation.
  - `tests.yml`: Pytest unit test execution with mocked S3/MinIO fixtures.
  - `docker-build.yml`: Automated container build and push to GitHub Container Registry (GHCR).
  - `release.yml`: Git tag-driven semantic releases.
  - `security-scan.yml`: Trivy container CVE scans and pip-audit dependency audits.

---

## ADR-008 — Decision Threshold Optimization at 0.99

- **Context**: Default classification threshold `0.5` is designed for balanced 50/50 datasets. In credit card fraud, fraud represents only **0.172%** (492 out of 284,807) of transactions.
- **Decision**: Perform empirical threshold tuning and set the production operating threshold to **`0.99`**.
- **Rationale**:
  1. At threshold `0.5`, the model suffers from excessive false positives, overwhelming compliance and investigation teams.
  2. Because the model is trained with `class_weight='balanced'`, uncalibrated positive probabilities are elevated across normal transactions.
  3. Sweeping thresholds between `0.10` and `0.99` demonstrated that threshold `0.99` delivers the optimal operating point:
     - **Recall**: `~85.7%` (captures the vast majority of true fraud)
     - **Precision**: `~54.9%` (more than half of flagged alerts are true fraud)
     - **F1 Score**: `~0.669` (best trade-off between investigation load and risk mitigation)

---

## ADR-009 — Scikit-Learn with Balanced Reweighting

- **Context**: Need a fast, auditable, reproducible model that executes real-time inference within Spark streaming micro-batches without GPU requirements.
- **Decision**: Use `StandardScaler` + `LogisticRegression(class_weight='balanced', max_iter=1000)` serialized with `joblib`.
- **Alternatives Considered**: XGBoost, LightGBM, Deep Neural Networks.
- **Rationale**:
  1. Sub-millisecond CPU inference per micro-batch, ideal for streaming execution.
  2. Fully explainable coefficients for financial compliance.
  3. Low memory footprint when broadcasted across Spark executors.
