# Machine Learning Lifecycle & CT/CD

This document details the machine learning dataset preparation, class imbalance handling, training pipeline, decision threshold optimization, evaluation metrics, and automated model promotion managed via Apache Airflow.

![Airflow DAG](Airflow%20DAG.png)

---

## 1. The Machine Learning Problem: Fraud Detection

Credit card fraud detection is characterized by **extreme class imbalance**:
- **Total Transactions**: 284,807
- **Fraudulent Transactions (Class = 1)**: 492
- **Legitimate Transactions (Class = 0)**: 284,315
- **Fraud Prevalence**: **0.172%** (less than 2 fraud cases per 1,000 transactions)

### Why Accuracy is a Deceptive Metric
A naive model predicting `0` (legitimate) for every single transaction achieves **99.83% accuracy**, yet catches **0% of fraud**, resulting in complete financial loss.
Therefore, our pipeline evaluates model performance strictly using:
- **Precision**: When the model flags fraud, how often is it actually fraud?
- **Recall (Sensitivity)**: What proportion of all actual fraud cases did the model catch?
- **F1 Score**: Harmonic mean of Precision and Recall.
- **PR-AUC (Precision-Recall Area Under Curve)**: Superior to ROC-AUC for extreme class imbalance.
- **ROC-AUC**: Evaluates overall separation capability across all thresholds.

---

## 2. Airflow ML Continuous Training Pipeline (`fraud_ml_pipeline`)

The end-to-end ML lifecycle is automated as a directed acyclic graph (DAG) in Apache Airflow 2.9.3:

```
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ validate_silver │ ──► │ build_ml_dataset │ ──► │   train_model    │
└─────────────────┘     └──────────────────┘     └────────┬─────────┘
                                                          │
                                                          ▼
┌─────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  promote_model  │ ◄── │  validate_model  │ ◄── │  evaluate_model  │
└─────────────────┘     └──────────────────┘     └──────────────────┘
```

### Stage Breakdown

1. **`validate_silver` (`apps/quality/validate_silver.py`)**:
   - Executes the 37-rule Great Expectations suite against `s3a://silver/transactions`.
   - Halts the retraining pipeline immediately if data quality drops below expectations.

2. **`build_ml_dataset` (`apps/ml/ingestion/prepare_training_data.py`)**:
   - Reads validated Silver Delta records.
   - Extracts 28 PCA features (`V1` to `V28`) and `Amount`.
   - Partitions data deterministically into train/test splits (80% train / 20% test, `random_state=42`).
   - Writes the snapshot to `s3a://ml-training/creditcard`.

3. **`train_model` (`apps/ml/training/train_model.py`)**:
   - Trains a Scikit-Learn `Pipeline` composed of:
     - `StandardScaler()`: Zero-mean, unit-variance normalization.
     - `LogisticRegression(class_weight='balanced', max_iter=1000, random_state=42)`: Applies inverse class frequency weighting to penalize fraud misclassification heavily.
   - Logs metrics, confusion matrix, hyperparameters, and feature weights into `metadata.json`.
   - Serializes model artifact `model.joblib` to MinIO at `s3a://models/fraud-detection/fraud-logistic-regression/<run_id>/`.

4. **`evaluate_model` (`apps/ml/training/evaluate_thresholds.py`)**:
   - Sweeps decision thresholds from `0.10` to `0.99` on the holdout test set.
   - Finds the operating point satisfying the configured `min_recall` requirement.

5. **`validate_model` (`apps/ml/training/verify_model.py`)**:
   - Performs automated sanity checks on the candidate model artifact:
     - Verifies `model.joblib` loads cleanly and executes test inference.
     - Confirms test set Recall exceeds threshold requirement (`min_recall >= 0.80`).
     - Verifies `metadata.json` contains all required schema fields.

6. **`promote_model` (`apps/ml/training/promote_model.py`)**:
   - Reads candidate `F1` score vs current active production `F1` score.
   - If candidate F1 > production F1 (or if no production model exists), atomically copies the candidate model to:
     `s3a://models/fraud-detection/fraud-logistic-regression/production/`
   - Real-time inference picks up the new production model automatically.

---

## 3. Threshold Optimization Strategy

Default classification models use a decision boundary of `P(fraud) > 0.5`. However, when training with `class_weight='balanced'`, predicted probabilities are calibrated for a 50/50 prior, causing excessive false alarms on real-world 0.17% data.

Our evaluation script sweeps thresholds and selects the optimal boundary:

| Decision Threshold | Precision | Recall | F1 Score | Notes |
|---|---|---|---|---|
| `0.50` (Default) | 0.082 | 0.918 | 0.151 | Too many false alarms (overwhelms ops) |
| `0.80` | 0.231 | 0.898 | 0.367 | High false positive rate |
| `0.95` | 0.412 | 0.878 | 0.561 | Improved balance |
| **`0.99` (Production)** | **0.549** | **0.857** | **0.669** | **Optimal Operating Point** |

### Production Operating Point
- **Threshold**: **`0.99`**
- **Recall**: **`85.7%`** (detects 84 out of 98 fraud cases in test set)
- **Precision**: **`54.9%`** (1 in every 1.8 alerts is true fraud)
- **F1 Score**: **`0.669`**
- **ROC-AUC**: **`0.978`**
- **PR-AUC**: **`0.764`**

---

## 4. MinIO Model Registry Structure

All model artifacts, evaluation histories, and production deployments are organized in MinIO object storage under `s3a://models/fraud-detection/fraud-logistic-regression/`:

```
s3a://models/fraud-detection/fraud-logistic-regression/
│
├── 1.0.0/
│   ├── model.joblib          # Scikit-Learn serialized pipeline
│   └── metadata.json         # Metrics (F1, Precision, Recall, PR-AUC, ROC-AUC)
│
├── manual__2026-08-31T.../  # Run-ID candidate versions
│   ├── model.joblib
│   └── metadata.json
│
└── production/               # Active model queried by streaming inference
    ├── model.joblib
    └── metadata.json
```
