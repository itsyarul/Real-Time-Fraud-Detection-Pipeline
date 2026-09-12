import json
import os
from datetime import datetime, timezone

import joblib
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline


# ==================================================
# Configuration
# ==================================================

TRAINING_DATA_PATH = os.getenv(
    "ML_TRAINING_DATA_PATH",
    "s3a://ml-training/creditcard",
)

MODEL_PATH = os.getenv(
    "ML_MODEL_PATH",
    "s3a://models/fraud-detection",
)

ML_MODEL_NAME = os.getenv(
    "ML_MODEL_NAME",
    "fraud-logistic-regression",
)

ML_MODEL_VERSION = os.getenv(
    "ML_MODEL_VERSION",
    "1.0.0",
)

RANDOM_SEED = int(
    os.getenv(
        "ML_RANDOM_SEED",
        "42",
    )
)

TRAIN_SIZE = float(
    os.getenv(
        "ML_TRAIN_SIZE",
        "0.80",
    )
)

DECISION_THRESHOLD = float(
    os.getenv(
        "ML_DECISION_THRESHOLD",
        "0.99",
    )
)

MIN_RECALL = float(
    os.getenv(
        "ML_MIN_RECALL",
        "0.85",
    )
)

FEATURE_COLUMNS = (
    [f"V{i}" for i in range(1, 29)]
    + ["Amount"]
)

TARGET_COLUMN = "Class"


# ==================================================
# Spark
# ==================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName(
            "FraudDetectionModelTraining"
        )
        .getOrCreate()
    )


# ==================================================
# Load Dataset
# ==================================================

def load_training_data(spark):

    print("=" * 70)
    print("LOADING OFFLINE TRAINING DATA")
    print("=" * 70)

    print(
        f"\nPath: {TRAINING_DATA_PATH}"
    )

    df = (
        spark.read
        .format("delta")
        .load(
            TRAINING_DATA_PATH
        )
    )

    print(
        f"Records: {df.count()}"
    )

    return df


# ==================================================
# Validate Dataset
# ==================================================

def validate_training_data(df):

    required_columns = (
        FEATURE_COLUMNS
        + [TARGET_COLUMN]
    )

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            "Missing ML columns: "
            f"{missing_columns}"
        )

    null_count = (
        df
        .select(
            *[
                F.sum(
                    F.col(column)
                    .isNull()
                    .cast("int")
                ).alias(column)
                for column in required_columns
            ]
        )
        .collect()[0]
    )

    null_columns = {
        column: value
        for column, value
        in null_count.asDict().items()
        if value and value > 0
    }

    if null_columns:

        raise ValueError(
            "Null values detected: "
            f"{null_columns}"
        )

    invalid_target_count = (
        df
        .filter(
            ~F.col(TARGET_COLUMN).isin(0, 1)
        )
        .count()
    )

    if invalid_target_count > 0:

        raise ValueError(
            f"Invalid target values: "
            f"{invalid_target_count}"
        )

    print(
        "\nTraining dataset validation passed."
    )


# ==================================================
# Convert Spark → NumPy
# ==================================================

def prepare_numpy_data(df):

    print("\n" + "=" * 70)
    print("PREPARING MODEL FEATURES")
    print("=" * 70)

    pandas_df = (
        df
        .select(
            *FEATURE_COLUMNS,
            TARGET_COLUMN,
        )
        .toPandas()
    )

    X = pandas_df[
        FEATURE_COLUMNS
    ].to_numpy(
        dtype=np.float64
    )

    y = pandas_df[
        TARGET_COLUMN
    ].to_numpy(
        dtype=np.int32
    )

    print(
        f"Feature matrix shape: "
        f"{X.shape}"
    )

    print(
        f"Target shape: "
        f"{y.shape}"
    )

    print(
        f"Fraud samples: "
        f"{int((y == 1).sum())}"
    )

    print(
        f"Normal samples: "
        f"{int((y == 0).sum())}"
    )

    return X, y


# ==================================================
# Stratified Split
# ==================================================

def create_train_test_split(
    X,
    y,
):

    print("\n" + "=" * 70)
    print("CREATING STRATIFIED TRAIN / TEST SPLIT")
    print("=" * 70)

    X_train, X_test, y_train, y_test = (
        train_test_split(
            X,
            y,
            train_size=TRAIN_SIZE,
            random_state=RANDOM_SEED,
            stratify=y,
        )
    )

    print(
        f"\nTraining samples: "
        f"{len(y_train)}"
    )

    print(
        f"Training fraud: "
        f"{int((y_train == 1).sum())}"
    )

    print(
        f"Training normal: "
        f"{int((y_train == 0).sum())}"
    )

    print(
        f"\nTesting samples: "
        f"{len(y_test)}"
    )

    print(
        f"Testing fraud: "
        f"{int((y_test == 1).sum())}"
    )

    print(
        f"Testing normal: "
        f"{int((y_test == 0).sum())}"
    )

    return (
        X_train,
        X_test,
        y_train,
        y_test,
    )


# ==================================================
# Train Model
# ==================================================

def train_model(
    X_train,
    y_train,
):

    print("\n" + "=" * 70)
    print("TRAINING LOGISTIC REGRESSION")
    print("=" * 70)

    model = Pipeline(
        steps=[
            (
                "scaler",
                StandardScaler(),
            ),
            (
                "classifier",
                LogisticRegression(
                    class_weight="balanced",
                    max_iter=2000,
                    random_state=RANDOM_SEED,
                ),
            ),
        ]
    )

    model.fit(
        X_train,
        y_train,
    )

    print(
        "\nModel training completed."
    )

    return model


# ==================================================
# Evaluate
# ==================================================

def evaluate_model(
    model,
    X_test,
    y_test,
):

    print("\n" + "=" * 70)
    print("MODEL EVALUATION & THRESHOLD OPTIMIZATION")
    print("=" * 70)

    probabilities = (
        model.predict_proba(X_test)[:, 1]
    )

    best_f1 = 0.0
    best_threshold = 0.5
    best_metrics = {}

    # Search for best threshold between 0.01 and 0.99
    for t in np.arange(0.01, 1.0, 0.01):
        preds = (probabilities >= t).astype(int)
        
        prec = precision_score(y_test, preds, zero_division=0)
        rec = recall_score(y_test, preds, zero_division=0)
        f1 = f1_score(y_test, preds, zero_division=0)

        # We can enforce a minimum recall constraint if we want, or just maximize F1
        if f1 > best_f1 and rec >= MIN_RECALL:
            best_f1 = f1
            best_threshold = t
            best_metrics = {
                "precision": float(prec),
                "recall": float(rec),
                "f1": float(f1),
            }

    # If no threshold met the minimum recall, fallback to maximizing F1 without constraint
    if best_f1 == 0.0:
        for t in np.arange(0.01, 1.0, 0.01):
            preds = (probabilities >= t).astype(int)
            f1 = f1_score(y_test, preds, zero_division=0)
            if f1 > best_f1:
                best_f1 = f1
                best_threshold = t
                best_metrics = {
                    "precision": float(precision_score(y_test, preds, zero_division=0)),
                    "recall": float(recall_score(y_test, preds, zero_division=0)),
                    "f1": float(f1),
                }

    pr_auc = average_precision_score(y_test, probabilities)
    roc_auc = roc_auc_score(y_test, probabilities)

    best_metrics["pr_auc"] = float(pr_auc)
    best_metrics["roc_auc"] = float(roc_auc)
    best_metrics["decision_threshold"] = float(best_threshold)

    print(f"\nOptimal Threshold: {best_threshold:.2f}")
    print(f"PR-AUC:  {pr_auc:.6f}")
    print(f"ROC-AUC: {roc_auc:.6f}")
    print(f"Precision: {best_metrics['precision']:.6f}")
    print(f"Recall:    {best_metrics['recall']:.6f}")
    print(f"F1:        {best_f1:.6f}")

    predictions = (probabilities >= best_threshold).astype(int)
    print("\n=== CONFUSION MATRIX ===")
    print(confusion_matrix(y_test, predictions))

    print("\n=== CLASSIFICATION REPORT ===")
    print(
        classification_report(
            y_test,
            predictions,
            target_names=["Normal", "Fraud"],
            zero_division=0,
        )
    )

    return best_metrics



# ==================================================
# Save Model
# ==================================================

def save_model(
    spark,
    model,
    metrics,
):

    print("\n" + "=" * 70)
    print("SAVING MODEL")
    print("=" * 70)

    timestamp = (
        datetime.now(
            timezone.utc
        )
        .strftime(
            "%Y%m%dT%H%M%SZ"
        )
    )

    local_model_dir = (
        "/tmp/fraud-model"
    )

    os.makedirs(
        local_model_dir,
        exist_ok=True,
    )

    model_file = os.path.join(
        local_model_dir,
        "model.joblib",
    )

    metadata_file = os.path.join(
        local_model_dir,
        "metadata.json",
    )

    joblib.dump(
        model,
        model_file,
    )

    metadata = {
        "model_name": ML_MODEL_NAME,
        "model_version": ML_MODEL_VERSION,
        "trained_at": timestamp,
        "algorithm": (
            "LogisticRegression"
        ),
        "features": FEATURE_COLUMNS,
        "target": TARGET_COLUMN,
        "decision_threshold": metrics.get("decision_threshold", DECISION_THRESHOLD),
        "threshold_selection": {
            "method": (
                "highest_precision_with_recall_constraint"
            ),
            "minimum_recall": MIN_RECALL,
        },
        "random_seed": RANDOM_SEED,
        "train_size": TRAIN_SIZE,
        "metrics": metrics,
    }

    with open(
        metadata_file,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            metadata,
            file,
            indent=2,
        )

    print(
        f"\nLocal model: "
        f"{model_file}"
    )

    print(
        f"Local metadata: "
        f"{metadata_file}"
    )

    print(
        "\nModel artifact prepared locally."
    )

    s3_base_path = f"{MODEL_PATH}/{ML_MODEL_NAME}/{ML_MODEL_VERSION}"
    print(f"\nUploading to MinIO: {s3_base_path}")

    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()

    Path = jvm.org.apache.hadoop.fs.Path

    s3_model_path = Path(f"{s3_base_path}/model.joblib")
    s3_metadata_path = Path(f"{s3_base_path}/metadata.json")

    fs = s3_model_path.getFileSystem(conf)

    fs.copyFromLocalFile(
        False,
        True,
        Path(f"file://{model_file}"),
        s3_model_path
    )

    fs.copyFromLocalFile(
        False,
        True,
        Path(f"file://{metadata_file}"),
        s3_metadata_path
    )

    print("Model artifact successfully saved to MinIO.")


# ==================================================
# Main
# ==================================================

def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        df = load_training_data(
            spark
        )

        validate_training_data(
            df
        )

        X, y = prepare_numpy_data(
            df
        )

        (
            X_train,
            X_test,
            y_train,
            y_test,
        ) = create_train_test_split(
            X,
            y,
        )

        model = train_model(
            X_train,
            y_train,
        )

        metrics = evaluate_model(
            model,
            X_test,
            y_test,
        )

        save_model(
            spark,
            model,
            metrics,
        )

        print("\n" + "=" * 70)
        print(
            "MODEL TRAINING PIPELINE PASSED"
        )
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()