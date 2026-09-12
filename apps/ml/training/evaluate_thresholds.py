import os

import joblib
import numpy as np

from pyspark.sql import SparkSession

from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
)


TRAINING_DATA_PATH = os.getenv(
    "ML_TRAINING_DATA_PATH",
    "s3a://ml-training/creditcard",
)

MODEL_LOCAL_PATH = os.getenv(
    "ML_LOCAL_MODEL_PATH",
    "/tmp/fraud-model/model.joblib",
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

FEATURE_COLUMNS = (
    [f"V{i}" for i in range(1, 29)]
    + ["Amount"]
)

TARGET_COLUMN = "Class"


def main():

    spark = (
        SparkSession.builder
        .appName(
            "FraudThresholdEvaluation"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        print("=" * 70)
        print("FRAUD MODEL THRESHOLD EVALUATION")
        print("=" * 70)

        # ==========================================
        # Load data
        # ==========================================

        df = (
            spark.read
            .format("delta")
            .load(
                TRAINING_DATA_PATH
            )
        )

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
        ].to_numpy()

        y = pandas_df[
            TARGET_COLUMN
        ].to_numpy()

        # ==========================================
        # Reproduce exact test split
        # ==========================================

        from sklearn.model_selection import (
            train_test_split
        )

        (
            X_train,
            X_test,
            y_train,
            y_test,
        ) = train_test_split(
            X,
            y,
            train_size=TRAIN_SIZE,
            random_state=RANDOM_SEED,
            stratify=y,
        )

        # ==========================================
        # Load trained model
        # ==========================================

        MODEL_VERSION = os.getenv("ML_MODEL_VERSION", "1.0.0")
        MODEL_REMOTE_PATH = os.getenv(
            "ML_MODEL_REMOTE_PATH",
            f"s3a://models/fraud-detection/fraud-logistic-regression/{MODEL_VERSION}"
        )

        os.makedirs(os.path.dirname(MODEL_LOCAL_PATH), exist_ok=True)

        sc = spark.sparkContext
        jvm = sc._jvm
        conf = sc._jsc.hadoopConfiguration()
        Path = jvm.org.apache.hadoop.fs.Path

        s3_model_path = Path(f"{MODEL_REMOTE_PATH}/model.joblib")
        fs = s3_model_path.getFileSystem(conf)

        print(f"\nDownloading model from {MODEL_REMOTE_PATH}...")
        fs.copyToLocalFile(
            False,
            s3_model_path,
            Path(f"file://{MODEL_LOCAL_PATH}"),
            True
        )

        print(
            f"\nLoading model: "
            f"{MODEL_LOCAL_PATH}"
        )

        model = joblib.load(
            MODEL_LOCAL_PATH
        )

        probabilities = (
            model.predict_proba(
                X_test
            )[:, 1]
        )

        # ==========================================
        # Threshold evaluation
        # ==========================================

        thresholds = [
                0.50,
                0.55,
                0.60,
                0.65,
                0.70,
                0.75,
                0.80,
                0.82,
                0.84,
                0.86,
                0.88,
                0.90,
                0.92,
                0.94,
                0.96,
                0.98,
                0.99,
            ]
        print("\n" + "=" * 70)
        print("THRESHOLD ANALYSIS")
        print("=" * 70)

        print(
            f"\n{'Threshold':>10}"
            f"{'Precision':>12}"
            f"{'Recall':>12}"
            f"{'F1':>12}"
            f"{'FP':>10}"
            f"{'FN':>10}"
            f"{'FPR':>12}"
        )

        print("-" * 80)

        results = []

        for threshold in thresholds:

            predictions = (
                probabilities >= threshold
            ).astype(int)

            precision = precision_score(
                y_test,
                predictions,
                zero_division=0,
            )

            recall = recall_score(
                y_test,
                predictions,
                zero_division=0,
            )

            f1 = f1_score(
                y_test,
                predictions,
                zero_division=0,
            )

            tn, fp, fn, tp = (
                confusion_matrix(
                    y_test,
                    predictions,
                    labels=[0, 1],
                )
                .ravel()
            )

            fpr = (
                fp / (fp + tn)
                if (fp + tn) > 0
                else 0
            )

            print(
                f"{threshold:>10.2f}"
                f"{precision:>12.4f}"
                f"{recall:>12.4f}"
                f"{f1:>12.4f}"
                f"{fp:>10}"
                f"{fn:>10}"
                f"{fpr:>12.6f}"
            )

            results.append(
                {
                    "threshold": threshold,
                    "precision": precision,
                    "recall": recall,
                    "f1": f1,
                    "fp": int(fp),
                    "fn": int(fn),
                    "fpr": fpr,
                }
            )

        # ==========================================
        # Best F1 threshold
        # ==========================================

        best_f1 = max(
            results,
            key=lambda x: x["f1"],
        )

        print("\n" + "=" * 70)
        print("BEST F1 THRESHOLD")
        print("=" * 70)

        print(
            f"Threshold: "
            f"{best_f1['threshold']:.2f}"
        )

        print(
            f"Precision: "
            f"{best_f1['precision']:.4f}"
        )

        print(
            f"Recall: "
            f"{best_f1['recall']:.4f}"
        )

        print(
            f"F1: "
            f"{best_f1['f1']:.4f}"
        )

        print(
            f"False positives: "
            f"{best_f1['fp']}"
        )

        print(
            f"False negatives: "
            f"{best_f1['fn']}"
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()