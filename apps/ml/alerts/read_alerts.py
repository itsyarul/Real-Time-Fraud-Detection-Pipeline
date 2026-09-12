import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


ALERTS_PATH = os.getenv(
    "ML_ALERTS_PATH",
    "s3a://fraud-alerts/fraud",
)

EXPECTED_MODEL_NAME = os.getenv(
    "ML_INFERENCE_MODEL_NAME",
    "fraud-logistic-regression",
)

EXPECTED_MODEL_VERSION = os.getenv(
    "ML_INFERENCE_MODEL_VERSION",
    "1.0.1",
)

EXPECTED_THRESHOLD = float(
    os.getenv(
        "ML_INFERENCE_THRESHOLD",
        "0.99",
    )
)


REQUIRED_COLUMNS = [
    "event_id",
    "fraud_probability",
    "fraud_prediction",
    "model_name",
    "model_version",
    "decision_threshold",
    "prediction_timestamp",
]


def create_spark_session():

    return (
        SparkSession.builder
        .appName("FraudDetection-AlertReader")
        .getOrCreate()
    )


def fail(message):

    print()
    print("=" * 70)
    print("ALERT VALIDATION FAILED")
    print("=" * 70)
    print(message)

    sys.exit(1)


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("FRAUD ALERT CONSUMER")
    print("=" * 70)

    print()
    print(
        f"Reading alerts from:\n"
        f"{ALERTS_PATH}"
    )

    # ------------------------------------------------
    # Read Delta alert table
    # ------------------------------------------------

    try:

        alerts_df = (
            spark.read
            .format("delta")
            .load(ALERTS_PATH)
        )

    except Exception as exc:

        fail(
            f"Unable to read alert Delta table:\n{exc}"
        )

    # ------------------------------------------------
    # Schema validation
    # ------------------------------------------------

    print()
    print("=== SCHEMA VALIDATION ===")

    actual_columns = alerts_df.columns

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in actual_columns
    ]

    if missing_columns:

        fail(
            "Missing required columns: "
            + ", ".join(missing_columns)
        )

    print("SUCCESS: Alert schema is valid.")

    # ------------------------------------------------
    # Record counts
    # ------------------------------------------------

    total_alerts = alerts_df.count()

    unique_event_ids = (
        alerts_df
        .select("event_id")
        .distinct()
        .count()
    )

    duplicate_alerts = (
        total_alerts
        - unique_event_ids
    )

    print()
    print("=== ALERT COUNTS ===")

    print(
        f"Total alerts:        {total_alerts}"
    )

    print(
        f"Unique event_ids:    {unique_event_ids}"
    )

    print(
        f"Duplicate event_ids: {duplicate_alerts}"
    )

    if duplicate_alerts != 0:

        (
            alerts_df
            .groupBy("event_id")
            .count()
            .filter(
                F.col("count") > 1
            )
            .orderBy(
                F.desc("count")
            )
            .show(20, truncate=False)
        )

        fail(
            "Duplicate event_ids found in fraud alerts."
        )

    print(
        "SUCCESS: Alert event_id uniqueness verified."
    )

    # ------------------------------------------------
    # Prediction validation
    # ------------------------------------------------

    invalid_predictions = (
        alerts_df
        .filter(
            F.col("fraud_prediction") != 1
        )
        .count()
    )

    print()
    print("=== FRAUD PREDICTION VALIDATION ===")

    print(
        f"Non-fraud records in alert table: "
        f"{invalid_predictions}"
    )

    if invalid_predictions != 0:

        fail(
            "Alert table contains records "
            "where fraud_prediction != 1."
        )

    print(
        "SUCCESS: Every alert represents a fraud prediction."
    )

    # ------------------------------------------------
    # Probability validation
    # ------------------------------------------------

    invalid_probability = (
        alerts_df
        .filter(
            (F.col("fraud_probability") < 0)
            |
            (F.col("fraud_probability") > 1)
        )
        .count()
    )

    if invalid_probability != 0:

        fail(
            "Invalid fraud_probability values found."
        )

    print(
        "SUCCESS: Fraud probabilities are within [0, 1]."
    )

    # ------------------------------------------------
    # Model validation
    # ------------------------------------------------

    model_versions = (
        alerts_df
        .select(
            "model_name",
            "model_version",
            "decision_threshold",
        )
        .distinct()
    )

    print()
    print("=== MODEL INFORMATION ===")

    model_versions.show(
        truncate=False
    )

    invalid_model = (
        alerts_df
        .filter(
            (F.col("model_name") != EXPECTED_MODEL_NAME)
            |
            (F.col("model_version") != EXPECTED_MODEL_VERSION)
            |
            (
                F.abs(
                    F.col("decision_threshold")
                    - F.lit(EXPECTED_THRESHOLD)
                )
                > F.lit(0.000001)
            )
        )
        .count()
    )

    if invalid_model != 0:

        fail(
            "Model metadata mismatch detected."
        )

    print(
        "SUCCESS: Model metadata verified."
    )

    # ------------------------------------------------
    # Top alerts
    # ------------------------------------------------

    print()
    print("=== TOP FRAUD ALERTS ===")

    (
        alerts_df
        .select(
            "event_id",
            "fraud_probability",
            "fraud_prediction",
            "model_name",
            "model_version",
            "decision_threshold",
            "prediction_timestamp",
        )
        .orderBy(
            F.desc(
                "fraud_probability"
            )
        )
        .show(
            20,
            truncate=False,
        )
    )

    # ------------------------------------------------
    # Probability statistics
    # ------------------------------------------------

    print()
    print("=== FRAUD PROBABILITY STATISTICS ===")

    (
        alerts_df
        .select(
            F.min(
                "fraud_probability"
            ).alias("min"),
            F.avg(
                "fraud_probability"
            ).alias("mean"),
            F.max(
                "fraud_probability"
            ).alias("max"),
        )
        .show(
            truncate=False
        )
    )

    # ------------------------------------------------
    # Final summary
    # ------------------------------------------------

    print()
    print("=" * 70)
    print("ALERT VALIDATION SUMMARY")
    print("=" * 70)

    print(
        f"Total alerts:        {total_alerts}"
    )

    print(
        f"Unique event_ids:    {unique_event_ids}"
    )

    print(
        f"Duplicate event_ids: {duplicate_alerts}"
    )

    print(
        f"Model:               {EXPECTED_MODEL_NAME}"
    )

    print(
        f"Version:             {EXPECTED_MODEL_VERSION}"
    )

    print(
        f"Threshold:           {EXPECTED_THRESHOLD}"
    )

    print()
    print(
        "FRAUD ALERT VALIDATION PASSED."
    )

    spark.stop()


if __name__ == "__main__":
    main()