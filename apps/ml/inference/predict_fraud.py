import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Fix path to allow importing from common
CURRENT_FILE = os.path.abspath(__file__)
ML_ROOT = os.path.dirname(os.path.dirname(CURRENT_FILE))
if ML_ROOT not in sys.path:
    sys.path.insert(0, ML_ROOT)

from common.model_registry import ModelRegistry


MODEL_NAME = os.getenv(
    "ML_INFERENCE_MODEL_NAME",
    "fraud-logistic-regression",
)

MODEL_VERSION = os.getenv(
    "ML_INFERENCE_MODEL_VERSION",
    "production",
)

CONFIGURED_THRESHOLD = float(
    os.getenv(
        "ML_INFERENCE_THRESHOLD",
        "0.99",
    )
)

SILVER_PATH = os.getenv(
    "SILVER_TRANSACTIONS_PATH",
    "s3a://silver/transactions",
)

PREDICTIONS_PATH = os.getenv(
    "ML_PREDICTIONS_PATH",
    "s3a://predictions/fraud",
)

ALERTS_PATH = os.getenv(
    "ML_ALERTS_PATH",
    "s3a://fraud-alerts/fraud",
)

CHECKPOINT_PATH = os.getenv(
    "ML_INFERENCE_CHECKPOINT",
    "s3a://spark-checkpoints/fraud-inference",
)

MONITORING_PATH = os.getenv(
    "ML_MONITORING_PATH",
    "s3a://monitoring/ml-inference",
)


FEATURE_COLUMNS = (
    [f"V{i}" for i in range(1, 29)]
    + ["Amount"]
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName(
            "FraudDetection-ML-Inference"
        )
        .getOrCreate()
    )


def load_model():

    print("=" * 70)
    print("LOADING FRAUD MODEL")
    print("=" * 70)

    registry = ModelRegistry()

    model, metadata = registry.load(
        MODEL_NAME,
        MODEL_VERSION,
    )

    # We do NOT pass expected_threshold anymore, because we want it to be dynamic!
    registry.validate_metadata(
        metadata=metadata,
        expected_model_name=MODEL_NAME,
        expected_version=MODEL_VERSION,
        expected_threshold=None,
    )

    # Extract the dynamic optimal threshold that was saved during training
    optimal_threshold = float(metadata.get("decision_threshold", 0.5))

    print(f"Model loaded: {MODEL_NAME}")
    print(f"Version: {MODEL_VERSION}")
    print(f"Dynamic Decision threshold (from metadata): {optimal_threshold}")

    return model, optimal_threshold


def predict_partition(
    rows,
    model_broadcast,
    threshold,
):

    import numpy as np

    model = model_broadcast.value

    rows = list(rows)

    if not rows:
        return iter([])

    feature_matrix = np.array(
        [
            [
                float(
                    getattr(row, column)
                )
                for column in FEATURE_COLUMNS
            ]
            for row in rows
        ],
        dtype=np.float64,
    )

    probabilities = (
        model.predict_proba(
            feature_matrix
        )[:, 1]
    )

    predictions = (
        probabilities
        >= threshold
    ).astype(int)

    prediction_timestamp = (
        datetime.now(
            timezone.utc
        )
    )

    for row, probability, prediction in zip(
        rows,
        probabilities,
        predictions,
    ):

        yield (
            row.event_id,
            float(probability),
            int(prediction),
            MODEL_NAME,
            MODEL_VERSION,
            threshold,
            prediction_timestamp.isoformat(),
        )


def write_monitoring_metrics(
    spark,
    batch_id,
    threshold,
    predictions_df,
    input_records,
):
    """
    Write one monitoring record per inference micro-batch.
    """

    prediction_records = predictions_df.count()

    fraud_predictions = (
        predictions_df
        .filter(
            F.col("fraud_prediction") == 1
        )
        .count()
    )

    normal_predictions = (
        predictions_df
        .filter(
            F.col("fraud_prediction") == 0
        )
        .count()
    )

    stats = (
        predictions_df
        .select(
            F.avg(
                "fraud_probability"
            ).alias(
                "avg_fraud_probability"
            ),
            F.max(
                "fraud_probability"
            ).alias(
                "max_fraud_probability"
            ),
        )
        .collect()[0]
    )

    fraud_rate = (
        fraud_predictions / prediction_records
        if prediction_records > 0
        else 0.0
    )

    metrics = [
        (
            int(batch_id),
            datetime.now(
                timezone.utc
            ),
            int(input_records),
            int(prediction_records),
            int(fraud_predictions),
            int(normal_predictions),
            float(fraud_rate),
            float(
                stats["avg_fraud_probability"]
                or 0.0
            ),
            float(
                stats["max_fraud_probability"]
                or 0.0
            ),
            MODEL_NAME,
            MODEL_VERSION,
            float(threshold),
        )
    ]

    metrics_df = spark.createDataFrame(
        metrics,
        [
            "batch_id",
            "metric_timestamp",
            "input_records",
            "prediction_records",
            "fraud_predictions",
            "normal_predictions",
            "fraud_rate",
            "avg_fraud_probability",
            "max_fraud_probability",
            "model_name",
            "model_version",
            "decision_threshold",
        ],
    )

    if not DeltaTable.isDeltaTable(spark, MONITORING_PATH):
        (
            metrics_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(MONITORING_PATH)
        )
    else:
        monitoring_table = DeltaTable.forPath(spark, MONITORING_PATH)
        (
            monitoring_table.alias("target")
            .merge(
                metrics_df.alias("source"),
                "target.batch_id = source.batch_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    print()
    print("=== MONITORING METRICS ===")
    print(
        f"Batch:                 {batch_id}"
    )
    print(
        f"Input records:         {input_records}"
    )
    print(
        f"Prediction records:    {prediction_records}"
    )
    print(
        f"Fraud predictions:     {fraud_predictions}"
    )
    print(
        f"Normal predictions:    {normal_predictions}"
    )
    print(
        f"Fraud rate:            {fraud_rate:.6%}"
    )
    print(
        f"Average probability:   "
        f"{float(stats['avg_fraud_probability'] or 0.0):.6f}"
    )
    print(
        f"Maximum probability:   "
        f"{float(stats['max_fraud_probability'] or 0.0):.6f}"
    )


def process_batch(
    spark,
    batch_df,
    batch_id,
    model_broadcast,
    threshold,
):

    print()
    print("=" * 70)
    print(
        f"PROCESSING ML MICRO-BATCH: {batch_id}"
    )
    print("=" * 70)

    if batch_df.isEmpty():
        print("Empty batch. Nothing to process.")
        return

    input_count = batch_df.count()

    print(
        f"Input records: {input_count}"
    )

    # ------------------------------------------------
    # Select ML feature contract
    # ------------------------------------------------

    features_df = batch_df.select(
        "event_id",
        *FEATURE_COLUMNS,
    )

    # ------------------------------------------------
    # Run model inference
    # ------------------------------------------------

    predictions_df = (
        features_df
        .rdd
        .mapPartitions(
            lambda rows:
                predict_partition(
                    rows,
                    model_broadcast,
                    threshold,
                )
        )
        .toDF(
            """
            event_id string,
            fraud_probability double,
            fraud_prediction int,
            model_name string,
            model_version string,
            decision_threshold double,
            prediction_timestamp string
            """
        )
    )

    predictions_df = (
        predictions_df
        .withColumn(
            "prediction_timestamp",
            F.to_timestamp(
                "prediction_timestamp"
            ),
        )
    )

    # ------------------------------------------------
    # Remove duplicates inside the current batch
    # ------------------------------------------------

    predictions_df = (
        predictions_df
        .dropDuplicates(
            ["event_id"]
        )
    )

    # ------------------------------------------------
    # Create prediction Delta table if necessary
    # ------------------------------------------------

    if not DeltaTable.isDeltaTable(
        spark,
        PREDICTIONS_PATH,
    ):

        print(
            "Prediction Delta table does not exist."
        )

        (
            predictions_df
            .write
            .format("delta")
            .mode("overwrite")
            .save(PREDICTIONS_PATH)
        )

    else:

        prediction_table = (
            DeltaTable.forPath(
                spark,
                PREDICTIONS_PATH,
            )
        )

        (
            prediction_table.alias("target")
            .merge(
                predictions_df.alias("source"),
                "target.event_id = source.event_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    # ------------------------------------------------
    # Generate fraud alerts
    # ------------------------------------------------

    alerts_df = (
        predictions_df
        .filter(
            F.col(
                "fraud_prediction"
            ) == 1
        )
        .dropDuplicates(
            ["event_id"]
        )
    )

    alert_count = alerts_df.count()

    # ------------------------------------------------
    # Idempotent alert sink
    # ------------------------------------------------

    if alert_count > 0:

        if not DeltaTable.isDeltaTable(
            spark,
            ALERTS_PATH,
        ):

            print(
                "Alert Delta table does not exist."
            )

            (
                alerts_df
                .write
                .format("delta")
                .mode("overwrite")
                .save(ALERTS_PATH)
            )

        else:

            alert_table = (
                DeltaTable.forPath(
                    spark,
                    ALERTS_PATH,
                )
            )

            (
                alert_table.alias("target")
                .merge(
                    alerts_df.alias("source"),
                    "target.event_id = source.event_id",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

    write_monitoring_metrics(
        spark,
        batch_id,
        threshold,
        predictions_df,
        input_count,
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 70)
    print("REAL-TIME FRAUD ML INFERENCE")
    print("=" * 70)

    print(
        f"Silver source: "
        f"{SILVER_PATH}"
    )

    print(
        f"Prediction output: "
        f"{PREDICTIONS_PATH}"
    )

    print(
        f"Alert output: "
        f"{ALERTS_PATH}"
    )

    model, optimal_threshold = load_model()

    model_broadcast = (
        spark.sparkContext.broadcast(model)
    )

    print()
    print(
        "Starting Silver Delta stream..."
    )

    silver_stream = (
        spark.readStream
        .format("delta")
        .load(SILVER_PATH)
    )

    query = (
        silver_stream
        .writeStream
        .foreachBatch(
            lambda df, batch_id:
                process_batch(
                    spark,
                    df,
                    batch_id,
                    model_broadcast,
                    optimal_threshold,
                )
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_PATH,
        )
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
