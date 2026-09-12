import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "transactions",
)

BRONZE_PATH = os.getenv(
    "BRONZE_TRANSACTIONS_PATH",
    "s3a://bronze/transactions",
)

CHECKPOINT_LOCATION = os.getenv(
    "BRONZE_CHECKPOINT_LOCATION",
    "s3a://spark-checkpoints/kafka-to-bronze",
)


# ==================================================
# Exact transaction event contract
# ==================================================

TRANSACTION_SCHEMA = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            False,
        ),
        StructField(
            "source_row_id",
            LongType(),
            False,
        ),
        StructField(
            "Time",
            DoubleType(),
            True,
        ),
        *[
            StructField(
                f"V{i}",
                DoubleType(),
                True,
            )
            for i in range(1, 29)
        ],
        StructField(
            "Amount",
            DoubleType(),
            True,
        ),
        StructField(
            "Class",
            LongType(),
            True,
        ),
    ]
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName("KafkaToBronze")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    # ==================================================
    # Read Kafka stream
    # ==================================================

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS,
        )
        .option(
            "subscribe",
            KAFKA_TOPIC,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )

    # ==================================================
    # Parse JSON while preserving Kafka metadata
    # ==================================================

    parsed_df = (
        kafka_df
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("key")
            .cast("string")
            .alias("kafka_key"),
            col("value")
            .cast("string")
            .alias("raw_value"),
        )
        .withColumn(
            "transaction",
            from_json(
                col("raw_value"),
                TRANSACTION_SCHEMA,
            ),
        )
    )

    # ==================================================
    # Bronze record
    # ==================================================

    bronze_df = (
        parsed_df
        .select(
            # Producer metadata
            col("transaction.event_id"),
            col("transaction.source_row_id"),

            # Original dataset features
            col("transaction.Time"),

            *[
                col(
                    f"transaction.V{i}"
                ).alias(
                    f"V{i}"
                )
                for i in range(1, 29)
            ],

            col("transaction.Amount"),
            col("transaction.Class"),

            # Raw event
            col("raw_value"),

            # Kafka metadata
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("kafka_key"),

            # Pipeline metadata
            current_timestamp().alias(
                "ingested_at"
            ),
        )
    )

    # ==================================================
    # Write Bronze Delta
    # ==================================================

    query = (
        bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION,
        )
        .trigger(
            processingTime="10 seconds",
        )
        .start(BRONZE_PATH)
    )

    print("=" * 70)

    print(
        "Kafka → Bronze Delta "
        "streaming job started"
    )

    print(
        f"Kafka topic: {KAFKA_TOPIC}"
    )

    print(
        f"Bronze path: {BRONZE_PATH}"
    )

    print(
        f"Checkpoint: {CHECKPOINT_LOCATION}"
    )

    print("=" * 70)

    query.awaitTermination()


if __name__ == "__main__":
    main()