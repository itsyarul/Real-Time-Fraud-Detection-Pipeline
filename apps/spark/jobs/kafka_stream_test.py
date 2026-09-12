import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "transactions",
)

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    "/opt/spark/work-dir/checkpoints/kafka-test",
)


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaStreamTest")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    transactions = (
        kafka_df
        .select(
            col("partition"),
            col("offset"),
            col("timestamp"),
            col("key").cast("string").alias("key"),
            col("value").cast("string").alias("value"),
        )
    )

    query = (
        transactions.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()