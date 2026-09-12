import os

from pyspark.sql import SparkSession


BRONZE_PATH = os.getenv(
    "BRONZE_TRANSACTIONS_PATH",
    "s3a://bronze/transactions",
)


def main():

    spark = (
        SparkSession.builder
        .appName("ReadBronze")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    df = (
        spark.read
        .format("delta")
        .load(BRONZE_PATH)
    )

    print("\n=== BRONZE SCHEMA ===")

    df.printSchema()

    print("\n=== SAMPLE RECORDS ===")

    (
        df
        .orderBy(
            "kafka_partition",
            "kafka_offset",
        )
        .select(
            "event_id",
            "source_row_id",
            "Time",
            "Amount",
            "Class",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
        )
        .show(
            20,
            truncate=False,
        )
    )

    print(
        f"\nTotal Bronze records: "
        f"{df.count()}"
    )

    print(
        "\n=== FRAUD DISTRIBUTION ==="
    )

    (
        df
        .groupBy("Class")
        .count()
        .orderBy("Class")
        .show()
    )

    spark.stop()


if __name__ == "__main__":
    main()