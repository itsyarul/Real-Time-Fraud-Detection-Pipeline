import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
)


QUARANTINE_PATH = os.getenv(
    "QUARANTINE_TRANSACTIONS_PATH",
    "s3a://quarantine/transactions",
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName("ReadQuarantine")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 70)
    print("READING QUARANTINE TRANSACTIONS")
    print(f"Quarantine path: {QUARANTINE_PATH}")
    print("=" * 70)

    df = (
        spark.read
        .format("delta")
        .load(QUARANTINE_PATH)
    )

    print("\n=== QUARANTINE SCHEMA ===")

    df.printSchema()

    print("\n=== QUARANTINED RECORDS ===")

    (
        df
        .select(
            "event_id",
            "source_row_id",
            "Amount",
            "Class",
            "quarantine_reason",
            "quarantined_at",
        )
        .show(
            100,
            truncate=False,
        )
    )

    print("\n=== QUARANTINE COUNT ===")

    print(
        f"Total quarantined records: "
        f"{df.count()}"
    )

    print("\n=== QUARANTINE REASONS ===")

    (
        df
        .groupBy("quarantine_reason")
        .agg(
            count("*").alias("count")
        )
        .orderBy(
            col("count").desc()
        )
        .show(
            truncate=False
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()