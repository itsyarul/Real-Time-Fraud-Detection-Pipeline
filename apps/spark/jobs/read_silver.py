import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
)


SILVER_PATH = os.getenv(
    "SILVER_TRANSACTIONS_PATH",
    "s3a://silver/transactions",
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName("ReadSilver")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 70)
    print("READING SILVER TRANSACTIONS")
    print(f"Silver path: {SILVER_PATH}")
    print("=" * 70)

    df = (
        spark.read
        .format("delta")
        .load(SILVER_PATH)
    )

    print("\n=== SILVER SCHEMA ===")

    df.printSchema()

    print("\n=== SAMPLE RECORDS ===")

    (
        df
        .orderBy("source_row_id")
        .show(
            20,
            truncate=False,
        )
    )

    print("\n=== RECORD COUNTS ===")

    total_records = df.count()

    unique_events = (
        df
        .select(
            countDistinct("event_id")
            .alias("unique_event_ids")
        )
        .first()["unique_event_ids"]
    )

    duplicate_records = (
        total_records
        - unique_events
    )

    print(
        f"Total Silver records: "
        f"{total_records}"
    )

    print(
        f"Unique event_ids: "
        f"{unique_events}"
    )

    print(
        f"Duplicate event_ids: "
        f"{duplicate_records}"
    )

    print("\n=== FRAUD DISTRIBUTION ===")

    (
        df
        .groupBy("Class")
        .agg(
            count("*").alias("count")
        )
        .orderBy("Class")
        .show()
    )

    print("\n=== AMOUNT STATISTICS ===")

    (
        df
        .select("Amount")
        .summary(
            "count",
            "min",
            "25%",
            "50%",
            "75%",
            "max",
            "mean",
        )
        .show(
            truncate=False
        )
    )

    print("\n=== EVENT ID CHECK ===")

    duplicate_event_ids = (
        df
        .groupBy("event_id")
        .count()
        .filter(
            col("count") > 1
        )
    )

    duplicate_count = (
        duplicate_event_ids
        .count()
    )

    print(
        f"Duplicate event_id groups: "
        f"{duplicate_count}"
    )

    if duplicate_count > 0:

        print(
            "\nWARNING: "
            "Duplicate event_ids found."
        )

        duplicate_event_ids.show(
            20,
            truncate=False,
        )

    else:

        print(
            "\nSUCCESS: "
            "No duplicate event_ids found."
        )

    print("\n=== SILVER VALIDATION SUMMARY ===")

    (
        df
        .select(
            count("*").alias("total_records"),
            countDistinct("event_id").alias(
                "unique_event_ids"
            ),
        )
        .show(
            truncate=False
        )
    )

    spark.stop()


if __name__ == "__main__":
    main()