from pyspark.sql import SparkSession


DELTA_PATH = "s3a://bronze/delta-smoke-test"


def main():

    spark = (
        SparkSession.builder
        .appName("DeltaMinIOSmokeTest")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------------------
    # Initial dataset
    # -----------------------------------------

    initial_data = [
        (1, "Delhi", 100.0),
        (2, "Mumbai", 200.0),
        (3, "Bangalore", 300.0),
    ]

    columns = [
        "id",
        "city",
        "amount",
    ]

    initial_df = spark.createDataFrame(
        initial_data,
        columns,
    )

    print("\n=== STEP 1: Writing initial Delta table to MinIO ===")

    (
        initial_df.write
        .format("delta")
        .mode("overwrite")
        .save(DELTA_PATH)
    )

    # -----------------------------------------
    # Read version 0
    # -----------------------------------------

    print("\n=== STEP 2: Reading Delta table ===")

    result_df = (
        spark.read
        .format("delta")
        .load(DELTA_PATH)
    )

    result_df.show()

    print(f"Initial row count: {result_df.count()}")

    # -----------------------------------------
    # Append new transaction
    # -----------------------------------------

    new_data = [
        (4, "Chennai", 400.0),
    ]

    new_df = spark.createDataFrame(
        new_data,
        columns,
    )

    print("\n=== STEP 3: Appending new data ===")

    (
        new_df.write
        .format("delta")
        .mode("append")
        .save(DELTA_PATH)
    )

    # -----------------------------------------
    # Read final table
    # -----------------------------------------

    print("\n=== STEP 4: Reading final Delta table ===")

    final_df = (
        spark.read
        .format("delta")
        .load(DELTA_PATH)
        .orderBy("id")
    )

    final_df.show()

    print(f"Final row count: {final_df.count()}")

    print("\n=== STEP 5: Delta table history ===")

    history_df = spark.sql(
        f"DESCRIBE HISTORY delta.`{DELTA_PATH}`"
    )

    history_df.show(
        truncate=False,)

    print("\n=== STEP 6: Reading Delta version 0 ===")

    version_zero_df = (
        spark.read
        .format("delta")
        .option("versionAsOf", 0)
        .load(DELTA_PATH)
    )

    version_zero_df.show()

    print(f"Version 0 row count: {version_zero_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()