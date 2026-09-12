from pyspark.sql import SparkSession


OUTPUT_PATH = "s3a://bronze/smoke-test"


def main():

    spark = (
        SparkSession.builder
        .appName("MinIOS3ATest")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    data = [
        (1, "Delhi", 100.0),
        (2, "Mumbai", 200.0),
        (3, "Bangalore", 300.0),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "city", "amount"],
    )

    print("=== Writing Parquet to MinIO ===")

    (
        df.write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print("=== Reading Parquet from MinIO ===")

    result = (
        spark.read
        .parquet(OUTPUT_PATH)
    )

    result.show()

    print(f"Rows: {result.count()}")

    spark.stop()


if __name__ == "__main__":
    main()