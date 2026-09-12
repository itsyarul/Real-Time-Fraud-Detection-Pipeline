from pyspark.sql import SparkSession


DELTA_PATH = "/opt/spark/work/delta-smoke-test"


def main():

    spark = (
        SparkSession.builder
        .appName("DeltaSmokeTest")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    data = [
        (1, "Delhi", 100.0),
        (2, "Mumbai", 200.0),
        (3, "Delhi", 150.0),
    ]

    columns = [
        "transaction_id",
        "city",
        "amount",
    ]

    df = spark.createDataFrame(
        data,
        columns,
    )

    print("=== Writing Delta table ===")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(DELTA_PATH)
    )

    print("=== Reading Delta table ===")

    result = (
        spark.read
        .format("delta")
        .load(DELTA_PATH)
    )

    result.show()

    print("=== Delta table count ===")
    print(result.count())

    spark.stop()


if __name__ == "__main__":
    main()