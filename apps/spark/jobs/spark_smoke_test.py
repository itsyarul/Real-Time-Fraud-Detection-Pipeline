from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


def main():
    spark = (
        SparkSession.builder
        .appName("SparkSmokeTest")
        .getOrCreate()
    )

    data = [
        (1, "Delhi", 100),
        (2, "Mumbai", 200),
        (3, "Delhi", 150),
        (4, "Bangalore", 300),
        (5, "Mumbai", 250),
    ]

    columns = ["transaction_id", "city", "amount"]

    df = spark.createDataFrame(data, columns)

    print("=== INPUT DATA ===")
    df.show()

    result = (
        df.groupBy("city")
        .agg(spark_sum("amount").alias("total_amount"))
        .orderBy(col("total_amount").desc())
    )

    print("=== AGGREGATED RESULT ===")
    result.show()

    total_transactions = df.count()

    print(f"Total transactions: {total_transactions}")

    spark.stop()


if __name__ == "__main__":
    main()