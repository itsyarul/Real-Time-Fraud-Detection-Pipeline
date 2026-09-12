import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
)


SILVER_TABLE_PATH = os.getenv(
    "SILVER_TRANSACTIONS_PATH",
    "s3a://silver/transactions",
)

TRAINING_DATA_PATH = os.getenv(
    "ML_TRAINING_DATA_PATH",
    "s3a://ml-training/creditcard",
)


EXPECTED_FEATURES = [
    f"V{i}"
    for i in range(1, 29)
] + ["Amount"]

TARGET_COLUMN = "Class"


def create_spark_session():

    return (
        SparkSession.builder
        .appName("PrepareFraudTrainingData")
        .getOrCreate()
    )


def validate_schema(df):

    required_columns = (
        EXPECTED_FEATURES
        + [TARGET_COLUMN]
    )

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            "Training dataset is missing "
            f"columns: {missing_columns}"
        )


def validate_target(df):

    invalid_target_count = (
        df.filter(
            ~col(TARGET_COLUMN).isin(0, 1)
        )
        .count()
    )

    if invalid_target_count > 0:

        raise ValueError(
            f"Found {invalid_target_count} "
            "invalid Class values."
        )

    distribution = (
        df.groupBy(TARGET_COLUMN)
        .agg(
            count("*").alias("count")
        )
        .orderBy(TARGET_COLUMN)
    )

    print("\n=== TARGET DISTRIBUTION ===")

    distribution.show()


def validate_nulls(df):

    required_columns = (
        EXPECTED_FEATURES
        + [TARGET_COLUMN]
    )

    null_expression = None

    for column_name in required_columns:

        condition = col(
            column_name
        ).isNull()

        if null_expression is None:

            null_expression = condition

        else:

            null_expression = (
                null_expression | condition
            )

    null_count = (
        df.filter(null_expression)
        .count()
    )

    if null_count > 0:

        raise ValueError(
            f"Found {null_count} records "
            "containing null ML values."
        )

    print(
        "\nSUCCESS: No null values found."
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        print("=" * 70)
        print(
            "OFFLINE ML TRAINING DATA PREPARATION"
        )
        print("=" * 70)

        print(
            f"\nSource: {SILVER_TABLE_PATH}"
        )

        print(
            f"Destination: {TRAINING_DATA_PATH}"
        )

        # ==========================================
        # Read original dataset
        # ==========================================

        df = (
            spark.read
            .format("delta")
            .load(SILVER_TABLE_PATH)
        )

        print(
            f"\nSource record count: "
            f"{df.count()}"
        )

        # ==========================================
        # Schema validation
        # ==========================================

        validate_schema(df)

        # ==========================================
        # Null validation
        # ==========================================

        validate_nulls(df)

        # ==========================================
        # Target validation
        # ==========================================

        validate_target(df)

        # ==========================================
        # Select ML columns
        # ==========================================

        training_df = (
            df
            .select(
                *EXPECTED_FEATURES,
                TARGET_COLUMN,
            )
        )

        print(
            "\n=== TRAINING COLUMNS ==="
        )

        training_df.printSchema()

        print(
            f"\nTraining record count: "
            f"{training_df.count()}"
        )

        # ==========================================
        # Write training dataset
        # ==========================================

        print(
            "\nWriting training dataset..."
        )

        (
            training_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(
                TRAINING_DATA_PATH
            )
        )

        print(
            "\nTraining dataset successfully "
            "written to MinIO."
        )

        # ==========================================
        # Verify write
        # ==========================================

        verification_df = (
            spark.read
            .format("delta")
            .load(
                TRAINING_DATA_PATH
            )
        )

        verification_count = (
            verification_df.count()
        )

        print(
            f"\nVerification record count: "
            f"{verification_count}"
        )

        if (
            verification_count
            != training_df.count()
        ):

            raise RuntimeError(
                "Training dataset verification "
                "failed: record counts differ."
            )

        print("\n" + "=" * 70)
        print(
            "OFFLINE TRAINING DATA PREPARATION PASSED"
        )
        print("=" * 70)

    finally:

        spark.stop()


if __name__ == "__main__":
    main()