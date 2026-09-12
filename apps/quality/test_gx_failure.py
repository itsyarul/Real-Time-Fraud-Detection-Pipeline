import os
import sys

import great_expectations as gx

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


SILVER_PATH = os.getenv(
    "SILVER_TRANSACTIONS_PATH",
    "s3a://silver/transactions",
)

GX_CONTEXT_ROOT = os.getenv(
    "GX_CONTEXT_ROOT",
    "/opt/spark/work-dir/great_expectations",
)

GX_VALIDATION_DEFINITION_NAME = os.getenv(
    "GX_SILVER_VALIDATION_DEFINITION_NAME",
    "silver_transactions_validation",
)


def main():

    spark = (
        SparkSession.builder
        .appName("TestGXFailurePath")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        print("=" * 70)
        print("GREAT EXPECTATIONS - FAILURE PATH TEST")
        print("=" * 70)

        # ==================================================
        # Read real Silver
        # ==================================================

        silver_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        print(
            f"\nOriginal Silver records: "
            f"{silver_df.count()}"
        )

        # ==================================================
        # Take one valid record
        # ==================================================

        base_record = silver_df.limit(1)

        original_event_id = (
            base_record
            .select("event_id")
            .collect()[0]["event_id"]
        )

        print(
            f"Using base event_id: "
            f"{original_event_id}"
        )

        # ==================================================
        # Create controlled invalid records
        # ==================================================

        # 1. Duplicate event_id
        duplicate_record = base_record

        # 2. Negative amount
        negative_amount_record = (
            base_record
            .withColumn(
                "event_id",
                lit("gx-test-negative-amount"),
            )
            .withColumn(
                "Amount",
                lit(-999.0),
            )
        )

        # 3. Invalid Class
        invalid_class_record = (
            base_record
            .withColumn(
                "event_id",
                lit("gx-test-invalid-class"),
            )
            .withColumn(
                "Class",
                lit(99),
            )
        )

        # 4. Null V1
        null_v1_record = (
            base_record
            .withColumn(
                "event_id",
                lit("gx-test-null-v1"),
            )
            .withColumn(
                "V1",
                lit(None).cast("double"),
            )
        )

        # ==================================================
        # Build corrupted DataFrame
        # ==================================================

        corrupted_df = (
            silver_df
            .unionByName(duplicate_record)
            .unionByName(negative_amount_record)
            .unionByName(invalid_class_record)
            .unionByName(null_v1_record)
        )

        print(
            f"\nCorrupted test records: "
            f"{corrupted_df.count()}"
        )

        print("\n=== CONTROLLED CORRUPTIONS ===")

        (
            corrupted_df
            .filter(
                col("event_id").startswith("gx-test")
                | (col("event_id") == original_event_id)
            )
            .select(
                "event_id",
                "Amount",
                "Class",
                "V1",
            )
            .show(
                truncate=False
            )
        )

        # ==================================================
        # Load GX Context
        # ==================================================

        print("\nLoading GX Data Context...")

        context = gx.get_context(
            mode="file",
            project_root_dir=GX_CONTEXT_ROOT,
        )

        # ==================================================
        # Get existing Validation Definition
        # ==================================================

        validation_definition = (
            context.validation_definitions.get(
                GX_VALIDATION_DEFINITION_NAME
            )
        )

        print(
            f"Using validation definition: "
            f"{GX_VALIDATION_DEFINITION_NAME}"
        )

        # ==================================================
        # Run GX against corrupted DataFrame
        # ==================================================

        print("\n" + "=" * 70)
        print("RUNNING FAILURE TEST")
        print("=" * 70)

        validation_result = (
            validation_definition.run(
                batch_parameters={
                    "dataframe": corrupted_df
                }
            )
        )

        print(
            f"\nOverall validation success: "
            f"{validation_result.success}"
        )

        # ==================================================
        # Inspect failed expectations
        # ==================================================

        results = (
            validation_result
            .results
        )

        failed_expectations = []

        print("\n=== FAILED EXPECTATIONS ===")

        for result in results:

            if not result.success:

                expectation_type = (
                    result.expectation_config.type
                )

                failed_expectations.append(
                    expectation_type
                )

                print(
                    f"[FAIL] {expectation_type}"
                )

        print("\n=== FAILURE TEST SUMMARY ===")

        print(
            f"Total failed expectations: "
            f"{len(failed_expectations)}"
        )

        # ==================================================
        # Assertions
        # ==================================================

        if validation_result.success:

            raise AssertionError(
                "GX FAILURE TEST FAILED: "
                "Corrupted data unexpectedly passed validation."
            )

        if len(failed_expectations) == 0:

            raise AssertionError(
                "GX FAILURE TEST FAILED: "
                "Validation failed but no failed expectations "
                "were detected."
            )

        print(
            "\nGX FAILURE PATH TEST PASSED."
        )

        print(
            "Great Expectations correctly detected "
            "the corrupted Silver data."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()