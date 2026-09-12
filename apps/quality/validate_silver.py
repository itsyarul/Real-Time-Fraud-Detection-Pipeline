import os

import great_expectations as gx

from pyspark.sql import SparkSession


SILVER_PATH = os.getenv(
    "SILVER_TRANSACTIONS_PATH",
    "s3a://silver/transactions",
)

GX_CONTEXT_ROOT = os.getenv(
    "GX_CONTEXT_ROOT",
    "/opt/spark/work-dir/great_expectations",
)

GX_DATASOURCE_NAME = os.getenv(
    "GX_SILVER_DATASOURCE_NAME",
    "silver_spark_datasource",
)

GX_DATA_ASSET_NAME = os.getenv(
    "GX_SILVER_DATA_ASSET_NAME",
    "silver_transactions",
)

GX_BATCH_DEFINITION_NAME = os.getenv(
    "GX_SILVER_BATCH_DEFINITION_NAME",
    "whole_silver_dataframe",
)

GX_EXPECTATION_SUITE_NAME = os.getenv(
    "GX_SILVER_EXPECTATION_SUITE_NAME",
    "silver_transactions_suite",
)

GX_VALIDATION_DEFINITION_NAME = os.getenv(
    "GX_SILVER_VALIDATION_DEFINITION_NAME",
    "silver_transactions_validation",
)


def get_or_create_datasource(context):
    """
    Get the Spark datasource if it already exists.
    Otherwise create it.
    """

    try:

        datasource = context.data_sources.get(
            GX_DATASOURCE_NAME
        )

        print(
            f"Using existing datasource: "
            f"{GX_DATASOURCE_NAME}"
        )

    except Exception:

        print(
            f"Creating datasource: "
            f"{GX_DATASOURCE_NAME}"
        )

        datasource = context.data_sources.add_spark(
            name=GX_DATASOURCE_NAME
        )

    return datasource


def get_or_create_data_asset(datasource):
    """
    Create or retrieve the DataFrame asset.
    """

    try:

        data_asset = datasource.get_asset(
            GX_DATA_ASSET_NAME
        )

        print(
            f"Using existing data asset: "
            f"{GX_DATA_ASSET_NAME}"
        )

    except Exception:

        print(
            f"Creating data asset: "
            f"{GX_DATA_ASSET_NAME}"
        )

        data_asset = (
            datasource.add_dataframe_asset(
                name=GX_DATA_ASSET_NAME
            )
        )

    return data_asset


def get_or_create_batch_definition(data_asset):
    """
    Create or retrieve the whole-dataframe
    batch definition.
    """

    try:

        batch_definition = (
            data_asset.get_batch_definition(
                GX_BATCH_DEFINITION_NAME
            )
        )

        print(
            f"Using existing batch definition: "
            f"{GX_BATCH_DEFINITION_NAME}"
        )

    except Exception:

        print(
            f"Creating batch definition: "
            f"{GX_BATCH_DEFINITION_NAME}"
        )

        batch_definition = (
            data_asset.add_batch_definition_whole_dataframe(
                name=GX_BATCH_DEFINITION_NAME
            )
        )

    return batch_definition


def get_or_create_expectation_suite(context):

    try:

        expectation_suite = (
            context.suites.get(
                GX_EXPECTATION_SUITE_NAME
            )
        )

        print(
            f"Using existing expectation suite: "
            f"{GX_EXPECTATION_SUITE_NAME}"
        )

    except Exception:

        print(
            f"Creating expectation suite: "
            f"{GX_EXPECTATION_SUITE_NAME}"
        )

        expectation_suite = (
            gx.ExpectationSuite(
                name=GX_EXPECTATION_SUITE_NAME
            )
        )

        context.suites.add(
            expectation_suite
        )

    return expectation_suite


def get_batch(batch_definition, silver_df):
    """
    Create a GX batch from the current Silver DataFrame.
    """

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": silver_df
        }
    )

    print(
        "Created GX batch from Silver DataFrame."
    )

    return batch


def add_expectations(expectation_suite):

    # ==========================================
    # Dataset-level expectations
    # ==========================================

    expectation_suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1
        )
    )

    # ==========================================
    # Identity
    # ==========================================

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="event_id"
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="event_id"
        )
    )

    # ==========================================
    # Required columns
    # ==========================================

    required_columns = [
        "source_row_id",
        "Time",
        "Amount",
        "Class",
    ]

    for column_name in required_columns:

        expectation_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column_name
            )
        )

    # ==========================================
    # Business rules
    # ==========================================

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Amount",
            min_value=0,
        )
    )

    expectation_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Class",
            value_set=[0, 1],
        )
    )

    # ==========================================
    # Feature completeness
    # ==========================================

    for i in range(1, 29):

        expectation_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=f"V{i}"
            )
        )

    print(
        "Expectation suite configured successfully."
    )


def configure_expectations(expectation_suite):

    if len(expectation_suite.expectations) > 0:

        print(
            f"Expectation suite already contains "
            f"{len(expectation_suite.expectations)} expectations."
        )

        return

    add_expectations(expectation_suite)

    print(
        f"Created "
        f"{len(expectation_suite.expectations)} expectations."
    )

def get_or_create_validation_definition(
    context,
    batch_definition,
    expectation_suite,
):
    """
    Create or reuse the GX validation definition.
    """

    try:

        validation_definition = (
            context.validation_definitions.get(
                GX_VALIDATION_DEFINITION_NAME
            )
        )

        print(
            f"Using existing validation definition: "
            f"{GX_VALIDATION_DEFINITION_NAME}"
        )

    except Exception:

        print(
            f"Creating validation definition: "
            f"{GX_VALIDATION_DEFINITION_NAME}"
        )

        validation_definition = (
            gx.ValidationDefinition(
                name=GX_VALIDATION_DEFINITION_NAME,
                data=batch_definition,
                suite=expectation_suite,
            )
        )

        context.validation_definitions.add(
            validation_definition
        )

    return validation_definition

def run_validation(
    validation_definition,
    silver_df,
):

    print("\n" + "=" * 70)
    print("RUNNING GREAT EXPECTATIONS VALIDATION")
    print("=" * 70)

    validation_result = (
        validation_definition.run(
            batch_parameters={
                "dataframe": silver_df
            }
        )
    )

    print("\n" + "=" * 70)
    print("VALIDATION RESULT")
    print("=" * 70)

    print(
        f"Overall success: "
        f"{validation_result.success}"
    )

    return validation_result

def print_validation_summary(
    validation_result,
):

    print("\n=== EXPECTATION SUMMARY ===")

    results = (
        validation_result
        .results
    )

    passed = 0
    failed = 0

    for result in results:

        expectation_type = (
            result.expectation_config.type
        )

        success = result.success

        if success:

            passed += 1
            status = "PASS"

        else:

            failed += 1
            status = "FAIL"

        print(
            f"[{status}] "
            f"{expectation_type}"
        )

    print("\n=== QUALITY SUMMARY ===")

    print(
        f"Passed expectations: {passed}"
    )

    print(
        f"Failed expectations: {failed}"
    )

    print(
        f"Total expectations: "
        f"{passed + failed}"
    )

def main():

    spark = (
        SparkSession.builder
        .appName("ValidateSilver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    try:

        print("=" * 70)
        print(
            "GREAT EXPECTATIONS - "
            "SILVER VALIDATION"
        )
        print("=" * 70)

        # ==========================================
        # Read Silver Delta
        # ==========================================

        print(
            f"\nReading Silver from: "
            f"{SILVER_PATH}"
        )

        silver_df = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        record_count = silver_df.count()

        print(
            f"Silver record count: "
            f"{record_count}"
        )

        # ==========================================
        # Create GX Context
        # ==========================================

        print(
            "\nCreating GX Data Context..."
        )

        context = gx.get_context(
            mode="file",
            project_root_dir=GX_CONTEXT_ROOT,
        )

        print(
            f"GX context type: "
            f"{type(context).__name__}"
        )

        # ==========================================
        # Datasource
        # ==========================================

        datasource = (
            get_or_create_datasource(
                context
            )
        )

        # ==========================================
        # Data Asset
        # ==========================================

        data_asset = (
            get_or_create_data_asset(
                datasource
            )
        )

        # ==========================================
        # Batch Definition
        # ==========================================

        batch_definition = (
            get_or_create_batch_definition(
                data_asset
            )
        )

        # ==========================================
        # Expectation Suite
        # ==========================================

        expectation_suite = (
            get_or_create_expectation_suite(
                context
            )
        )

        # ==========================================
        # Configure Expectations
        # ==========================================

        configure_expectations(
            expectation_suite
        )

        # ==========================================
        # Create GX Batch
        # ==========================================

        batch = get_batch(
            batch_definition,
            silver_df,
        )

        print(
            f"\nGX batch created successfully."
        )

        print(
            f"Expectation count: "
            f"{len(expectation_suite.expectations)}"
        )

        print("\n" + "=" * 70)
        print(
            "GX OBJECTS CREATED SUCCESSFULLY"
        )
        print("=" * 70)

        print(
            f"Datasource: "
            f"{datasource.name}"
        )

        print(
            f"Data Asset: "
            f"{data_asset.name}"
        )

        print(
            f"Batch Definition: "
            f"{batch_definition.name}"
        )

        print(
            f"Expectation Suite: "
            f"{expectation_suite.name}"
        )

        # ==========================================
        # Validation Definition
        # ==========================================

        validation_definition = (
            get_or_create_validation_definition(
                context,
                batch_definition,
                expectation_suite,
            )
        )

        # ==========================================
        # Run Validation
        # ==========================================

        validation_result = run_validation(
            validation_definition,
            silver_df,
        )

        # ==========================================
        # Print Summary
        # ==========================================

        print_validation_summary(
            validation_result
        )

        # ==========================================
        # Fail the job if data quality fails
        # ==========================================

        if not validation_result.success:

            raise RuntimeError(
                "Silver data quality validation failed."
            )

        print(
            "\nSILVER DATA QUALITY VALIDATION PASSED."
        )

    finally:

        spark.stop()


if __name__ == "__main__":
    main()