import os

from delta.tables import DeltaTable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    concat_ws,
    current_timestamp,
    expr,
    lit,
    size,
    when,
)

from pyspark import StorageLevel


# ==================================================
# Configuration
# ==================================================

BRONZE_PATH = os.getenv(
    "BRONZE_TRANSACTIONS_PATH",
    "s3a://bronze/transactions",
)

SILVER_PATH = os.getenv(
    "SILVER_TRANSACTIONS_PATH",
    "s3a://silver/transactions",
)

QUARANTINE_PATH = os.getenv(
    "QUARANTINE_TRANSACTIONS_PATH",
    "s3a://quarantine/transactions",
)

CHECKPOINT_LOCATION = os.getenv(
    "SILVER_CHECKPOINT_LOCATION",
    "s3a://spark-checkpoints/bronze-to-silver",
)


# ==================================================
# Spark
# ==================================================

def create_spark_session():

    return (
        SparkSession.builder
        .appName("BronzeToSilver")
        .getOrCreate()
    )


# ==================================================
# Validation
# ==================================================

def add_validation_columns(df):

    raw_validation_errors = array(

            when(
                col("event_id").isNull(),
                lit("NULL_EVENT_ID"),
            ),

            when(
                col("source_row_id").isNull(),
                lit("NULL_SOURCE_ROW_ID"),
            ),

            when(
                col("source_row_id") < 0,
                lit("INVALID_SOURCE_ROW_ID"),
            ),

            when(
                col("Time").isNull(),
                lit("NULL_TIME"),
            ),

            when(
                col("Time") < 0,
                lit("NEGATIVE_TIME"),
            ),

            when(
                col("Amount").isNull(),
                lit("NULL_AMOUNT"),
            ),

            when(
                col("Amount") < 0,
                lit("NEGATIVE_AMOUNT"),
            ),

            when(
                col("Class").isNull(),
                lit("NULL_CLASS"),
            ),

            when(
                ~col("Class").isin(0, 1),
                lit("INVALID_CLASS"),
            ),

            *[
                when(
                    col(f"V{i}").isNull(),
                    lit(f"NULL_V{i}"),
                )
                for i in range(1, 29)
            ],
        )

    return (
        df
        .withColumn(
            "raw_validation_errors",
            raw_validation_errors,
        )
        .withColumn(
            "validation_errors",
            expr("filter(raw_validation_errors, x -> x is not null)"),
        )
        .drop("raw_validation_errors")
        .withColumn(
            "validation_error_count",
            size(
                col("validation_errors")
            ),
        )
        .withColumn(
            "is_valid",
            col("validation_error_count") == 0,
        )
    )

def process_batch(
    batch_df,
    batch_id,
):

    print(
        f"\nProcessing batch: {batch_id}"
    )

    if batch_df.isEmpty():

        print(
            "Batch is empty."
        )

        return

    validated_df = (
        add_validation_columns(
            batch_df
        )
        .persist(
            StorageLevel.MEMORY_AND_DISK
        )
    )

    try:
        total_count = validated_df.count()

        valid_count = (
            validated_df
            .filter(col("is_valid"))
            .count()
        )

        invalid_count = (
            validated_df
            .filter(~col("is_valid"))
            .count()
        )

        print(f"Batch records: {total_count}")
        print(f"Valid records: {valid_count}")
        print(f"Invalid records: {invalid_count}")

        if invalid_count > 0:
            (
                validated_df
                .filter(~col("is_valid"))
                .select(
                    "event_id",
                    "source_row_id",
                    "Amount",
                    "Class",
                    "validation_errors",
                )
                .show(20, truncate=False)
            )

        valid_df = (
            validated_df
            .filter(
                col("is_valid")
            )
        )

        invalid_df = (
            validated_df
            .filter(
                ~col("is_valid")
            )
            .withColumn(
                "quarantined_at",
                current_timestamp(),
            )
            .withColumn(
                "quarantine_reason",
                concat_ws(
                    ",",
                    col("validation_errors"),
                ),
            )
        )

        # ----------------------------------------------
        # Write invalid records to Quarantine
        # ----------------------------------------------

        if not invalid_df.isEmpty():

            invalid_count = (
                invalid_df.count()
            )

            print(
                f"Quarantining "
                f"{invalid_count} records"
            )

            (
                invalid_df
                .write
                .format("delta")
                .mode("append")
                .save(
                    QUARANTINE_PATH
                )
            )

        # ==============================================
        # Prepare valid records for Silver
        # ==============================================

        if valid_count > 0:

            silver_df = (
                valid_df
                .drop(
                    "validation_errors",
                    "validation_error_count",
                    "is_valid",
                )
                .dropDuplicates(
                    ["event_id"]
                )
                .withColumn(
                    "silver_processed_at",
                    current_timestamp(),
                )
            )

            batch_unique_count = silver_df.count()

            print(
                f"Unique valid records in batch: "
                f"{batch_unique_count}"
            )

            # ==========================================
            # Merge into Silver
            # ==========================================

            if DeltaTable.isDeltaTable(
                batch_df.sparkSession,
                SILVER_PATH,
            ):

                silver_table = DeltaTable.forPath(
                    batch_df.sparkSession,
                    SILVER_PATH,
                )

                (
                    silver_table
                    .alias("target")
                    .merge(
                        silver_df.alias("source"),
                        "target.event_id = source.event_id",
                    )
                    .whenNotMatchedInsertAll()
                    .execute()
                )

                print(
                    "Merged unique records into Silver."
                )

            else:

                (
                    silver_df
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .save(
                        SILVER_PATH
                    )
                )

                print(
                    "Created Silver Delta table."
                )

        else:

            print(
                "No valid records in batch."
            )
    finally:
    
        validated_df.unpersist()

def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 70)

    print(
        "Bronze → Silver "
        "streaming job starting"
    )

    print(
        f"Bronze path: {BRONZE_PATH}"
    )

    print(
        f"Silver path: {SILVER_PATH}"
    )

    print(
        f"Quarantine path: "
        f"{QUARANTINE_PATH}"
    )

    print(
        f"Checkpoint: "
        f"{CHECKPOINT_LOCATION}"
    )

    print("=" * 70)

    bronze_stream = (
        spark.readStream
        .format("delta")
        .load(
            BRONZE_PATH
        )
    )

    query = (
        bronze_stream
        .writeStream
        .foreachBatch(
            process_batch
        )
        .option(
            "checkpointLocation",
            CHECKPOINT_LOCATION,
        )
        .trigger(
            processingTime="10 seconds"
        )
        .start()
    )

    query.awaitTermination()



if __name__ == "__main__":
    main()