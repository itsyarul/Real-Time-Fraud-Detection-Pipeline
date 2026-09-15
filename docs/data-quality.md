# Data Quality & Great Expectations

This document explains the data quality framework implemented in the platform using **Great Expectations 1.19.1** and the Spark quarantine routing mechanism.

---

## Data Quality Philosophy

In financial fraud detection systems, dirty or malformed data has severe consequences:
1. Missing features lead to skewed inference scores or runtime null-pointer exceptions.
2. Inverted or negative amounts distort monetary risk calculations.
3. Corrupted timestamps prevent accurate rolling velocity aggregates.

Rather than relying on ad-hoc assertions, this platform enforces data contracts at two distinct stages:
1. **Streaming Ingestion Filter (Spark)**: Real-time filtering routing bad records to the `quarantine` bucket.
2. **Batch Validation Suite (Great Expectations)**: Formal test suite executed against the Silver table before any analytical reporting or ML retraining.

---

## 1. Streaming Quarantine Filter (`bronze_to_silver.py`)

During micro-batch processing from Bronze to Silver, every record is evaluated against structural validity rules:

| Rule Check | Spark Condition | Failure Reason |
|---|---|---|
| Non-null Event ID | `col("event_id").isNull()` | `NULL_EVENT_ID` |
| Non-null Source Row | `col("source_row_id").isNull()` | `NULL_SOURCE_ROW_ID` |
| Positive Source Row | `col("source_row_id") < 0` | `INVALID_SOURCE_ROW_ID` |
| Non-null Time | `col("Time").isNull()` | `NULL_TIME` |
| Positive Time | `col("Time") < 0` | `NEGATIVE_TIME` |
| Non-null Amount | `col("Amount").isNull()` | `NULL_AMOUNT` |
| Non-negative Amount | `col("Amount") < 0` | `NEGATIVE_AMOUNT` |

Records with any error are diverted to `s3a://quarantine/transactions` with the full error list preserved. Valid records proceed to `s3a://silver/transactions`.

---

## 2. Great Expectations Suite (`silver_transactions_suite`)

The formal Great Expectations suite is defined in:
`apps/great_expectations/gx/expectations/silver_transactions_suite.json`

It validates **37 distinct expectations** categorized across:

### A. Table Completeness & Volume
- `expect_table_row_count_to_be_between`: Minimum 1 row; prevents empty table execution.

### B. Uniqueness & Primary Key Integrity
- `expect_column_values_to_not_be_null` on `event_id`.
- `expect_column_values_to_be_unique` on `event_id`.
- `expect_column_values_to_not_be_null` on `source_row_id`.

### C. Domain & Business Constraints
- `expect_column_values_to_not_be_null` on `Time` and `Amount`.
- `expect_column_values_to_be_between` on `Amount` (min: `0.0`, non-negative transaction values).
- `expect_column_values_to_not_be_null` on `Class`.
- `expect_column_values_to_be_in_set` on `Class` (allowed set: `[0, 1]`).

### D. Feature Completeness (PCA Features V1 to V28)
- 28 expectations ensuring `expect_column_values_to_not_be_null` across `V1`, `V2`, ..., `V28`.

---

## 3. Airflow Integration & Pipeline Guardrail

The first task in the Airflow ML DAG is `validate_silver`:
- Runs `apps/quality/validate_silver.py`.
- If any expectation in the suite fails, the Airflow task raises an exception and halts the pipeline immediately.
- This prevents contaminated data from ever triggering automated model retraining or overwriting the production model.

### Manual Verification
To manually execute the Great Expectations validation suite:
```bash
docker compose run --rm spark-submit \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark/work-dir/quality/validate_silver.py
```
