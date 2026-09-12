import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MONITORING_PATH = os.getenv(
    "ML_MONITORING_PATH",
    "s3a://monitoring/ml-inference",
)

def main():
    spark = SparkSession.builder.appName("ReadMetrics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("ML INFERENCE MONITORING")
    print("=" * 70)
    
    print()
    print("Monitoring path:")
    print(MONITORING_PATH)
    print()
    
    try:
        df = spark.read.format("delta").load(MONITORING_PATH)
    except Exception as e:
        print(f"Failed to read monitoring table: {e}")
        return

    # Pipeline Summary
    total_batches = df.count()
    summary = df.agg(
        F.sum("input_records").alias("total_input"),
        F.sum("prediction_records").alias("total_predictions"),
        F.sum("fraud_predictions").alias("total_fraud")
    ).collect()[0]

    print("=== PIPELINE SUMMARY ===\n")
    print(f"Total batches:          {total_batches}")
    print(f"Total input records:    {summary['total_input']}")
    print(f"Total predictions:      {summary['total_predictions']}")
    print(f"Total fraud predictions:{summary['total_fraud']}")
    print()

    # Fraud Rate
    fraud_rate = (summary['total_fraud'] / summary['total_predictions']) if summary['total_predictions'] > 0 else 0.0
    
    print("=== FRAUD RATE ===\n")
    print(f"Fraud rate:             {fraud_rate:.6%}")
    print()

    # Model Info
    model_info = df.select("model_name", "model_version", "decision_threshold").distinct().collect()[0]

    print("=== MODEL ===\n")
    print(f"Model:                  {model_info['model_name']}")
    print(f"Version:                {model_info['model_version']}")
    print(f"Threshold:              {model_info['decision_threshold']}")
    print()

    # Recent Batches
    print("=== RECENT BATCHES ===\n")
    print("batch_id | input | predictions | fraud | fraud_rate")
    print("-" * 52)
    
    recent_batches = df.orderBy(F.col("batch_id").desc()).limit(10).collect()
    for row in recent_batches:
        print(f"{row['batch_id']:<8} | {row['input_records']:<5} | {row['prediction_records']:<11} | {row['fraud_predictions']:<5} | {row['fraud_rate']:.4%}")

    spark.stop()

if __name__ == "__main__":
    main()
