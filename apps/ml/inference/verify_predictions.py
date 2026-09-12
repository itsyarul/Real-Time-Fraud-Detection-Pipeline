import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder.appName("VerifyPredictions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "="*70)
    print("TEST 1: INSPECT PREDICTIONS")
    print("="*70)
    
    predictions_path = os.getenv("ML_PREDICTIONS_PATH", "s3a://predictions/fraud")
    print(f"Reading from: {predictions_path}")
    
    df_pred = spark.read.format("delta").load(predictions_path)
    
    total_preds = df_pred.count()
    unique_preds = df_pred.select("event_id").distinct().count()
    duplicate_preds = total_preds - unique_preds
    
    print(f"Total predictions:       {total_preds}")
    print(f"Unique event_ids:        {unique_preds}")
    print(f"Duplicate event_ids:     {duplicate_preds}")
    
    # Verify model contract info
    model_version_row = df_pred.select("model_version").distinct().collect()
    threshold_row = df_pred.select("decision_threshold").distinct().collect()
    
    versions = [r[0] for r in model_version_row]
    thresholds = [r[0] for r in threshold_row]
    
    print(f"model_version = {versions[0] if len(versions) == 1 else versions}")
    print(f"decision_threshold = {thresholds[0] if len(thresholds) == 1 else thresholds}")


    print("\n" + "="*70)
    print("TEST 2: INSPECT ALERTS")
    print("="*70)
    
    alerts_path = os.getenv("ML_ALERTS_PATH", "s3a://fraud-alerts/fraud")
    print(f"Reading from: {alerts_path}")
    
    df_alerts = spark.read.format("delta").load(alerts_path)
    
    total_alerts = df_alerts.count()
    unique_alerts = df_alerts.select("event_id").distinct().count()
    duplicate_alerts = total_alerts - unique_alerts
    
    print(f"Total alerts:             {total_alerts}")
    print(f"Unique event_ids:         {unique_alerts}")
    print(f"Duplicate event_ids:      {duplicate_alerts}")
    
    print("\n")
    spark.stop()

if __name__ == "__main__":
    main()
