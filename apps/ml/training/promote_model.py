import json
import os
import sys

from pyspark.sql import SparkSession

MODEL_NAME = os.getenv(
    "ML_MODEL_NAME",
    "fraud-logistic-regression",
)

MODEL_VERSION = os.getenv(
    "ML_MODEL_VERSION",
    "1.0.1",
)

MODEL_PATH = os.getenv(
    "ML_MODEL_PATH",
    "s3a://models/fraud-detection",
)

CANDIDATE_URI = f"{MODEL_PATH}/{MODEL_NAME}/{MODEL_VERSION}"
PRODUCTION_URI = f"{MODEL_PATH}/{MODEL_NAME}/production"

LOCAL_CANDIDATE_META = "/tmp/candidate_metadata.json"
LOCAL_PRODUCTION_META = "/tmp/production_metadata.json"


def promote_model(spark):
    sc = spark.sparkContext
    jvm = sc._jvm
    conf = sc._jsc.hadoopConfiguration()
    Path = jvm.org.apache.hadoop.fs.Path

    s3_candidate_meta = Path(f"{CANDIDATE_URI}/metadata.json")
    s3_candidate_model = Path(f"{CANDIDATE_URI}/model.joblib")
    
    s3_prod_meta = Path(f"{PRODUCTION_URI}/metadata.json")
    s3_prod_model = Path(f"{PRODUCTION_URI}/model.joblib")
    
    fs = s3_candidate_meta.getFileSystem(conf)

    print("\n" + "=" * 70)
    print("EVALUATING MODEL FOR PROMOTION")
    print("=" * 70)

    # 1. Load Candidate Metadata
    print(f"Fetching candidate metadata from: {CANDIDATE_URI}")
    if not fs.exists(s3_candidate_meta):
        raise FileNotFoundError(f"Candidate metadata not found at {s3_candidate_meta}")
        
    fs.copyToLocalFile(False, s3_candidate_meta, Path(f"file://{LOCAL_CANDIDATE_META}"), True)
    
    with open(LOCAL_CANDIDATE_META, "r") as f:
        candidate_metadata = json.load(f)
        
    candidate_f1 = candidate_metadata.get("metrics", {}).get("f1", 0.0)
    print(f"Candidate Model ({MODEL_VERSION}) F1 Score: {candidate_f1:.4f}")

    # 2. Load Production Metadata (if exists)
    production_f1 = 0.0
    if fs.exists(s3_prod_meta):
        print(f"Fetching production metadata from: {PRODUCTION_URI}")
        fs.copyToLocalFile(False, s3_prod_meta, Path(f"file://{LOCAL_PRODUCTION_META}"), True)
        with open(LOCAL_PRODUCTION_META, "r") as f:
            production_metadata = json.load(f)
        production_f1 = production_metadata.get("metrics", {}).get("f1", 0.0)
        print(f"Current Production Model F1 Score: {production_f1:.4f}")
    else:
        print("No existing production model found.")

    # 3. Compare and Promote
    if candidate_f1 > production_f1:
        print(f"\n✅ Candidate model is BETTER ({candidate_f1:.4f} > {production_f1:.4f}). Promoting!")
        
        # We use FileUtil.copy to copy within HDFS/S3
        FileUtil = jvm.org.apache.hadoop.fs.FileUtil
        
        # Copy metadata.json
        FileUtil.copy(fs, s3_candidate_meta, fs, s3_prod_meta, False, True, conf)
        # Copy model.joblib
        FileUtil.copy(fs, s3_candidate_model, fs, s3_prod_model, False, True, conf)
        
        print(f"Model successfully promoted to: {PRODUCTION_URI}")
    else:
        print(f"\n❌ Candidate model is NOT better ({candidate_f1:.4f} <= {production_f1:.4f}). Skipping promotion.")


def main():
    spark = SparkSession.builder.appName("PromoteModel").getOrCreate()
    try:
        promote_model(spark)
    except Exception as exc:
        print(f"\nMODEL PROMOTION FAILED: {exc}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
