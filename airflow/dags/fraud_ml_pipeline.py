from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'fraud_ml_pipeline',
    default_args=default_args,
    description='Offline ML lifecycle for fraud detection',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['fraud', 'ml'],
) as dag:

    # Base docker compose command mapping to the spark-submit profile container
    # We pass ML_MODEL_VERSION via environment variables dynamically
    def get_spark_cmd(script_path):
        return (
            f"cd /project && "
            f"export ML_MODEL_VERSION={{{{ run_id }}}} && "
            f"export PROJECT_ROOT=d:/Real-time-fraud-detection-pipeline && "
            f"docker compose run --rm spark-submit "
            f"/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
            f"{script_path}"
        )

    # 1. Validate Silver
    validate_silver = BashOperator(
        task_id='validate_silver',
        bash_command=get_spark_cmd('/opt/spark/work-dir/quality/validate_silver.py'),
    )

    # 2. Build ML Dataset
    build_ml_dataset = BashOperator(
        task_id='build_ml_dataset',
        bash_command=get_spark_cmd('/opt/spark/work-dir/ml/ingestion/prepare_training_data.py'),
    )

    # 3. Train Model
    train_model = BashOperator(
        task_id='train_model',
        bash_command=get_spark_cmd('/opt/spark/work-dir/ml/training/train_model.py'),
    )

    # 4. Evaluate Model
    evaluate_model = BashOperator(
        task_id='evaluate_model',
        bash_command=get_spark_cmd('/opt/spark/work-dir/ml/training/evaluate_thresholds.py'),
    )

    # 5. Validate Model
    validate_model = BashOperator(
        task_id='validate_model',
        bash_command=get_spark_cmd('/opt/spark/work-dir/ml/training/verify_model.py'),
    )

    # 6. Promote Model
    promote_model = BashOperator(
        task_id='promote_model',
        bash_command=get_spark_cmd('/opt/spark/work-dir/ml/training/promote_model.py'),
    )

    validate_silver >> build_ml_dataset >> train_model >> evaluate_model >> validate_model >> promote_model
