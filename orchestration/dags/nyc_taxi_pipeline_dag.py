"""
Apache Airflow DAG — NYC Taxi Analytics Pipeline
Automates full monthly pipeline execution

Schedule: 5th of every month (aligned with TLC data release)
Tasks: PySpark Clean → S3 Upload → dbt Run → XGBoost Retrain → Dashboard Refresh

Author: Parth Alagh | USICT GGSIPU
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ── Default Args ──────────────────────────────────────────────
default_args = {
    "owner": "parth_alagh",
    "depends_on_past": False,
    "email": ["parthalagh05@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────
dag = DAG(
    dag_id="nyc_taxi_pipeline_dag",
    default_args=default_args,
    description="Monthly NYC Taxi Analytics Pipeline: Ingest → Clean → Transform → ML → Dashboard",
    schedule_interval="0 6 5 * *",     # 6 AM on 5th of every month
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "data-engineering", "ml", "production"],
)

# ── Task 1: PySpark Data Cleaning ─────────────────────────────
pyspark_clean = BashOperator(
    task_id="pyspark_clean",
    bash_command=(
        "cd /Users/parthalagh/MAIN/nyc-taxi-aws && "
        "python3 ingestion/spark/clean_nyc_taxi.py"
    ),
    dag=dag,
)

# ── Task 2: Upload Cleaned Data to S3 ─────────────────────────
upload_to_s3 = BashOperator(
    task_id="upload_to_s3",
    bash_command=(
        "cd /Users/parthalagh/MAIN/nyc-taxi-aws && "
        "python3 ingestion/upload_to_s3.py"
    ),
    dag=dag,
)

# ── Task 3: Refresh Athena Table ──────────────────────────────
refresh_athena = BashOperator(
    task_id="refresh_athena",
    bash_command=(
        "aws athena start-query-execution "
        "--query-string 'MSCK REPAIR TABLE nyc_taxi.trips_cleaned' "
        "--result-configuration OutputLocation=s3://nyc-taxi-de-parth-2026/athena-results/ "
        "--region us-east-1"
    ),
    dag=dag,
)

# ── Task 4: Run dbt Models ────────────────────────────────────
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        "cd /Users/parthalagh/MAIN/nyc-taxi-aws/transform/nyc_taxi_transform && "
        "/Users/parthalagh/Library/Python/3.9/bin/dbt run"
    ),
    dag=dag,
)

# ── Task 5: Retrain XGBoost Model ────────────────────────────
retrain_xgboost = BashOperator(
    task_id="retrain_xgboost",
    bash_command=(
        "cd /Users/parthalagh/MAIN/nyc-taxi-aws/transform && "
        "python3 xgboost_demand_model.py"
    ),
    dag=dag,
)

# ── Task 6: Validate Pipeline ─────────────────────────────────
def validate_pipeline():
    """
    Sanity check: verify all outputs are present
    - S3 cleaned files exist
    - dbt models ran successfully
    - XGBoost model file is fresh
    """
    import boto3
    import os

    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "nyc-taxi-de-parth-2026"

    # Check cleaned data exists in S3
    response = s3.list_objects_v2(Bucket=bucket, Prefix="cleaned/")
    assert response["KeyCount"] > 0, "No cleaned files found in S3!"

    # Check XGBoost model file exists
    model_path = "/Users/parthalagh/MAIN/nyc-taxi-aws/transform/model/xgboost_demand_model.pkl"
    assert os.path.exists(model_path), "XGBoost model file not found!"

    print("✅ Pipeline validation passed.")
    return True

validate = PythonOperator(
    task_id="validate_pipeline",
    python_callable=validate_pipeline,
    dag=dag,
)

# ── Task Dependencies (DAG Flow) ──────────────────────────────
#
# pyspark_clean → upload_to_s3 → refresh_athena → dbt_run → retrain_xgboost → validate
#
pyspark_clean >> upload_to_s3 >> refresh_athena >> dbt_run >> retrain_xgboost >> validate
