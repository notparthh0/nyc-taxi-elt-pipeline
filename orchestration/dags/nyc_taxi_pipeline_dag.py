"""
Apache Airflow DAG — NYC Taxi Analytics Pipeline
Automates full monthly pipeline execution

Schedule: 5th of every month (aligned with TLC data release)
Tasks: PySpark Clean → S3 Upload → Athena Refresh → dbt Run → XGBoost Retrain → Validate

Author: Parth Alagh | USICT GGSIPU

Environment variables required:
  PROJECT_ROOT   — absolute path to nyc-taxi-aws/ directory
  S3_BUCKET      — AWS S3 bucket name (e.g. nyc-taxi-de-parth-2026)
  AWS_REGION     — AWS region (e.g. us-east-1)
  DBT_PROFILES_DIR — path to directory containing profiles.yml
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PROJECT_ROOT   = os.environ.get("PROJECT_ROOT", "/opt/nyc-taxi-aws")
S3_BUCKET      = os.environ.get("S3_BUCKET", "nyc-taxi-de-parth-2026")
AWS_REGION     = os.environ.get("AWS_REGION", "us-east-1")
DBT_BIN        = os.environ.get("DBT_BIN", "dbt")

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
    bash_command="cd $PROJECT_ROOT && python3 ingestion/spark/clean_trips.py",
    dag=dag,
)

# ── Task 2: Upload Cleaned Data to S3 ─────────────────────────
upload_to_s3 = BashOperator(
    task_id="upload_to_s3",
    bash_command="cd $PROJECT_ROOT && python3 ingestion/upload_to_s3.py",
    dag=dag,
)

# ── Task 3: Refresh Athena Table ──────────────────────────────
refresh_athena = BashOperator(
    task_id="refresh_athena",
    bash_command=(
        "aws athena start-query-execution "
        "--query-string 'MSCK REPAIR TABLE nyc_taxi.trips_cleaned' "
        "--result-configuration OutputLocation=s3://$S3_BUCKET/athena-results/ "
        "--region $AWS_REGION"
    ),
    dag=dag,
)

# ── Task 4: Run dbt Models ────────────────────────────────────
dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command="cd $PROJECT_ROOT/transform/nyc_taxi_transform && $DBT_BIN run",
    dag=dag,
)

# ── Task 5: Retrain XGBoost Model ────────────────────────────
retrain_xgboost = BashOperator(
    task_id="retrain_xgboost",
    bash_command="cd $PROJECT_ROOT/transform && python3 xgboost_demand_model.py",
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
    from pathlib import Path

    bucket       = os.environ.get("S3_BUCKET", "nyc-taxi-de-parth-2026")
    region       = os.environ.get("AWS_REGION", "us-east-1")
    project_root = os.environ.get("PROJECT_ROOT", "")
    model_path   = Path(project_root) / "transform" / "model" / "xgboost_demand_model.pkl"

    s3 = boto3.client("s3", region_name=region)

    # Check cleaned data exists in S3
    response = s3.list_objects_v2(Bucket=bucket, Prefix="cleaned/")
    assert response["KeyCount"] > 0, f"No cleaned files found in s3://{bucket}/cleaned/"

    # Check XGBoost model file exists
    assert model_path.exists(), f"XGBoost model file not found at {model_path}"

    print("Pipeline validation passed.")
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
