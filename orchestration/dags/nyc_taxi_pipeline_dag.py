"""
NYC Taxi monthly pipeline. One run processes ONE month of TLC Yellow Taxi data.

PIPELINE_TARGET=local (default) runs entirely on a laptop:
    resolve_month -> download_raw -> spark_clean -> dbt_build (DuckDB) -> retrain_model -> validate

PIPELINE_TARGET=aws adds S3 and Athena:
    resolve_month -> download_raw -> spark_clean -> upload_to_s3 -> register_athena_partition
                  -> dbt_build (Athena) -> retrain_model -> validate

Which month? The run's conf {"month": "YYYY-MM"} if given; otherwise two months
before the run's data interval, because TLC publishes each month about two
months later. Every step is idempotent, so re-running a month is safe.

Try it locally (Airflow 3):
    airflow dags test nyc_taxi_pipeline_dag -f orchestration/dags/nyc_taxi_pipeline_dag.py -c '{"month": "2023-01"}'

Worker environment:
    PROJECT_ROOT     repo path (default: two folders above this file)
    PIPELINE_TARGET  local | aws
    PIPELINE_PYTHON  Python with PySpark/XGBoost installed (default: the worker's Python)
    DBT_BIN          dbt executable (default: dbt)
    MODEL_DIR        where retrained models go (default: data/lake/model)
    S3_BUCKET, S3_STAGING, S3_DATA_DIR, AWS_REGION   aws target only
"""

import json
import os
import re
import shlex
import sys
from datetime import timedelta
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.decorators import dag, task
    from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))
TARGET = os.environ.get("PIPELINE_TARGET", "local").strip().lower()
if TARGET not in ("local", "aws"):
    raise ValueError(f"PIPELINE_TARGET must be 'local' or 'aws', got {TARGET!r}")
PYTHON = os.environ.get("PIPELINE_PYTHON", sys.executable)
DBT = os.environ.get("DBT_BIN", "dbt")
MODEL_DIR = os.environ.get("MODEL_DIR", str(PROJECT_ROOT / "data" / "lake" / "model"))
LAKE_DIR = PROJECT_ROOT / "data" / "lake" / "trips_cleaned"

MONTH = "{{ ti.xcom_pull(task_ids='resolve_month') }}"
MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
TLC_PUBLISH_LAG_MONTHS = 2


def _q(value) -> str:
    return shlex.quote(str(value))


def _bash(task_id: str, command: str) -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=command, cwd=str(PROJECT_ROOT))


@task
def resolve_month(**context) -> str:
    """Pick the month to process: conf {"month": ...} first, else data interval minus the TLC lag."""
    dag_run = context.get("dag_run")
    month = ((getattr(dag_run, "conf", None) or {}).get("month") or "").strip()
    if not month:
        ref = context.get("data_interval_start") or context.get("logical_date")
        if ref is None:
            raise ValueError('No month to process: trigger with conf {"month": "YYYY-MM"}')
        year, mon = ref.year, ref.month - TLC_PUBLISH_LAG_MONTHS
        while mon < 1:
            year, mon = year - 1, mon + 12
        month = f"{year}-{mon:02d}"
    if not MONTH_PATTERN.match(month):
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    print(f"processing month {month} (target={TARGET})")
    return month


@task
def validate(month: str) -> dict:
    """Fail the run unless the month's quality gate passed and a fresh model was written."""
    report_path = LAKE_DIR / "_reports" / f"quality_{month}.json"
    if not report_path.exists():
        raise FileNotFoundError(f"missing quality report {report_path}")
    report = json.loads(report_path.read_text())
    if not report.get("passed_quality_gate"):
        raise ValueError(f"quality gate failed for {month}: {report}")

    metrics_path = Path(MODEL_DIR) / "evaluation_metrics.json"
    model_path = Path(MODEL_DIR) / "xgboost_demand_model.pkl"
    for path in (metrics_path, model_path):
        if not path.exists():
            raise FileNotFoundError(f"missing model output {path}")
    metrics = json.loads(metrics_path.read_text())

    if TARGET == "aws":
        import boto3

        year, mm = month.split("-")
        prefix = f"{os.environ.get('S3_LAKE_PREFIX', 'lake/trips_cleaned').strip('/')}/year={year}/month={mm}/"
        listing = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1")).list_objects_v2(
            Bucket=os.environ["S3_BUCKET"], Prefix=prefix)
        if listing.get("KeyCount", 0) == 0:
            raise FileNotFoundError(f"no objects under s3://{os.environ['S3_BUCKET']}/{prefix}")

    summary = {"month": month, "clean_rows": report["clean_rows"], "clean_ratio": report["clean_ratio"],
               "model_rmse": metrics.get("rmse"), "model_r2": metrics.get("r2"), "target": TARGET}
    print(json.dumps(summary, indent=2))
    return summary


@dag(
    dag_id="nyc_taxi_pipeline_dag",
    description="Monthly NYC Taxi pipeline: download -> PySpark clean -> dbt -> XGBoost",
    schedule="0 6 5 * *",  # 06:00 UTC on the 5th of every month
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "parth_alagh", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["nyc-taxi", "data-engineering", TARGET],
    doc_md=__doc__,
)
def nyc_taxi_pipeline():
    month = resolve_month()

    download_raw = _bash("download_raw", f"{_q(PYTHON)} ingestion/download_tlc.py --month {MONTH}")
    spark_clean = _bash("spark_clean", f"{_q(PYTHON)} ingestion/spark/clean_trips.py --month {MONTH}")

    dbt_target = "athena" if TARGET == "aws" else "local"
    dbt_vars = "'{\"month\": \"" + MONTH + "\"}'"
    dbt_build = _bash(
        "dbt_build",
        f"cd transform/nyc_taxi_transform && {_q(DBT)} build --profiles-dir profiles "
        f"--target {dbt_target} --vars {dbt_vars}",
    )

    data_source = "athena" if TARGET == "aws" else "duckdb"
    retrain_model = _bash(
        "retrain_model",
        f"DATA_SOURCE={data_source} MODEL_DIR={_q(MODEL_DIR)} {_q(PYTHON)} transform/xgboost_demand_model.py",
    )

    month >> download_raw >> spark_clean
    if TARGET == "aws":
        upload_to_s3 = _bash("upload_to_s3", f"{_q(PYTHON)} ingestion/upload_to_s3.py --month {MONTH}")
        register = _bash("register_athena_partition",
                         f"{_q(PYTHON)} ingestion/athena_register.py --month {MONTH}")
        spark_clean >> upload_to_s3 >> register >> dbt_build
    else:
        spark_clean >> dbt_build
    dbt_build >> retrain_model >> validate(month)


nyc_taxi_pipeline()
