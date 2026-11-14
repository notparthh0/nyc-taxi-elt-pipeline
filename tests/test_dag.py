"""The Airflow DAG must parse cleanly and wire its tasks in the right order for both targets."""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models.dagbag import DagBag  # noqa: E402

DAG_FILE = Path(__file__).resolve().parents[1] / "orchestration" / "dags" / "nyc_taxi_pipeline_dag.py"


def load_dag(monkeypatch, target):
    monkeypatch.setenv("PIPELINE_TARGET", target)
    bag = DagBag(dag_folder=str(DAG_FILE), include_examples=False)
    assert bag.import_errors == {}
    return bag.get_dag("nyc_taxi_pipeline_dag")


def chain(dag):
    """Walk the single linear path from resolve_month to validate."""
    order, task = [], dag.get_task("resolve_month")
    while task is not None:
        order.append(task.task_id)
        downstream = sorted(task.downstream_task_ids - set(order))
        task = dag.get_task(downstream[0]) if downstream else None
    return order


def test_local_dag(monkeypatch):
    dag = load_dag(monkeypatch, "local")
    assert chain(dag) == ["resolve_month", "download_raw", "spark_clean", "dbt_build",
                          "retrain_model", "validate"]


def test_aws_dag_adds_s3_and_athena_steps(monkeypatch):
    dag = load_dag(monkeypatch, "aws")
    assert chain(dag) == ["resolve_month", "download_raw", "spark_clean", "upload_to_s3",
                          "register_athena_partition", "dbt_build", "retrain_model", "validate"]


def test_dag_runs_monthly_without_catchup(monkeypatch):
    dag = load_dag(monkeypatch, "local")
    assert dag.catchup is False
    assert dag.max_active_runs == 1
