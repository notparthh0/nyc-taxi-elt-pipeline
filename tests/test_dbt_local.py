"""
End to end without AWS: synthetic raw months -> PySpark cleaning into Iceberg -> dbt build
on DuckDB reading that Iceberg table.
Proves the dbt models and all their tests pass, and that re-running a month
incrementally does not duplicate rows.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

from tests.synthetic import write_raw_month

pytest.importorskip("pyspark")
duckdb = pytest.importorskip("duckdb")
from ingestion.spark.clean_trips import run  # noqa: E402

DBT_PROJECT = Path(__file__).resolve().parents[1] / "transform" / "nyc_taxi_transform"
DBT = shutil.which("dbt")
pytestmark = pytest.mark.skipif(DBT is None, reason="dbt is not installed")


def dbt_build(tmp_path, *extra):
    env = {**os.environ,  # ICEBERG_WAREHOUSE is the per-session test warehouse (conftest.py)
           "DUCKDB_PATH": str(tmp_path / "warehouse.duckdb"),
           "DBT_TARGET": "local"}
    command = [DBT, "build", "--profiles-dir", "profiles", "--target", "local",
               "--target-path", str(tmp_path / "dbt_target"), "--log-path", str(tmp_path / "dbt_logs"),
               *extra]
    result = subprocess.run(command, cwd=DBT_PROJECT, env=env, capture_output=True, text=True)
    assert result.returncode == 0, result.stdout[-4000:] + result.stderr[-2000:]
    return result.stdout


def mart_totals(tmp_path):
    with duckdb.connect(str(tmp_path / "warehouse.duckdb"), read_only=True) as con:
        return dict(con.execute(
            "select month_key, sum(trip_count) from nyc_taxi.trips_hourly_demand group by 1 order by 1"
        ).fetchall())


def test_two_month_backfill_then_incremental_rerun(spark, tmp_path):
    raw_dir, lake = tmp_path / "raw", tmp_path / "lake"
    for i, month in enumerate(["2023-03", "2023-04"]):
        write_raw_month(raw_dir, month, n_clean=150 + 50 * i, seed=i)
        run(spark, month, raw_dir, lake)

    output = dbt_build(tmp_path, "--full-refresh")  # non-zero exit if any model or test fails
    assert "ERROR=0" in output
    assert mart_totals(tmp_path) == {"2023-03": 150, "2023-04": 200}

    # Re-run one month incrementally, twice: totals must not change.
    dbt_build(tmp_path, "--vars", '{"month": "2023-04"}')
    dbt_build(tmp_path, "--vars", '{"month": "2023-04"}')
    assert mart_totals(tmp_path) == {"2023-03": 150, "2023-04": 200}
