import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Keep Airflow from creating ~/airflow when the DAG test imports it.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow_home_"))
# Tests get their own Iceberg warehouse, never the real one in data/lake/iceberg.
os.environ["ICEBERG_WAREHOUSE"] = tempfile.mkdtemp(prefix="iceberg_warehouse_")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from ingestion.spark.clean_trips import build_spark

    session = build_spark("nyc-taxi-tests", warehouse=Path(os.environ["ICEBERG_WAREHOUSE"]))
    session.conf.set("spark.sql.shuffle.partitions", "2")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
