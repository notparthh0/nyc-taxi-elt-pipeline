"""
Local Apache Iceberg lakehouse shared by the batch job and the streaming job.

Catalog `lake` is a file-based (Hadoop) Iceberg catalog under data/lake/iceberg/,
so it needs no server. Tables:

  lake.nyc.trips_cleaned      cleaned trips, written by ingestion/spark/clean_trips.py,
                              hidden-partitioned by months(tpep_pickup_datetime)
  lake.nyc.zone_windows_live  5-minute zone counts, MERGEd by the streaming job
  lake.nyc.demand_spikes      spikes detected by the streaming job

DuckDB (and so dbt) reads the same tables with iceberg_scan('<warehouse>/nyc/<table>').
Override the location with ICEBERG_WAREHOUSE.
"""

import os
from pathlib import Path

from ingestion.common import PROJECT_ROOT
from ingestion.schema import CLEANED_COLUMNS

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
CATALOG = "lake"
NAMESPACE = f"{CATALOG}.nyc"
TRIPS_TABLE = f"{NAMESPACE}.trips_cleaned"
DEFAULT_WAREHOUSE = PROJECT_ROOT / "data" / "lake" / "iceberg"


def warehouse_path() -> Path:
    return Path(os.environ.get("ICEBERG_WAREHOUSE", DEFAULT_WAREHOUSE)).resolve()


def with_iceberg(builder, warehouse: Path = None, extra_packages=()):
    """Add the Iceberg runtime and the `lake` catalog to a SparkSession builder."""
    packages = ",".join([ICEBERG_PACKAGE, *extra_packages])
    return (builder
            .config("spark.jars.packages", packages)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
            .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
            .config(f"spark.sql.catalog.{CATALOG}.warehouse", (warehouse or warehouse_path()).as_uri()))


def create_trips_table_sql(table: str = TRIPS_TABLE) -> str:
    columns = ",\n  ".join(f"{name} {spark_type}" for name, spark_type, _ in CLEANED_COLUMNS)
    return (f"CREATE TABLE IF NOT EXISTS {table} (\n  {columns}\n) USING iceberg\n"
            f"PARTITIONED BY (months(tpep_pickup_datetime))\n"
            f"TBLPROPERTIES ('write.parquet.compression-codec' = 'snappy')")
