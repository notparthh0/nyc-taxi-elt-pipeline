import json
import uuid

import pytest

from ingestion.common import parse_month
from ingestion.schema import CLEANED_COLUMNS
from tests.synthetic import EXPECTED_DROPS, write_raw_month

pytest.importorskip("pyspark")
from ingestion.spark.clean_trips import run  # noqa: E402

MONTH = "2023-03"


def fresh_table():
    """Each test writes its own Iceberg table so tests can't affect each other."""
    return f"lake.nyc.trips_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cleaned(spark, tmp_path):
    raw_dir, out_dir = tmp_path / "raw", tmp_path / "lake"
    write_raw_month(raw_dir, MONTH, n_clean=200)
    table = fresh_table()
    report = run(spark, MONTH, raw_dir, out_dir, iceberg_table=table)
    return report, raw_dir, out_dir


def test_each_rule_removes_exactly_the_rows_built_to_break_it(cleaned):
    report, _, _ = cleaned
    assert report["removed_by_rule_in_order"] == EXPECTED_DROPS


def test_counts_add_up(cleaned):
    report, _, _ = cleaned
    assert report["clean_rows"] == 200
    assert report["raw_rows"] == 200 + sum(EXPECTED_DROPS.values())
    assert report["removed_rows"] == report["raw_rows"] - report["clean_rows"]
    assert report["clean_rows_with_original_6_rules"] == 201  # the stray-month row passes rules 1-6
    assert report["passed_quality_gate"] is True


def test_output_lands_in_its_month_partition_with_the_canonical_schema(spark, cleaned):
    _, _, out_dir = cleaned
    partition = out_dir / "year=2023" / "month=03"
    df = spark.read.parquet(str(partition))
    assert df.count() == 200
    assert df.columns == [name for name, _, _ in CLEANED_COLUMNS]
    assert dict(df.dtypes)["passenger_count"] == "int"
    assert dict(df.dtypes)["tpep_pickup_datetime"] == "timestamp_ntz"


def test_rerunning_a_month_replaces_it_instead_of_duplicating(spark, cleaned):
    report, raw_dir, out_dir = cleaned
    table = report["iceberg_table"]
    run(spark, MONTH, raw_dir, out_dir, iceberg_table=table)
    run(spark, MONTH, raw_dir, out_dir, iceberg_table=table)
    assert spark.read.parquet(str(out_dir / "year=2023" / "month=03")).count() == report["clean_rows"]
    assert spark.table(table).count() == report["clean_rows"]
    assert spark.sql(f"SELECT COUNT(*) FROM {table}.snapshots").first()[0] == 3  # one per run


def test_iceberg_table_holds_the_clean_rows_partitioned_by_month(spark, cleaned):
    report, _, _ = cleaned
    table = report["iceberg_table"]
    assert report["iceberg_snapshot_id"] is not None
    assert spark.table(table).count() == report["clean_rows"]
    partitions = [r.partition.tpep_pickup_datetime_month for r in spark.table(f"{table}.partitions").collect()]
    assert len(partitions) == 1  # rule 7 keeps every row inside the loaded month


def test_loading_a_second_month_leaves_the_first_untouched(spark, tmp_path):
    raw_dir, table = tmp_path / "raw", fresh_table()
    write_raw_month(raw_dir, "2023-03", n_clean=120, seed=1)
    write_raw_month(raw_dir, "2023-04", n_clean=80, seed=2)
    run(spark, "2023-03", raw_dir, tmp_path / "lake", iceberg_table=table)
    run(spark, "2023-04", raw_dir, tmp_path / "lake", iceberg_table=table)
    run(spark, "2023-04", raw_dir, tmp_path / "lake", iceberg_table=table)  # rerun April
    by_month = dict(spark.sql(f"SELECT date_format(tpep_pickup_datetime, 'yyyy-MM'), COUNT(*) "
                              f"FROM {table} GROUP BY 1").collect())
    assert by_month == {"2023-03": 120, "2023-04": 80}


def test_report_is_written_to_disk(cleaned):
    report, _, out_dir = cleaned
    on_disk = json.loads((out_dir / "_reports" / f"quality_{MONTH}.json").read_text())
    assert on_disk["clean_rows"] == report["clean_rows"]


def test_derived_columns(spark, cleaned):
    _, _, out_dir = cleaned
    pdf = spark.read.parquet(str(out_dir / "year=2023" / "month=03")).toPandas()
    minutes = (pdf["tpep_dropoff_datetime"] - pdf["tpep_pickup_datetime"]).dt.total_seconds() / 60
    assert (pdf["trip_duration_minutes"] - minutes.round(2)).abs().max() < 1e-9
    assert (pdf["cost_per_mile"] - (pdf["total_amount"] / pdf["trip_distance"]).round(2)).abs().max() < 1e-9


def test_quality_gate_trips_when_most_rows_are_bad(spark, tmp_path):
    raw_dir = tmp_path / "raw"
    write_raw_month(raw_dir, MONTH, n_clean=2)  # 2 clean rows vs 10 broken ones
    report = run(spark, MONTH, raw_dir, tmp_path / "lake", min_clean_ratio=0.85, iceberg_table=fresh_table())
    assert report["passed_quality_gate"] is False


def test_missing_column_becomes_null_not_an_error(spark, tmp_path):
    raw_dir = tmp_path / "raw"
    write_raw_month(raw_dir, MONTH, n_clean=20, drop_columns=["airport_fee"])
    run(spark, MONTH, raw_dir, tmp_path / "lake", iceberg_table=fresh_table())
    df = spark.read.parquet(str(tmp_path / "lake" / "year=2023" / "month=03"))
    assert df.filter("airport_fee is not null").count() == 0


def test_missing_raw_file_fails_clearly(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="download_tlc"):
        run(spark, MONTH, tmp_path / "nothing_here", tmp_path / "lake")


@pytest.mark.parametrize("bad", ["2023-13", "2023-1", "23-01", "", None, "2023/01"])
def test_parse_month_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        parse_month(bad)
