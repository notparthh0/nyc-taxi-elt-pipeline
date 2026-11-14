import json

import pytest

from ingestion.common import parse_month
from ingestion.schema import CLEANED_COLUMNS
from tests.synthetic import EXPECTED_DROPS, write_raw_month

pytest.importorskip("pyspark")
from ingestion.spark.clean_trips import run  # noqa: E402

MONTH = "2023-03"


@pytest.fixture
def cleaned(spark, tmp_path):
    raw_dir, out_dir = tmp_path / "raw", tmp_path / "lake"
    write_raw_month(raw_dir, MONTH, n_clean=200)
    report = run(spark, MONTH, raw_dir, out_dir)
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
    run(spark, MONTH, raw_dir, out_dir)
    run(spark, MONTH, raw_dir, out_dir)
    assert spark.read.parquet(str(out_dir / "year=2023" / "month=03")).count() == report["clean_rows"]


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
    report = run(spark, MONTH, raw_dir, tmp_path / "lake", min_clean_ratio=0.85)
    assert report["passed_quality_gate"] is False


def test_missing_column_becomes_null_not_an_error(spark, tmp_path):
    raw_dir = tmp_path / "raw"
    write_raw_month(raw_dir, MONTH, n_clean=20, drop_columns=["airport_fee"])
    run(spark, MONTH, raw_dir, tmp_path / "lake")
    df = spark.read.parquet(str(tmp_path / "lake" / "year=2023" / "month=03"))
    assert df.filter("airport_fee is not null").count() == 0


def test_missing_raw_file_fails_clearly(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="download_tlc"):
        run(spark, MONTH, tmp_path / "nothing_here", tmp_path / "lake")


@pytest.mark.parametrize("bad", ["2023-13", "2023-1", "23-01", "", None, "2023/01"])
def test_parse_month_rejects_bad_values(bad):
    with pytest.raises(ValueError):
        parse_month(bad)
