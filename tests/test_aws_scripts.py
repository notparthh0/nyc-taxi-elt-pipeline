"""The S3 and Athena scripts are tested through their planning/SQL functions: no AWS calls."""

import pytest

from ingestion.athena_register import add_partition_sql, create_table_sql
from ingestion.schema import CLEANED_COLUMNS
from ingestion.upload_to_s3 import plan_upload


def test_upload_plan_puts_files_under_the_month_partition(tmp_path):
    partition = tmp_path / "year=2023" / "month=01"
    partition.mkdir(parents=True)
    (partition / "part-0000.parquet").write_bytes(b"x")
    (partition / "_SUCCESS").write_bytes(b"")  # Spark marker file: must not be uploaded
    (tmp_path / "_reports").mkdir()
    (tmp_path / "_reports" / "quality_2023-01.json").write_text("{}")

    prefix, uploads = plan_upload("2023-01", tmp_path, prefix="lake/trips_cleaned")
    keys = [key for _, key in uploads]
    assert prefix == "lake/trips_cleaned/year=2023/month=01/"
    assert keys == ["lake/trips_cleaned/year=2023/month=01/part-0000.parquet",
                    "quality_reports/quality_2023-01.json"]


def test_upload_plan_fails_when_the_month_was_not_cleaned(tmp_path):
    with pytest.raises(FileNotFoundError):
        plan_upload("2023-01", tmp_path)


def test_athena_table_has_every_canonical_column_and_month_partitions():
    sql = create_table_sql("nyc_taxi", "trips_cleaned_v2", "bucket", "lake/trips_cleaned")
    for name, _, athena_type in CLEANED_COLUMNS:
        assert f"`{name.lower()}` {athena_type}" in sql
    assert "PARTITIONED BY (`year` string, `month` string)" in sql
    assert "LOCATION 's3://bucket/lake/trips_cleaned/'" in sql
    assert sql.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS")


def test_athena_partition_statement_is_idempotent_and_points_at_the_month():
    sql = add_partition_sql("nyc_taxi", "trips_cleaned_v2", "bucket", "lake/trips_cleaned", "2023-02")
    assert "ADD IF NOT EXISTS PARTITION (year = '2023', month = '02')" in sql
    assert sql.endswith("LOCATION 's3://bucket/lake/trips_cleaned/year=2023/month=02/'")
