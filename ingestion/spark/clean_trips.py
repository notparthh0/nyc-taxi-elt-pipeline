"""
PySpark cleaning job for ONE month of NYC TLC Yellow Taxi data.

    python ingestion/spark/clean_trips.py --month 2023-01

What it does
  1. Reads raw/yellow_tripdata_<month>.parquet
  2. Tags every row with the FIRST cleaning rule it fails, in a single pass
  3. Keeps rows that fail no rule, adds trip_duration_minutes and cost_per_mile,
     and casts everything to one canonical schema (ingestion/schema.py)
  4. Overwrites only that month's partition: <out-dir>/year=YYYY/month=MM/
     -> re-running a month replaces it instead of duplicating rows (idempotent)
  5. Writes a data-quality report: <out-dir>/_reports/quality_<month>.json
  6. Exits with code 1 if the share of clean rows falls below --min-clean-ratio,
     so a broken upstream file stops the pipeline instead of loading silently

Rules 1-6 are the original internship rules (they reproduce 2,881,179 clean rows
for January 2023). Rule 7 is new: it drops trips whose pickup is not inside the
file's month - the raw January file contains stray trips from other months.
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from ingestion.common import DEFAULT_LAKE_DIR, DEFAULT_RAW_DIR, parse_month  # noqa: E402
from ingestion.schema import CLEANED_COLUMNS  # noqa: E402

DEFAULT_OUT_DIR = DEFAULT_LAKE_DIR
MAX_TRIP_MINUTES = 180


def duration_minutes_col() -> F.Column:
    """Trip duration in minutes, rounded to 2 decimals (same formula as the original job)."""
    seconds = F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")
    return F.round(seconds / 60, 2)


def cleaning_rules(month: str):
    """Ordered (rule_name, failing_condition) pairs. Nulls are failures, explicitly."""
    parse_month(month)
    pickup, dropoff = F.col("tpep_pickup_datetime"), F.col("tpep_dropoff_datetime")
    return [
        ("fare_amount <= 0 or null", F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0)),
        ("trip_distance <= 0 or null", F.col("trip_distance").isNull() | (F.col("trip_distance") <= 0)),
        ("passenger_count < 1 or null", F.col("passenger_count").isNull() | (F.col("passenger_count") < 1)),
        ("total_amount <= 0 or null", F.col("total_amount").isNull() | (F.col("total_amount") <= 0)),
        ("dropoff not after pickup", pickup.isNull() | dropoff.isNull() | (dropoff <= pickup)),
        (f"trip duration > {MAX_TRIP_MINUTES} min", duration_minutes_col() > MAX_TRIP_MINUTES),
        ("pickup outside the file's month", F.date_format(pickup, "yyyy-MM") != F.lit(month)),
    ]


def tag_drop_reason(df: DataFrame, month: str) -> DataFrame:
    """Add `_drop_reason`: the first rule a row fails, or null if the row is clean."""
    reason = F.lit(None).cast("string")
    for name, failed in reversed(cleaning_rules(month)):
        reason = F.when(failed, F.lit(name)).otherwise(reason)
    return df.withColumn("_drop_reason", reason)


def to_canonical(df: DataFrame) -> DataFrame:
    """Add derived columns and cast to the canonical schema (missing columns become null)."""
    df = df.withColumn("trip_duration_minutes", duration_minutes_col()).withColumn(
        "cost_per_mile", F.round(F.col("total_amount") / F.col("trip_distance"), 2)
    )
    present = {c.lower(): c for c in df.columns}
    selected = []
    for name, spark_type, _ in CLEANED_COLUMNS:
        source = present.get(name.lower())
        column = F.col(source) if source else F.lit(None)
        selected.append(column.cast(spark_type).alias(name))
    return df.select(*selected)


def build_spark(app_name: str = "nyc-taxi-clean") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # TLC files mix column types across months; the vectorised reader can choke on that
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        # UTC has no daylight-saving jumps, so durations are identical on any machine
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run(spark: SparkSession, month: str, raw_dir: Path, out_dir: Path,
        min_clean_ratio: float = 0.85, output_files: int = 1) -> dict:
    """Clean one month. Returns the quality report (also written to disk)."""
    year, mm = parse_month(month)
    raw_path = Path(raw_dir) / f"yellow_tripdata_{month}.parquet"
    if not raw_path.exists():
        raise FileNotFoundError(f"raw file not found: {raw_path} (run ingestion/download_tlc.py first)")

    tagged = tag_drop_reason(spark.read.parquet(str(raw_path)), month)

    # One aggregation pass gives raw rows, clean rows and drops per rule.
    counts = {row["_drop_reason"]: row["n"]
              for row in tagged.groupBy("_drop_reason").agg(F.count(F.lit(1)).alias("n")).collect()}
    raw_rows = sum(counts.values())
    clean_rows = counts.get(None, 0)
    removed_by_rule = {name: counts.get(name, 0) for name, _ in cleaning_rules(month)}

    partition_dir = Path(out_dir) / f"year={year}" / f"month={mm}"
    clean = to_canonical(tagged.filter(F.col("_drop_reason").isNull()))
    clean.repartition(output_files).write.mode("overwrite").parquet(str(partition_dir))

    month_rule = "pickup outside the file's month"
    report = {
        "month": month,
        "raw_file": str(raw_path),
        "output_partition": str(partition_dir),
        "raw_rows": raw_rows,
        "clean_rows": clean_rows,
        "removed_rows": raw_rows - clean_rows,
        "clean_ratio": round(clean_rows / raw_rows, 4) if raw_rows else 0.0,
        "removed_by_rule_in_order": removed_by_rule,
        "clean_rows_with_original_6_rules": clean_rows + removed_by_rule[month_rule],
        "min_clean_ratio": min_clean_ratio,
        "spark_version": spark.version,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    report["passed_quality_gate"] = raw_rows > 0 and report["clean_ratio"] >= min_clean_ratio

    report_dir = Path(out_dir) / "_reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / f"quality_{month}.json").write_text(json.dumps(report, indent=2))
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="month to clean, e.g. 2023-01")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--min-clean-ratio", type=float, default=0.85,
                        help="fail if fewer than this share of rows survive cleaning")
    parser.add_argument("--output-files", type=int, default=1, help="parquet files per partition")
    args = parser.parse_args(argv)

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        report = run(spark, args.month, args.raw_dir, args.out_dir,
                     args.min_clean_ratio, args.output_files)
    finally:
        spark.stop()

    print(f"[{report['month']}] raw={report['raw_rows']:,} clean={report['clean_rows']:,} "
          f"removed={report['removed_rows']:,} ({1 - report['clean_ratio']:.2%})")
    for rule, n in report["removed_by_rule_in_order"].items():
        print(f"    {n:>9,}  {rule}")
    print(f"    written to {report['output_partition']}")
    if not report["passed_quality_gate"]:
        print(f"QUALITY GATE FAILED: clean ratio {report['clean_ratio']} < {report['min_clean_ratio']}",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
