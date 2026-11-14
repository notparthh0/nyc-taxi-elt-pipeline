"""
Make a cleaned month queryable in Athena.

    python ingestion/athena_register.py --month 2023-01             # needs S3_BUCKET, S3_STAGING
    python ingestion/athena_register.py --month 2023-01 --dry-run   # print the SQL only

Runs two idempotent statements:
  1. CREATE EXTERNAL TABLE IF NOT EXISTS  (partitioned by year/month, schema from ingestion/schema.py)
  2. ALTER TABLE ... ADD IF NOT EXISTS PARTITION for this month only
     (cheaper than MSCK REPAIR TABLE, which rescans every folder)
"""

import argparse
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.schema import CLEANED_COLUMNS, PARTITION_COLUMNS  # noqa: E402
from ingestion.common import parse_month  # noqa: E402
from ingestion.upload_to_s3 import DEFAULT_PREFIX  # noqa: E402

DEFAULT_DATABASE = "nyc_taxi"
DEFAULT_TABLE = "trips_cleaned_v2"


def create_table_sql(database: str, table: str, bucket: str, prefix: str) -> str:
    columns = ",\n  ".join(f"`{name.lower()}` {athena_type}" for name, _, athena_type in CLEANED_COLUMNS)
    partitions = ", ".join(f"`{name}` {athena_type}" for name, athena_type in PARTITION_COLUMNS)
    return (
        f"CREATE EXTERNAL TABLE IF NOT EXISTS `{database}`.`{table}` (\n  {columns}\n)\n"
        f"PARTITIONED BY ({partitions})\n"
        f"STORED AS PARQUET\n"
        f"LOCATION 's3://{bucket}/{prefix.strip('/')}/'\n"
        f"TBLPROPERTIES ('parquet.compression' = 'SNAPPY')"
    )


def add_partition_sql(database: str, table: str, bucket: str, prefix: str, month: str) -> str:
    year, mm = parse_month(month)
    return (
        f"ALTER TABLE `{database}`.`{table}` ADD IF NOT EXISTS "
        f"PARTITION (year = '{year}', month = '{mm}') "
        f"LOCATION 's3://{bucket}/{prefix.strip('/')}/year={year}/month={mm}/'"
    )


def run_query(client, sql: str, staging: str, poll_seconds: float = 2.0) -> None:
    execution_id = client.start_query_execution(
        QueryString=sql, ResultConfiguration={"OutputLocation": staging}
    )["QueryExecutionId"]
    while True:
        status = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll_seconds)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {status['State']}: {status.get('StateChangeReason')}\n{sql}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True)
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--prefix", default=os.environ.get("S3_LAKE_PREFIX", DEFAULT_PREFIX))
    parser.add_argument("--staging", default=os.environ.get("S3_STAGING"),
                        help="Athena results location, e.g. s3://bucket/athena-results/")
    parser.add_argument("--database", default=os.environ.get("ATHENA_DATABASE", DEFAULT_DATABASE))
    parser.add_argument("--table", default=os.environ.get("ATHENA_TABLE", DEFAULT_TABLE))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not args.bucket:
        parser.error("set S3_BUCKET or pass --bucket")

    statements = [create_table_sql(args.database, args.table, args.bucket, args.prefix),
                  add_partition_sql(args.database, args.table, args.bucket, args.prefix, args.month)]
    if args.dry_run:
        print(";\n\n".join(statements) + ";")
        return 0
    if not args.staging:
        parser.error("set S3_STAGING or pass --staging")

    import boto3

    client = boto3.client("athena", region_name=args.region)
    for sql in statements:
        run_query(client, sql, args.staging)
    print(f"registered {args.database}.{args.table} partition for {args.month}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
