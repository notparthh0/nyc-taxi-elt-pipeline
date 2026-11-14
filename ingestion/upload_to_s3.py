"""
Upload one cleaned month to S3, replacing whatever that month had before.

    python ingestion/upload_to_s3.py --month 2023-01             # needs S3_BUCKET
    python ingestion/upload_to_s3.py --month 2023-01 --dry-run   # print the plan only

Layout in S3 (Hive-style partitions that Athena understands):
    s3://<bucket>/<prefix>/year=2023/month=01/part-....parquet
    s3://<bucket>/quality_reports/quality_2023-01.json

Re-running a month is safe: objects under that month's prefix that are not part
of the new upload are deleted, so stale files from an older run can't be read
twice by Athena.
"""

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import DEFAULT_LAKE_DIR, parse_month  # noqa: E402

DEFAULT_PREFIX = "lake/trips_cleaned"


def plan_upload(month: str, lake_dir: Path, prefix: str = DEFAULT_PREFIX):
    """Return (partition_prefix, [(local_path, s3_key), ...]) for one month."""
    year, mm = parse_month(month)
    partition_dir = Path(lake_dir) / f"year={year}" / f"month={mm}"
    files = sorted(p for p in partition_dir.glob("*.parquet") if p.is_file())
    if not files:
        raise FileNotFoundError(f"no parquet files in {partition_dir} (run the cleaning job first)")
    partition_prefix = f"{prefix.strip('/')}/year={year}/month={mm}/"
    uploads = [(p, partition_prefix + p.name) for p in files]
    report = Path(lake_dir) / "_reports" / f"quality_{month}.json"
    if report.exists():
        uploads.append((report, f"quality_reports/quality_{month}.json"))
    return partition_prefix, uploads


def upload(month: str, bucket: str, lake_dir: Path, prefix: str = DEFAULT_PREFIX,
           region: str = "us-east-1", dry_run: bool = False) -> None:
    partition_prefix, uploads = plan_upload(month, lake_dir, prefix)
    new_keys = {key for _, key in uploads}

    print(f"{'[dry run] ' if dry_run else ''}month {month} -> s3://{bucket}/{partition_prefix}")
    for path, key in uploads:
        print(f"    upload {path.name:<60} -> {key}")
    if dry_run:
        return

    import boto3  # imported here so --dry-run and tests work without AWS libraries

    s3 = boto3.client("s3", region_name=region)
    paginator = s3.get_paginator("list_objects_v2")
    stale = [obj["Key"]
             for page in paginator.paginate(Bucket=bucket, Prefix=partition_prefix)
             for obj in page.get("Contents", [])
             if obj["Key"] not in new_keys]
    for path, key in uploads:
        s3.upload_file(str(path), bucket, key)
    for start in range(0, len(stale), 1000):
        s3.delete_objects(Bucket=bucket,
                          Delete={"Objects": [{"Key": k} for k in stale[start:start + 1000]]})
    print(f"uploaded {len(uploads)} file(s), removed {len(stale)} stale object(s)")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True)
    parser.add_argument("--bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--prefix", default=os.environ.get("S3_LAKE_PREFIX", DEFAULT_PREFIX))
    parser.add_argument("--lake-dir", type=Path, default=DEFAULT_LAKE_DIR)
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    if not args.bucket:
        parser.error("set S3_BUCKET or pass --bucket")
    upload(args.month, args.bucket, args.lake_dir, args.prefix, args.region, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
