"""
Download one month of NYC TLC Yellow Taxi data into raw/.

    python ingestion/download_tlc.py --month 2023-02

Skips the download if a valid file is already there, writes to a temporary
file first and renames it at the end (so a half-finished download never looks
like a real file), and checks the Parquet magic bytes before keeping it.
"""

import argparse
import os
import sys
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.common import DEFAULT_RAW_DIR, parse_month  # noqa: E402

TLC_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"


def is_parquet(path: Path) -> bool:
    """A Parquet file starts and ends with the 4 bytes b'PAR1'."""
    if not path.exists() or path.stat().st_size < 12:
        return False
    with path.open("rb") as f:
        head = f.read(4)
        f.seek(-4, os.SEEK_END)
        tail = f.read(4)
    return head == b"PAR1" and tail == b"PAR1"


def download(month: str, raw_dir: Path = DEFAULT_RAW_DIR, force: bool = False) -> Path:
    parse_month(month)
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / f"yellow_tripdata_{month}.parquet"
    if is_parquet(dest) and not force:
        print(f"already present, skipping: {dest}")
        return dest

    tmp = dest.with_suffix(".parquet.part")
    url = TLC_URL.format(month=month)
    print(f"downloading {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-elt-pipeline/2.0"})
    with urllib.request.urlopen(request, timeout=120) as response, tmp.open("wb") as out:
        while chunk := response.read(1 << 20):
            out.write(chunk)
    if not is_parquet(tmp):
        tmp.unlink(missing_ok=True)
        raise RuntimeError(f"downloaded file is not valid Parquet: {url}")
    os.replace(tmp, dest)
    print(f"saved {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--month", required=True, help="e.g. 2023-02")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--force", action="store_true", help="download even if the file exists")
    args = parser.parse_args(argv)
    download(args.month, args.raw_dir, args.force)
    return 0


if __name__ == "__main__":
    sys.exit(main())
