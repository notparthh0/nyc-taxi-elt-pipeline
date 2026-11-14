"""Small helpers shared by the ingestion scripts (no PySpark needed)."""

import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RAW_DIR = PROJECT_ROOT / "raw"
DEFAULT_LAKE_DIR = PROJECT_ROOT / "data" / "lake" / "trips_cleaned"

_MONTH_PATTERN = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


def parse_month(month: str):
    """'2023-01' -> ('2023', '01'). Raises ValueError for anything else."""
    match = _MONTH_PATTERN.match(month or "")
    if not match:
        raise ValueError(f"month must look like YYYY-MM, got {month!r}")
    return match.group(1), match.group(2)
