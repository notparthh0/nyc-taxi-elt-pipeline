"""
Synthetic TLC-shaped raw files for tests: realistic clean trips plus one row
for each cleaning rule, so every rule's count is known in advance.
"""

from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# Rows deliberately broken, in the order the cleaning job checks them.
# One row breaks two rules (fare AND distance) to prove the FIRST rule wins.
EXPECTED_DROPS = {
    "fare_amount <= 0 or null": 3,
    "trip_distance <= 0 or null": 1,
    "passenger_count < 1 or null": 2,
    "total_amount <= 0 or null": 1,
    "dropoff not after pickup": 1,
    "trip duration > 180 min": 1,
    "pickup outside the file's month": 1,
}


def _trip(pickup, minutes, fare=12.5, distance=2.3, passengers=1.0, total=None, pu=132, do=236):
    return {
        "VendorID": 2,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=minutes),
        "passenger_count": passengers,
        "trip_distance": distance,
        "RatecodeID": 1.0,
        "store_and_fwd_flag": "N",
        "PULocationID": pu,
        "DOLocationID": do,
        "payment_type": 1,
        "fare_amount": fare,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": total if total is not None else (fare + 4.5 if fare is not None else 10.0),
        "congestion_surcharge": 2.5,
        "airport_fee": 0.0,
    }


def make_raw_month(month: str, n_clean: int = 200, seed: int = 0) -> pd.DataFrame:
    year, mm = (int(x) for x in month.split("-"))
    start = datetime(year, mm, 1)
    rng = np.random.default_rng(seed)

    rows = []
    for _ in range(n_clean):
        pickup = start + timedelta(days=int(rng.integers(0, 28)), hours=int(rng.integers(0, 24)),
                                   minutes=int(rng.integers(0, 60)))
        rows.append(_trip(pickup, minutes=int(rng.integers(1, 60)),
                          fare=float(rng.uniform(3, 60)), distance=float(rng.uniform(0.5, 20)),
                          passengers=float(rng.integers(1, 5)),
                          pu=int(rng.integers(1, 264)), do=int(rng.integers(1, 264))))

    noon = start + timedelta(days=3, hours=12)
    rows += [
        _trip(noon, 10, fare=0.0),                    # fare <= 0
        _trip(noon, 10, fare=None),                   # fare null
        _trip(noon, 10, fare=-5.0, distance=0.0),     # breaks fare AND distance -> counted as fare
        _trip(noon, 10, distance=0.0),                # distance <= 0
        _trip(noon, 10, passengers=None),             # passenger null
        _trip(noon, 10, passengers=0.0),              # passenger 0
        _trip(noon, 10, total=0.0),                   # total <= 0
        _trip(noon, -5),                              # dropoff before pickup
        _trip(noon, 200),                             # 200-minute trip
        _trip(start - timedelta(days=2), 10),         # pickup in the previous month
    ]
    df = pd.DataFrame(rows)
    for column in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[column] = df[column].astype("datetime64[us]")
    return df


def write_raw_month(raw_dir: Path, month: str, n_clean: int = 200, seed: int = 0, drop_columns=()) -> Path:
    raw_dir = Path(raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"yellow_tripdata_{month}.parquet"
    make_raw_month(month, n_clean, seed).drop(columns=list(drop_columns)).to_parquet(path, index=False)
    return path
