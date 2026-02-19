"""Generate a minimal 100-row NYC Yellow Taxi Parquet fixture for CI tests.

Output: tests/fixtures/taxi_fixture.parquet

Schema matches yellow_tripdata_2024-01.parquet exactly (same column names and
Arrow types). Seed is fixed so the output is deterministic across CI runs.

Usage:
    uv run python tests/fixtures/make_parquet.py
"""

import pathlib
import random
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq

OUT = pathlib.Path(__file__).parent / "taxi_fixture.parquet"
ROWS = 100
RNG = random.Random(42)  # deterministic seed

BASE_DT = datetime(2024, 1, 15, 8, 0, 0)

pickups = [BASE_DT + timedelta(minutes=RNG.randint(0, 60 * 24 * 30)) for _ in range(ROWS)]
dropoffs = [p + timedelta(minutes=RNG.randint(3, 60)) for p in pickups]

table = pa.table(
    {
        "VendorID": pa.array([RNG.choice([1, 2]) for _ in range(ROWS)], type=pa.int64()),
        "tpep_pickup_datetime": pa.array(pickups, type=pa.timestamp("us")),
        "tpep_dropoff_datetime": pa.array(dropoffs, type=pa.timestamp("us")),
        "passenger_count": pa.array([RNG.randint(1, 4) for _ in range(ROWS)], type=pa.int64()),
        "trip_distance": pa.array(
            [round(RNG.uniform(0.5, 15.0), 2) for _ in range(ROWS)], type=pa.float64()
        ),
        "RatecodeID": pa.array([RNG.choice([1, 1, 1, 2, 3]) for _ in range(ROWS)], type=pa.int64()),
        "store_and_fwd_flag": pa.array(
            [RNG.choice(["N", "N", "N", "Y"]) for _ in range(ROWS)], type=pa.string()
        ),
        "PULocationID": pa.array([RNG.randint(1, 265) for _ in range(ROWS)], type=pa.int64()),
        "DOLocationID": pa.array([RNG.randint(1, 265) for _ in range(ROWS)], type=pa.int64()),
        "payment_type": pa.array([RNG.choice([1, 1, 2]) for _ in range(ROWS)], type=pa.int64()),
        "fare_amount": pa.array(
            [round(RNG.uniform(5.0, 50.0), 2) for _ in range(ROWS)], type=pa.float64()
        ),
        "extra": pa.array([RNG.choice([0.0, 0.5, 1.0]) for _ in range(ROWS)], type=pa.float64()),
        "mta_tax": pa.array([0.5] * ROWS, type=pa.float64()),
        "tip_amount": pa.array(
            [round(RNG.uniform(0.0, 10.0), 2) for _ in range(ROWS)], type=pa.float64()
        ),
        "tolls_amount": pa.array(
            [round(RNG.uniform(0.0, 6.5), 2) for _ in range(ROWS)], type=pa.float64()
        ),
        "improvement_surcharge": pa.array([0.3] * ROWS, type=pa.float64()),
        "total_amount": pa.array(
            [round(RNG.uniform(7.0, 70.0), 2) for _ in range(ROWS)], type=pa.float64()
        ),
        "congestion_surcharge": pa.array(
            [RNG.choice([0.0, 2.5]) for _ in range(ROWS)], type=pa.float64()
        ),
        "Airport_fee": pa.array([RNG.choice([0.0, 1.25]) for _ in range(ROWS)], type=pa.float64()),
    }
)

OUT.parent.mkdir(parents=True, exist_ok=True)
pq.write_table(table, OUT, compression="snappy")
print(f"Written: {OUT}  ({ROWS} rows, {OUT.stat().st_size:,} bytes)")
