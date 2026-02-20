"""
Integration scenarios for ALLOW_EMPTY behavior and scaffold-generated assets.

These tests require Docker and are marked `integration` + `slow`.
"""

from __future__ import annotations

import pathlib
import shutil
import subprocess
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.slow]

PROJECT_ROOT = pathlib.Path(__file__).parent.parent.parent


def _make(
    *targets: str,
    extra_env: dict[str, str] | None = None,
    make_vars: dict[str, str] | None = None,
    check: bool = True,
    timeout: int = 1200,
) -> subprocess.CompletedProcess:
    import os

    env = {**os.environ, **(extra_env or {})}
    make_cmd = ["make"]
    if make_vars:
        make_cmd.extend([f"{key}={value}" for key, value in make_vars.items()])
    make_cmd.extend(list(targets))

    result = subprocess.run(
        make_cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    if check and result.returncode != 0:
        pytest.fail(
            f"make {' '.join(targets)} failed (exit {result.returncode}):\n"
            f"STDOUT:\n{result.stdout[-3000:]}\n"
            f"STDERR:\n{result.stderr[-3000:]}"
        )
    return result


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=10)
        return result.returncode == 0
    except Exception:
        return False


needs_docker = pytest.mark.skipif(not _docker_available(), reason="Docker not available")


@pytest.fixture(scope="module", autouse=True)
def ensure_env():
    env_path = PROJECT_ROOT / ".env"
    local_env = PROJECT_ROOT / "env" / "local.env"
    if local_env.exists():
        shutil.copy(local_env, env_path)
    elif not env_path.exists():
        pytest.skip(".env not found and env/local.env not found")


def _write_taxi_parquet(path: pathlib.Path, rows: int) -> None:
    base_dt = datetime(2024, 1, 15, 8, 0, 0)
    pickups = [base_dt + timedelta(minutes=i * 7) for i in range(rows)]
    dropoffs = [p + timedelta(minutes=12) for p in pickups]

    table = pa.table(
        {
            "VendorID": pa.array([1 for _ in range(rows)], type=pa.int64()),
            "tpep_pickup_datetime": pa.array(pickups, type=pa.timestamp("us")),
            "tpep_dropoff_datetime": pa.array(dropoffs, type=pa.timestamp("us")),
            "passenger_count": pa.array([1 for _ in range(rows)], type=pa.int64()),
            "trip_distance": pa.array([2.5 for _ in range(rows)], type=pa.float64()),
            "RatecodeID": pa.array([1 for _ in range(rows)], type=pa.int64()),
            "store_and_fwd_flag": pa.array(["N" for _ in range(rows)], type=pa.string()),
            "PULocationID": pa.array([10 for _ in range(rows)], type=pa.int64()),
            "DOLocationID": pa.array([20 for _ in range(rows)], type=pa.int64()),
            "payment_type": pa.array([1 for _ in range(rows)], type=pa.int64()),
            "fare_amount": pa.array([12.0 for _ in range(rows)], type=pa.float64()),
            "extra": pa.array([0.0 for _ in range(rows)], type=pa.float64()),
            "mta_tax": pa.array([0.5 for _ in range(rows)], type=pa.float64()),
            "tip_amount": pa.array([2.0 for _ in range(rows)], type=pa.float64()),
            "tolls_amount": pa.array([0.0 for _ in range(rows)], type=pa.float64()),
            "improvement_surcharge": pa.array([0.3 for _ in range(rows)], type=pa.float64()),
            "total_amount": pa.array([14.8 for _ in range(rows)], type=pa.float64()),
            "congestion_surcharge": pa.array([0.0 for _ in range(rows)], type=pa.float64()),
            "Airport_fee": pa.array([0.0 for _ in range(rows)], type=pa.float64()),
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path, compression="snappy")


@needs_docker
def test_scaffolded_non_empty_pipeline_passes(tmp_path):
    data_dir = tmp_path / "fixtures"
    non_empty_file = data_dir / "taxi_nonempty.parquet"
    _write_taxi_parquet(non_empty_file, rows=25)

    env = {
        "HOST_DATA_DIR": str(data_dir),
    }
    make_vars = {
        "HOST_DATA_DIR": str(data_dir),
        "DATA_PATH": "/data/taxi_nonempty.parquet",
        "DATASET_NAME": "taxi",
        "ALLOW_EMPTY": "false",
    }

    _make("down", check=False)
    try:
        _make("scaffold", make_vars={"DATASET": "taxi"})
        _make("up", extra_env=env, make_vars=make_vars)
        _make("create-topics", extra_env=env, make_vars=make_vars)
        _make("generate", extra_env=env, make_vars=make_vars)
        _make("process", extra_env=env, make_vars=make_vars)
        _make("wait-for-silver", extra_env=env, make_vars=make_vars)


        _make("dbt-build", extra_env=env, make_vars=make_vars)
        validate = _make("validate", extra_env=env, make_vars=make_vars, check=False)
        assert validate.returncode == 0, validate.stdout[-2000:]
    finally:
        _make("down", check=False)


@needs_docker
def test_empty_pipeline_fails_then_passes_with_allow_empty(tmp_path):
    data_dir = tmp_path / "fixtures"
    empty_file = data_dir / "taxi_empty.parquet"
    _write_taxi_parquet(empty_file, rows=0)

    base_env = {
        "HOST_DATA_DIR": str(data_dir),
    }
    base_make_vars = {
        "HOST_DATA_DIR": str(data_dir),
        "DATA_PATH": "/data/taxi_empty.parquet",
        "DATASET_NAME": "taxi",
    }

    _make("down", check=False)
    try:
        _make("up", extra_env=base_env, make_vars=base_make_vars)
        _make("create-topics", extra_env=base_env, make_vars=base_make_vars)
        _make("generate", extra_env=base_env, make_vars=base_make_vars)
        _make("process", extra_env=base_env, make_vars=base_make_vars)
        _make("dbt-build", extra_env=base_env, make_vars=base_make_vars)

        fail_run = _make(
            "validate",
            extra_env=base_env,
            make_vars={**base_make_vars, "ALLOW_EMPTY": "false"},
            check=False,
        )
        assert fail_run.returncode != 0, "Expected validate to fail with ALLOW_EMPTY=false"
        assert "empty (0 rows)" in fail_run.stdout or "could not read" in fail_run.stdout

        pass_run = _make(
            "validate",
            extra_env=base_env,
            make_vars={**base_make_vars, "ALLOW_EMPTY": "true"},
            check=False,
        )
        assert pass_run.returncode == 0, pass_run.stdout[-2000:]
    finally:
        _make("down", check=False)
