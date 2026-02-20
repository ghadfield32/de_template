"""Unit tests for run_metrics.json freshness/metadata guard logic."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from config.settings import Settings
from scripts import validate as validate_mod


def _cfg(**overrides) -> Settings:
    base = {
        "STORAGE": "minio",
        "WAREHOUSE": "s3a://warehouse/",
        "S3_ENDPOINT": "http://minio:9000",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        "TOPIC": "taxi.raw_trips",
        "DLQ_TOPIC": "taxi.raw_trips.dlq",
        "DATA_PATH": "/data/yellow_tripdata_2024-01.parquet",
        "DUCKDB_S3_ENDPOINT": "minio:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
    }
    base.update(overrides)
    return Settings(**base)


def test_local_run_metrics_missing_file_fails_by_default(tmp_path, monkeypatch):
    missing_path = tmp_path / "missing.json"
    monkeypatch.setattr(validate_mod, "RUN_METRICS_PATH", missing_path)
    checks, events = validate_mod._validate_local_run_metrics(_cfg())
    assert len(checks) == 1
    assert checks[0].name == "Run metrics file"
    assert not checks[0].passed
    assert events is None


def test_local_run_metrics_missing_file_allowed_when_disabled(tmp_path, monkeypatch):
    missing_path = tmp_path / "missing.json"
    monkeypatch.setattr(validate_mod, "RUN_METRICS_PATH", missing_path)
    checks, events = validate_mod._validate_local_run_metrics(_cfg(REQUIRE_RUN_METRICS=False))
    assert len(checks) == 1
    assert checks[0].passed
    assert events is None


def test_local_run_metrics_stale_and_mismatch_fails_by_default(tmp_path, monkeypatch):
    stale_metrics_path = tmp_path / "run_metrics.json"
    payload = {
        "events": 10,
        "produced_at": (datetime.now(tz=UTC) - timedelta(hours=10)).isoformat(),
        "topic": "orders.raw_events",
        "dataset": "orders",
        "data_path": "/data/orders.parquet",
    }
    stale_metrics_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(validate_mod, "RUN_METRICS_PATH", stale_metrics_path)

    checks, events = validate_mod._validate_local_run_metrics(_cfg(DATASET_NAME="taxi"))
    assert events is None
    assert any(not check.passed for check in checks)
    assert any(check.name == "Run metrics freshness" and not check.passed for check in checks)


def test_local_run_metrics_stale_allowed_when_override_enabled(tmp_path, monkeypatch):
    stale_metrics_path = tmp_path / "run_metrics.json"
    payload = {
        "events": 42,
        "produced_at": (datetime.now(tz=UTC) - timedelta(hours=10)).isoformat(),
        "topic": "orders.raw_events",
        "dataset": "orders",
        "data_path": "/data/orders.parquet",
    }
    stale_metrics_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(validate_mod, "RUN_METRICS_PATH", stale_metrics_path)

    checks, events = validate_mod._validate_local_run_metrics(
        _cfg(
            DATASET_NAME="taxi",
            TOPIC="taxi.raw_trips",
            DATA_PATH="/data/yellow_tripdata_2024-01.parquet",
            ALLOW_STALE_RUN_METRICS=True,
        )
    )
    assert events == 42
    assert all(check.passed for check in checks)
