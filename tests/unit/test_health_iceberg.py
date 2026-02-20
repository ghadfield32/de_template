"""Unit tests for scripts.health.iceberg table-count logic."""

from __future__ import annotations

from config.settings import Settings
from scripts.health import iceberg as health_iceberg


def _minio_cfg(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "STORAGE": "minio",
        "WAREHOUSE": "s3a://warehouse/",
        "S3_ENDPOINT": "http://minio:9000",
        "MINIO_ROOT_USER": "minioadmin",
        "MINIO_ROOT_PASSWORD": "minioadmin",
        "TOPIC": "test.topic",
        "DLQ_TOPIC": "test.topic.dlq",
        "DATA_PATH": "/data/test.parquet",
        "DUCKDB_S3_ENDPOINT": "minio:9000",
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
    }
    base.update(overrides)
    return Settings(**base)


def test_silver_rows_respects_min_threshold(monkeypatch):
    cfg = _minio_cfg(WAIT_FOR_SILVER_MIN_ROWS=5)

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        class Result:
            stdout = "BRONZE=10\nSILVER=3\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(health_iceberg.subprocess, "run", fake_run)
    results = health_iceberg._check_table_counts(cfg, compose_args=[], events_produced=0, allow_empty=False)
    silver = next(r for r in results if r.name == "Silver rows")
    assert silver.passed is False
    assert "WAIT_FOR_SILVER_MIN_ROWS=5" in silver.message


def test_silver_zero_with_allow_empty_passes_even_with_threshold(monkeypatch):
    cfg = _minio_cfg(WAIT_FOR_SILVER_MIN_ROWS=5)

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        class Result:
            stdout = "BRONZE=0\nSILVER=0\n"
            stderr = ""

        return Result()

    monkeypatch.setattr(health_iceberg.subprocess, "run", fake_run)
    results = health_iceberg._check_table_counts(cfg, compose_args=[], events_produced=0, allow_empty=True)
    silver = next(r for r in results if r.name == "Silver rows")
    assert silver.passed is True
    assert "ALLOW_EMPTY=true" in silver.message


def test_error_lines_are_exposed_in_details(monkeypatch):
    cfg = _minio_cfg()

    def fake_run(*args, **kwargs):  # noqa: ANN002, ANN003
        class Result:
            stdout = (
                "BRONZE=-1\n"
                "SILVER=-1\n"
                "ERROR_BRONZE=credential failure\n"
                "ERROR_SILVER=missing metadata files\n"
            )
            stderr = ""

        return Result()

    monkeypatch.setattr(health_iceberg.subprocess, "run", fake_run)
    results = health_iceberg._check_table_counts(cfg, compose_args=[], events_produced=0, allow_empty=False)

    bronze = next(r for r in results if r.name == "Bronze row count")
    silver = next(r for r in results if r.name == "Silver row count")
    assert bronze.detail == "credential failure"
    assert silver.detail == "missing metadata files"
