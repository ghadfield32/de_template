"""Unit tests for typed dataset manifest validation."""

import pytest

from scripts.dataset_manifest import DatasetManifest


def _valid_manifest(**overrides):
    manifest = {
        "manifest_version": 1,
        "name": "orders",
        "description": "Orders events",
        "topic": "${TOPIC}",
        "dlq_topic": "${DLQ_TOPIC}",
        "bronze_table": "bronze.raw_orders",
        "silver_table": "silver.cleaned_orders",
        "event_ts_col": "event_ts",
        "ts_format": "yyyy-MM-dd''T''HH:mm:ss",
        "partition_date_col": "event_date",
        "partition_date_expr": "CAST(event_ts AS DATE)",
        "columns": [
            {
                "name": "event_ts",
                "kafka_type": "STRING",
                "bronze_type": "TIMESTAMP(3)",
                "silver_name": "event_ts",
                "silver_type": "TIMESTAMP(3)",
                "is_event_ts": True,
            },
            {
                "name": "amount",
                "kafka_type": "DOUBLE",
                "bronze_type": "DOUBLE",
                "silver_name": "amount",
                "silver_type": "DECIMAL(10, 2)",
            },
        ],
        "dedup_key": ["event_ts", "amount"],
        "surrogate_key_fields": ["event_ts", "amount"],
        "quality_filters": ["event_ts IS NOT NULL", "amount >= 0"],
    }
    manifest.update(overrides)
    return manifest


def test_valid_manifest_parses():
    parsed = DatasetManifest.model_validate(_valid_manifest())
    assert parsed.manifest_version == 1
    assert parsed.event_ts_silver_col == "event_ts"
    assert parsed.silver_table_name == "cleaned_orders"


def test_manifest_version_required():
    payload = _valid_manifest()
    payload.pop("manifest_version")
    with pytest.raises(Exception):
        DatasetManifest.model_validate(payload)


def test_manifest_version_must_be_supported():
    with pytest.raises(Exception, match="unsupported manifest_version"):
        DatasetManifest.model_validate(_valid_manifest(manifest_version=2))


def test_requires_single_event_timestamp_column():
    payload = _valid_manifest()
    payload["columns"][1]["is_event_ts"] = True
    with pytest.raises(Exception, match="exactly one column must have is_event_ts"):
        DatasetManifest.model_validate(payload)


def test_quality_filters_cannot_include_semicolon():
    with pytest.raises(Exception, match="must not include ';'"):
        DatasetManifest.model_validate(_valid_manifest(quality_filters=["amount >= 0;"]))


def test_surrogate_key_name_must_be_identifier():
    with pytest.raises(Exception, match="valid SQL identifier"):
        DatasetManifest.model_validate(_valid_manifest(surrogate_key_name="trip-id"))
