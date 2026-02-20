"""Unit tests for scaffold helper logic."""

from scripts.dataset_manifest import DatasetManifest
from scripts.scaffold_dataset import extract_range_tests


def _manifest_with_filters(filters: list[str]) -> DatasetManifest:
    return DatasetManifest.model_validate(
        {
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
                {
                    "name": "quantity",
                    "kafka_type": "BIGINT",
                    "bronze_type": "BIGINT",
                    "silver_name": "quantity",
                    "silver_type": "INT",
                },
            ],
            "dedup_key": ["event_ts", "amount"],
            "surrogate_key_fields": ["event_ts", "amount"],
            "quality_filters": filters,
        }
    )


def test_extract_range_tests_uses_numeric_filters():
    manifest = _manifest_with_filters(["amount > 0", "quantity >= 0"])
    tests = extract_range_tests(manifest)
    assert tests == [
        {"column": "amount", "min_value": 0, "inclusive": False},
        {"column": "quantity", "min_value": 0, "inclusive": True},
    ]


def test_extract_range_tests_tracks_upper_and_lower_bounds():
    manifest = _manifest_with_filters(["amount >= 1", "amount <= 100"])
    tests = extract_range_tests(manifest)
    assert tests == [
        {
            "column": "amount",
            "min_value": 1,
            "max_value": 100,
            "inclusive": True,
        }
    ]
