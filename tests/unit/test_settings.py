"""
tests/unit/test_settings.py — Unit tests for config/settings.py.

These tests validate the Pydantic Settings schema with no Docker or network
dependencies. They run in under 1 second.

Run: make test-unit
     pytest tests/unit/test_settings.py -v
"""

import pytest

# conftest.py adds project root to sys.path
from config.settings import Settings

# ---------------------------------------------------------------------------
# Helpers: minimal valid kwargs for each STORAGE type
# ---------------------------------------------------------------------------


def _minio_kwargs(**overrides):
    base = dict(
        STORAGE="minio",
        S3_ENDPOINT="http://minio:9000",
        MINIO_ROOT_USER="minioadmin",
        MINIO_ROOT_PASSWORD="minioadmin",
        TOPIC="test.topic",
        DLQ_TOPIC="test.topic.dlq",
        DATA_PATH="/data/test.parquet",
        DUCKDB_S3_ENDPOINT="minio:9000",
        AWS_ACCESS_KEY_ID="minioadmin",
        AWS_SECRET_ACCESS_KEY="minioadmin",
    )
    base.update(overrides)
    return base


def _aws_kwargs(**overrides):
    base = dict(
        STORAGE="aws_s3",
        TOPIC="test.topic",
        DLQ_TOPIC="test.topic.dlq",
        DATA_PATH="/data/test.parquet",
        DUCKDB_S3_ENDPOINT="s3.amazonaws.com",
    )
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Axis validation
# ---------------------------------------------------------------------------


class TestAxisValidation:
    def test_valid_default_minio(self):
        s = Settings(**_minio_kwargs())
        assert s.BROKER == "redpanda"
        assert s.CATALOG == "hadoop"
        assert s.STORAGE == "minio"
        assert s.MODE == "batch"

    def test_valid_kafka_broker(self):
        s = Settings(**_minio_kwargs(BROKER="kafka"))
        assert s.BROKER == "kafka"

    def test_valid_streaming_mode(self):
        s = Settings(**_minio_kwargs(MODE="streaming_bronze"))
        assert s.MODE == "streaming_bronze"

    def test_valid_rest_catalog(self):
        s = Settings(**_minio_kwargs(CATALOG="rest"))
        assert s.CATALOG == "rest"

    def test_invalid_broker_raises(self):
        with pytest.raises(Exception):  # pydantic ValidationError
            Settings(**_minio_kwargs(BROKER="rabbitmq"))

    def test_invalid_storage_raises(self):
        with pytest.raises(Exception):
            Settings(**_minio_kwargs(STORAGE="hdfs"))

    def test_invalid_mode_raises(self):
        with pytest.raises(Exception):
            Settings(**_minio_kwargs(MODE="realtime"))

    def test_invalid_catalog_raises(self):
        with pytest.raises(Exception):
            Settings(**_minio_kwargs(CATALOG="hive"))

    def test_axis_values_stripped_of_whitespace(self):
        """GNU make leaves trailing whitespace after `include .env`."""
        s = Settings(**_minio_kwargs(BROKER="redpanda  ", STORAGE="minio  "))
        assert s.BROKER == "redpanda"
        assert s.STORAGE == "minio"


# ---------------------------------------------------------------------------
# Storage combination validation
# ---------------------------------------------------------------------------


class TestStorageCombos:
    def test_minio_requires_s3_endpoint(self):
        kwargs = _minio_kwargs()
        del kwargs["S3_ENDPOINT"]
        with pytest.raises(ValueError, match="S3_ENDPOINT"):
            Settings(**kwargs)

    def test_minio_requires_minio_root_user(self):
        kwargs = _minio_kwargs()
        del kwargs["MINIO_ROOT_USER"]
        with pytest.raises(ValueError, match="MINIO_ROOT_USER"):
            Settings(**kwargs)

    def test_minio_requires_minio_root_password(self):
        kwargs = _minio_kwargs()
        del kwargs["MINIO_ROOT_PASSWORD"]
        with pytest.raises(ValueError, match="MINIO_ROOT_PASSWORD"):
            Settings(**kwargs)

    def test_aws_with_minio_endpoint_raises(self):
        with pytest.raises(ValueError, match="minio"):
            Settings(**_aws_kwargs(S3_ENDPOINT="http://minio:9000"))

    def test_aws_without_endpoint_is_valid(self):
        s = Settings(**_aws_kwargs())
        assert s.STORAGE == "aws_s3"
        assert s.S3_ENDPOINT is None

    def test_gcs_storage_valid(self):
        s = Settings(**_aws_kwargs(STORAGE="gcs", DUCKDB_S3_ENDPOINT="storage.googleapis.com"))
        assert s.STORAGE == "gcs"

    def test_azure_storage_valid(self):
        s = Settings(
            **_aws_kwargs(
                STORAGE="azure",
                DUCKDB_S3_ENDPOINT="myaccount.blob.core.windows.net",
            )
        )
        assert s.STORAGE == "azure"


# ---------------------------------------------------------------------------
# Convenience properties
# ---------------------------------------------------------------------------


class TestConvenienceProperties:
    def test_warehouse_s3_path_converts_s3a_to_s3(self):
        s = Settings(**_minio_kwargs(WAREHOUSE="s3a://warehouse/"))
        assert s.warehouse_s3_path == "s3://warehouse/"

    def test_warehouse_s3_path_passthrough_for_s3(self):
        s = Settings(**_aws_kwargs(WAREHOUSE="s3://mybucket/warehouse/"))
        assert s.warehouse_s3_path == "s3://mybucket/warehouse/"

    def test_effective_s3_key_uses_aws_key_when_set(self):
        s = Settings(**_minio_kwargs(AWS_ACCESS_KEY_ID="mykey"))
        assert s.effective_s3_key == "mykey"

    def test_effective_duckdb_endpoint_defaults_to_minio(self):
        s = Settings(**_minio_kwargs(DUCKDB_S3_ENDPOINT=None))
        assert s.effective_duckdb_endpoint == "minio:9000"

    def test_effective_duckdb_endpoint_uses_configured_value(self):
        s = Settings(**_minio_kwargs(DUCKDB_S3_ENDPOINT="s3.amazonaws.com"))
        assert s.effective_duckdb_endpoint == "s3.amazonaws.com"


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------


class TestDefaults:
    def test_dlq_max_defaults_to_zero(self):
        s = Settings(**_minio_kwargs())
        assert s.DLQ_MAX == 0

    def test_max_events_defaults_to_zero(self):
        s = Settings(**_minio_kwargs())
        assert s.MAX_EVENTS == 0

    def test_project_defaults(self):
        s = Settings(**_minio_kwargs())
        assert s.PROJECT == "de_pipeline"
        assert s.AWS_REGION == "us-east-1"
