"""
config/settings.py — Canonical configuration contract for de_template.

Uses pydantic-settings to load, validate, and type-check all environment
variables. Replaces ad-hoc .env parsing spread across render_sql.py,
validate.sh, wait_for_iceberg.py, etc.

Two usage modes:
  Production / scripts:
      cfg = load_settings()              # reads from .env + os.environ
      cfg = load_settings("env/aws.env") # override env file path

  Tests (isolated — no env file, no os.environ bleed):
      cfg = Settings(BROKER="kafka", STORAGE="minio", ...)
      # All values come exclusively from kwargs → clean, reproducible.
"""

from __future__ import annotations

import os
import re
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class Settings(BaseSettings):
    # env_file=None disables dotenv reading, but env_settings (os.environ) is
    # still active in the default source chain. We override customise_sources
    # to return ONLY init_settings so Settings() reads purely from kwargs —
    # no os.environ bleed, no file reading. This makes unit tests reliable even
    # when make's bare `export` directive injects .env vars into subprocess env.
    # load_settings() is the explicit production entry point that reads both.
    model_config = SettingsConfigDict(
        env_file=None,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Only kwargs. load_settings() supplies env vars explicitly as kwargs.
        return (init_settings,)

    # -------------------------------------------------------------------------
    # Four configuration axes
    # -------------------------------------------------------------------------
    BROKER: Literal["redpanda", "kafka"] = "redpanda"
    CATALOG: Literal["hadoop", "rest"] = "hadoop"
    STORAGE: Literal["minio", "aws_s3", "gcs", "azure"] = "minio"
    MODE: Literal["batch", "streaming_bronze"] = "batch"

    # -------------------------------------------------------------------------
    # Topic
    # -------------------------------------------------------------------------
    TOPIC: str = Field(..., description="Primary Kafka topic")
    DLQ_TOPIC: str = Field(..., description="Dead-letter topic")
    DLQ_MAX: int = 0

    # -------------------------------------------------------------------------
    # Data source
    # -------------------------------------------------------------------------
    DATA_PATH: str = Field(..., description="Generator input parquet path")
    MAX_EVENTS: int = 0
    GENERATOR_MODE: Literal["burst", "realtime", "batch"] = "burst"
    ALLOW_EMPTY: bool = False
    DATASET_NAME: str | None = None
    RUN_METRICS_MAX_AGE_MINUTES: int = 120
    ALLOW_STALE_RUN_METRICS: bool = False
    REQUIRE_RUN_METRICS: bool = True
    HOST_DATA_DIR: str | None = None

    # -------------------------------------------------------------------------
    # Warehouse (Flink S3A + Iceberg)
    # -------------------------------------------------------------------------
    WAREHOUSE: str = Field(..., description="Iceberg warehouse root (s3a://...)")
    S3_ENDPOINT: str = Field(..., description="Flink/Hadoop S3 endpoint")
    S3_USE_SSL: bool = False
    S3_PATH_STYLE: bool = True

    # -------------------------------------------------------------------------
    # DuckDB httpfs  (NO http:// prefix — DuckDB httpfs format requirement)
    # -------------------------------------------------------------------------
    DUCKDB_S3_ENDPOINT: str = Field(..., description="DuckDB httpfs endpoint")
    DUCKDB_S3_USE_SSL: bool = False

    # -------------------------------------------------------------------------
    # Iceberg table contract
    # -------------------------------------------------------------------------
    BRONZE_TABLE: str = "bronze.raw_trips"
    SILVER_TABLE: str = "silver.cleaned_trips"

    # -------------------------------------------------------------------------
    # Validation thresholds/timeouts (explicitly configurable)
    # -------------------------------------------------------------------------
    BRONZE_COMPLETENESS_RATIO: float = 0.95
    WAIT_FOR_SILVER_MIN_ROWS: int = 1
    WAIT_FOR_SILVER_TIMEOUT_SECONDS: int = 90
    WAIT_FOR_SILVER_POLL_SECONDS: int = 5
    HEALTH_HTTP_TIMEOUT_SECONDS: int = 5
    HEALTH_DOCKER_TIMEOUT_SECONDS: int = 15
    ICEBERG_QUERY_TIMEOUT_SECONDS: int = 90
    ICEBERG_METADATA_TIMEOUT_SECONDS: int = 30
    DLQ_READ_TIMEOUT_SECONDS: int = 10
    DBT_TEST_TIMEOUT_SECONDS: int = 300

    # -------------------------------------------------------------------------
    # Credentials
    # -------------------------------------------------------------------------
    MINIO_ROOT_USER: str | None = None
    MINIO_ROOT_PASSWORD: str | None = None
    AWS_ACCESS_KEY_ID: str | None = None
    AWS_SECRET_ACCESS_KEY: str | None = None
    AWS_REGION: str = "us-east-1"

    # -------------------------------------------------------------------------
    # Project
    # -------------------------------------------------------------------------
    PROJECT: str = "de_pipeline"

    # -------------------------------------------------------------------------
    # Convenience properties
    # -------------------------------------------------------------------------

    @property
    def warehouse_s3_path(self) -> str:
        """Warehouse path with s3:// prefix (for DuckDB iceberg_scan)."""
        return self.WAREHOUSE.replace("s3a://", "s3://")

    @property
    def effective_s3_key(self) -> str:
        return self.AWS_ACCESS_KEY_ID or ""

    @property
    def effective_s3_secret(self) -> str:
        return self.AWS_SECRET_ACCESS_KEY or ""

    @property
    def effective_duckdb_endpoint(self) -> str:
        return self.DUCKDB_S3_ENDPOINT

    @property
    def expected_dataset_name(self) -> str:
        if self.DATASET_NAME and self.DATASET_NAME.strip():
            return self.DATASET_NAME.strip()
        return self.TOPIC.split(".", 1)[0]

    @property
    def bronze_table_path(self) -> str:
        schema, table = self.BRONZE_TABLE.split(".", 1)
        return f"{self.warehouse_s3_path.rstrip('/')}/{schema}/{table}"

    @property
    def silver_table_path(self) -> str:
        schema, table = self.SILVER_TABLE.split(".", 1)
        return f"{self.warehouse_s3_path.rstrip('/')}/{schema}/{table}"

    @property
    def silver_metadata_glob(self) -> str:
        return f"{self.silver_table_path}/metadata/*.json"

    @property
    def bronze_completeness_pct(self) -> float:
        return self.BRONZE_COMPLETENESS_RATIO * 100.0

    # -------------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------------

    @field_validator("BROKER", "CATALOG", "STORAGE", "MODE", "GENERATOR_MODE", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        """Strip trailing whitespace that GNU make leaves after include .env."""
        return v.strip()

    @field_validator(
        "TOPIC",
        "DLQ_TOPIC",
        "DATA_PATH",
        "WAREHOUSE",
        "S3_ENDPOINT",
        "DUCKDB_S3_ENDPOINT",
        mode="before",
    )
    @classmethod
    def require_non_empty_string(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("must be a string")
        stripped = v.strip()
        if not stripped:
            raise ValueError("must be a non-empty string")
        return stripped

    @field_validator("BRONZE_TABLE", "SILVER_TABLE", mode="before")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("must be a string")
        table = v.strip()
        parts = table.split(".")
        if len(parts) != 2 or not all(parts):
            raise ValueError("must be in '<schema>.<table>' format")
        ident = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        if not ident.match(parts[0]) or not ident.match(parts[1]):
            raise ValueError("must be in '<schema>.<table>' format with valid identifiers")
        return table

    @field_validator("BRONZE_COMPLETENESS_RATIO")
    @classmethod
    def validate_completeness_ratio(cls, v: float) -> float:
        if v <= 0 or v > 1:
            raise ValueError("BRONZE_COMPLETENESS_RATIO must be > 0 and <= 1")
        return v

    @field_validator("WAIT_FOR_SILVER_MIN_ROWS")
    @classmethod
    def validate_wait_for_silver_min_rows(cls, v: int) -> int:
        if v < 0:
            raise ValueError("WAIT_FOR_SILVER_MIN_ROWS must be >= 0")
        return v

    @field_validator(
        "RUN_METRICS_MAX_AGE_MINUTES",
        "WAIT_FOR_SILVER_TIMEOUT_SECONDS",
        "WAIT_FOR_SILVER_POLL_SECONDS",
        "HEALTH_HTTP_TIMEOUT_SECONDS",
        "HEALTH_DOCKER_TIMEOUT_SECONDS",
        "ICEBERG_QUERY_TIMEOUT_SECONDS",
        "ICEBERG_METADATA_TIMEOUT_SECONDS",
        "DLQ_READ_TIMEOUT_SECONDS",
        "DBT_TEST_TIMEOUT_SECONDS",
    )
    @classmethod
    def validate_positive_ints(cls, v: int) -> int:
        if v < 1:
            raise ValueError("must be >= 1")
        return v

    @model_validator(mode="after")
    def validate_storage_combo(self) -> Settings:
        """Enforce rules between STORAGE axis and required credentials."""
        if self.STORAGE == "minio":
            if not self.MINIO_ROOT_USER:
                raise ValueError("STORAGE=minio requires MINIO_ROOT_USER in .env")
            if not self.MINIO_ROOT_PASSWORD:
                raise ValueError("STORAGE=minio requires MINIO_ROOT_PASSWORD in .env")
            if not self.AWS_ACCESS_KEY_ID:
                raise ValueError(
                    "STORAGE=minio requires AWS_ACCESS_KEY_ID to be explicitly set "
                    "(typically same value as MINIO_ROOT_USER)"
                )
            if not self.AWS_SECRET_ACCESS_KEY:
                raise ValueError(
                    "STORAGE=minio requires AWS_SECRET_ACCESS_KEY to be explicitly set "
                    "(typically same value as MINIO_ROOT_PASSWORD)"
                )

        if self.STORAGE == "aws_s3" and self.S3_ENDPOINT:
            if "minio" in self.S3_ENDPOINT.lower():
                raise ValueError(
                    "STORAGE=aws_s3 but S3_ENDPOINT contains 'minio'. "
                    "Use STORAGE=minio for local MinIO, or remove S3_ENDPOINT for AWS."
                )

        if not self.WAREHOUSE.startswith(("s3a://", "s3://")):
            raise ValueError("WAREHOUSE must start with s3a:// or s3://")

        if self.WAIT_FOR_SILVER_POLL_SECONDS >= self.WAIT_FOR_SILVER_TIMEOUT_SECONDS:
            raise ValueError(
                "WAIT_FOR_SILVER_POLL_SECONDS must be less than WAIT_FOR_SILVER_TIMEOUT_SECONDS"
            )

        return self


def load_settings(env_file: str = ".env") -> Settings:
    """Load and validate settings from an env file + os.environ.

    Manually parses the env file and merges with os.environ (os.environ wins),
    then passes only known Settings fields as explicit kwargs. This is required
    because settings_customise_sources returns only init_settings — the
    pydantic-settings dotenv and env source chain is intentionally disabled so
    that Settings() is a pure validation contract (no implicit env reads).

    os.environ takes precedence over env file values — same behaviour as
    Makefile's `include .env` + `export`.

    Activate a profile first:
        make env-select ENV=env/local.env   # copies → .env

    Requires host-side deps:
        make setup-dev   (runs: uv sync)

    Raises:
        ValidationError: if any axis value is invalid or required vars missing.
        ValueError: if axis combination is incompatible (e.g. minio without S3_ENDPOINT).
    """
    file_vals: dict[str, str] = {}
    try:
        with open(env_file, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                # Strip inline comments: "redpanda   # redpanda | kafka" → "redpanda"
                v = re.sub(r"\s+#.*$", "", v.strip())
                if k:
                    file_vals[k] = v
    except FileNotFoundError:
        pass
    merged = {**file_vals, **os.environ}  # os.environ wins
    known = {k: v for k, v in merged.items() if k in Settings.model_fields}
    return Settings(**known)
