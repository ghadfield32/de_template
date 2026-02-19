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
from typing import Literal, Optional

from pydantic import field_validator, model_validator
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
    TOPIC: str = "taxi.raw_trips"
    DLQ_TOPIC: str = "taxi.raw_trips.dlq"
    DLQ_MAX: int = 0

    # -------------------------------------------------------------------------
    # Data source
    # -------------------------------------------------------------------------
    DATA_PATH: str = "/data/yellow_tripdata_2024-01.parquet"
    MAX_EVENTS: int = 0
    HOST_DATA_DIR: Optional[str] = None

    # -------------------------------------------------------------------------
    # Warehouse (Flink S3A + Iceberg)
    # -------------------------------------------------------------------------
    WAREHOUSE: str = "s3a://warehouse/"
    S3_ENDPOINT: Optional[str] = None
    S3_USE_SSL: bool = False
    S3_PATH_STYLE: bool = True

    # -------------------------------------------------------------------------
    # DuckDB httpfs  (NO http:// prefix — DuckDB httpfs format requirement)
    # -------------------------------------------------------------------------
    DUCKDB_S3_ENDPOINT: Optional[str] = None
    DUCKDB_S3_USE_SSL: bool = False

    # -------------------------------------------------------------------------
    # Credentials
    # -------------------------------------------------------------------------
    MINIO_ROOT_USER: Optional[str] = None
    MINIO_ROOT_PASSWORD: Optional[str] = None
    AWS_ACCESS_KEY_ID: Optional[str] = None
    AWS_SECRET_ACCESS_KEY: Optional[str] = None
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
        return self.AWS_ACCESS_KEY_ID or self.MINIO_ROOT_USER or ""

    @property
    def effective_s3_secret(self) -> str:
        return self.AWS_SECRET_ACCESS_KEY or self.MINIO_ROOT_PASSWORD or ""

    @property
    def effective_duckdb_endpoint(self) -> str:
        return self.DUCKDB_S3_ENDPOINT or "minio:9000"

    # -------------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------------

    @field_validator("BROKER", "CATALOG", "STORAGE", "MODE", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        """Strip trailing whitespace that GNU make leaves after include .env."""
        return v.strip()

    @model_validator(mode="after")
    def validate_storage_combo(self) -> Settings:
        """Enforce rules between STORAGE axis and required credentials."""
        if self.STORAGE == "minio":
            if not self.S3_ENDPOINT:
                raise ValueError(
                    "STORAGE=minio requires S3_ENDPOINT (e.g. http://minio:9000). "
                    "Check your .env or run: make env-select ENV=env/local.env"
                )
            if not self.MINIO_ROOT_USER:
                raise ValueError("STORAGE=minio requires MINIO_ROOT_USER in .env")
            if not self.MINIO_ROOT_PASSWORD:
                raise ValueError("STORAGE=minio requires MINIO_ROOT_PASSWORD in .env")

        if self.STORAGE == "aws_s3" and self.S3_ENDPOINT:
            if "minio" in self.S3_ENDPOINT.lower():
                raise ValueError(
                    "STORAGE=aws_s3 but S3_ENDPOINT contains 'minio'. "
                    "Use STORAGE=minio for local MinIO, or remove S3_ENDPOINT for AWS."
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
