"""
Typed dataset manifest contract for scaffold generation.

The dataset manifest is treated as an API contract:
  - strict required fields
  - `manifest_version` for forward compatibility
  - clear validation errors for common mistakes
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

REPO_ROOT = Path(__file__).parent.parent
DATASETS_DIR = REPO_ROOT / "datasets"
SUPPORTED_MANIFEST_VERSION = 1
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ManifestColumn(BaseModel):
    """Single field mapping from Kafka JSON -> Bronze -> Silver."""

    name: str
    kafka_type: str
    bronze_type: str
    silver_name: str | None = None
    silver_type: str
    is_event_ts: bool = False
    comment: str | None = None

    @field_validator("name", "kafka_type", "bronze_type", "silver_type")
    @classmethod
    def non_empty_required(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("must be a non-empty string")
        return value

    @field_validator("silver_name")
    @classmethod
    def optional_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("silver_name cannot be blank")
        if not _IDENTIFIER_RE.match(value):
            raise ValueError("silver_name must be a valid SQL identifier")
        return value

    @property
    def silver_output_name(self) -> str:
        return self.silver_name or self.name


class DatasetManifest(BaseModel):
    """Versioned contract for datasets/<name>/dataset.yml."""

    manifest_version: int = Field(..., description="Manifest schema version")
    name: str
    description: str = ""
    topic: str
    dlq_topic: str
    bronze_table: str
    silver_table: str
    event_ts_col: str
    ts_format: str
    partition_date_col: str
    partition_date_expr: str
    surrogate_key_name: str | None = None
    # Kafka message key field. Set to a column name for partition affinity
    # (all events with the same key land on the same partition, preserving order).
    # Leave unset or null for null keys (round-robin, maximum throughput).
    key_field: str | None = None
    # Watermark lag in seconds — how late events are tolerated before being dropped.
    # Increase for datasets with higher end-to-end latency or out-of-order delivery.
    watermark_interval_seconds: int = 10
    columns: list[ManifestColumn]
    dedup_key: list[str]
    surrogate_key_fields: list[str]
    quality_filters: list[str]

    @field_validator("manifest_version")
    @classmethod
    def validate_manifest_version(cls, value: int) -> int:
        if value != SUPPORTED_MANIFEST_VERSION:
            raise ValueError(
                f"unsupported manifest_version={value}; expected {SUPPORTED_MANIFEST_VERSION}"
            )
        return value

    @field_validator(
        "name",
        "topic",
        "dlq_topic",
        "event_ts_col",
        "ts_format",
        "partition_date_col",
        "partition_date_expr",
        mode="before",
    )
    @classmethod
    def strip_required_strings(cls, value: str) -> str:
        if not isinstance(value, str):
            raise ValueError("must be a string")
        value = value.strip()
        if not value:
            raise ValueError("must be a non-empty string")
        return value

    @field_validator("name", "event_ts_col", "partition_date_col")
    @classmethod
    def validate_required_identifier_fields(cls, value: str) -> str:
        if not _IDENTIFIER_RE.match(value):
            raise ValueError(f"'{value}' is not a valid SQL identifier")
        return value

    @field_validator("surrogate_key_name", "key_field")
    @classmethod
    def validate_optional_identifier_field(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            return None
        if not _IDENTIFIER_RE.match(value):
            raise ValueError(f"'{value}' is not a valid SQL identifier")
        return value

    @field_validator("watermark_interval_seconds")
    @classmethod
    def validate_watermark_interval(cls, value: int) -> int:
        if value < 1:
            raise ValueError("watermark_interval_seconds must be >= 1")
        return value

    @field_validator("bronze_table", "silver_table")
    @classmethod
    def validate_table_name(cls, value: str) -> str:
        value = value.strip()
        parts = value.split(".")
        if len(parts) != 2 or not all(parts):
            raise ValueError("must be in '<schema>.<table>' format")
        return value

    @field_validator("partition_date_expr")
    @classmethod
    def validate_partition_expr(cls, value: str) -> str:
        if ";" in value:
            raise ValueError("must not include ';'")
        return value

    @field_validator("quality_filters")
    @classmethod
    def validate_quality_filters(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("must include at least one filter expression")
        cleaned: list[str] = []
        for idx, expr in enumerate(value):
            if not isinstance(expr, str):
                raise ValueError(f"quality_filters[{idx}] must be a string")
            expr = expr.strip()
            if not expr:
                raise ValueError(f"quality_filters[{idx}] cannot be blank")
            if ";" in expr:
                raise ValueError(f"quality_filters[{idx}] must not include ';'")
            cleaned.append(expr)
        return cleaned

    @model_validator(mode="after")
    def validate_cross_field_constraints(self) -> DatasetManifest:
        col_names = [c.name for c in self.columns]
        if len(col_names) != len(set(col_names)):
            duplicates = sorted({name for name in col_names if col_names.count(name) > 1})
            raise ValueError(f"columns contain duplicate names: {', '.join(duplicates)}")

        event_cols = [c.name for c in self.columns if c.is_event_ts]
        if len(event_cols) != 1:
            raise ValueError(
                f"exactly one column must have is_event_ts: true; found {len(event_cols)}"
            )
        if self.event_ts_col not in col_names:
            raise ValueError(
                f"event_ts_col '{self.event_ts_col}' not found in columns: {', '.join(col_names)}"
            )
        if event_cols[0] != self.event_ts_col:
            raise ValueError(
                f"event_ts_col is '{self.event_ts_col}' but is_event_ts is set on '{event_cols[0]}'"
            )

        unknown_dedup = sorted(set(self.dedup_key) - set(col_names))
        if unknown_dedup:
            raise ValueError(f"dedup_key contains unknown column(s): {', '.join(unknown_dedup)}")

        unknown_surrogate = sorted(set(self.surrogate_key_fields) - set(col_names))
        if unknown_surrogate:
            raise ValueError(
                f"surrogate_key_fields contains unknown column(s): {', '.join(unknown_surrogate)}"
            )

        if self.key_field is not None and self.key_field not in col_names:
            raise ValueError(
                f"key_field '{self.key_field}' not found in columns: {', '.join(col_names)}"
            )

        return self

    @property
    def silver_schema(self) -> str:
        return self.silver_table.split(".", 1)[0]

    @property
    def silver_table_name(self) -> str:
        return self.silver_table.split(".", 1)[1]

    @property
    def event_ts_silver_col(self) -> str:
        event_col = next(c for c in self.columns if c.is_event_ts)
        return event_col.silver_output_name

    @property
    def resolved_surrogate_key_name(self) -> str:
        return self.surrogate_key_name or f"{self.name}_id"


def manifest_path(dataset_name: str) -> Path:
    return DATASETS_DIR / dataset_name / "dataset.yml"


def available_datasets() -> list[str]:
    return sorted(
        p.name for p in DATASETS_DIR.iterdir() if p.is_dir() and not p.name.startswith("_")
    )


def load_manifest_yaml(path: Path) -> dict:
    with open(path, encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            raise ValueError("manifest root must be a YAML mapping/object")
        return payload


def parse_manifest(dataset_name: str) -> tuple[Path, DatasetManifest]:
    path = manifest_path(dataset_name)
    if not path.exists():
        available = ", ".join(available_datasets()) or "(none yet)"
        raise FileNotFoundError(
            f"Dataset manifest not found: {path}\nAvailable datasets: {available}"
        )
    payload = load_manifest_yaml(path)
    try:
        return path, DatasetManifest.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(exc) from exc
