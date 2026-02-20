"""
scripts/scaffold_dataset.py — Generate SQL + dbt assets from dataset manifests.

Usage:
    uv run python scripts/scaffold_dataset.py taxi
    uv run python scripts/scaffold_dataset.py orders --with-mart-stub
    make scaffold DATASET=taxi
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from scripts.dataset_manifest import (  # noqa: E402
    DATASETS_DIR,
    REPO_ROOT,
    DatasetManifest,
    parse_manifest,
)

TEMPLATE_DIR = DATASETS_DIR / "_template"
FLINK_SQL_DIR = REPO_ROOT / "flink" / "sql"
DBT_STAGING_DIR = REPO_ROOT / "dbt" / "models" / "staging"
DBT_SOURCES_DIR = REPO_ROOT / "dbt" / "models" / "sources"
DBT_MARTS_CORE_DIR = REPO_ROOT / "dbt" / "models" / "marts" / "core"
REGISTRY_PATH = DATASETS_DIR / "registry.yml"

_NUMERIC_FILTER_RE = re.compile(
    r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*(>=|>|<=|<)\s*(-?\d+(?:\.\d+)?)\s*$"
)
_NUMERIC_TYPES = ("TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "DOUBLE", "FLOAT", "REAL")


def build_jinja_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    env.filters["ljust"] = lambda value, width: str(value).ljust(width)
    return env


def render(env: Environment, template_name: str, context: dict[str, Any]) -> str:
    template = env.get_template(template_name)
    return template.render(**context)


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"  wrote  {path.relative_to(REPO_ROOT)}")


def _is_numeric_sql_type(sql_type: str) -> bool:
    normalized = sql_type.strip().upper()
    if normalized.startswith("DECIMAL(") or normalized.startswith("NUMERIC("):
        return True
    return normalized in _NUMERIC_TYPES


def _to_number(raw: str) -> int | float:
    return float(raw) if "." in raw else int(raw)


def _is_stricter_lower(
    candidate: tuple[int | float, bool], current: tuple[int | float, bool]
) -> bool:
    cand_value, cand_inclusive = candidate
    cur_value, cur_inclusive = current
    if cand_value > cur_value:
        return True
    if cand_value < cur_value:
        return False
    return not cand_inclusive and cur_inclusive


def _is_stricter_upper(
    candidate: tuple[int | float, bool], current: tuple[int | float, bool]
) -> bool:
    cand_value, cand_inclusive = candidate
    cur_value, cur_inclusive = current
    if cand_value < cur_value:
        return True
    if cand_value > cur_value:
        return False
    return not cand_inclusive and cur_inclusive


def extract_range_tests(manifest: DatasetManifest) -> list[dict[str, Any]]:
    """
    Infer basic accepted_range tests from simple numeric quality filter expressions.

    Example supported expressions:
      - total_amount > 0
      - passenger_count >= 0
      - trip_distance <= 200
    """

    raw_to_silver = {col.name: col.silver_output_name for col in manifest.columns}
    numeric_silver_cols = {
        col.silver_output_name for col in manifest.columns if _is_numeric_sql_type(col.silver_type)
    }

    lower_bounds: dict[str, tuple[int | float, bool]] = {}
    upper_bounds: dict[str, tuple[int | float, bool]] = {}

    for expr in manifest.quality_filters:
        match = _NUMERIC_FILTER_RE.match(expr)
        if not match:
            continue

        raw_col, op, number_raw = match.groups()
        silver_col = raw_to_silver.get(raw_col)
        if silver_col is None or silver_col not in numeric_silver_cols:
            continue

        value = _to_number(number_raw)
        if op in (">", ">="):
            candidate = (value, op == ">=")
            current = lower_bounds.get(silver_col)
            if current is None or _is_stricter_lower(candidate, current):
                lower_bounds[silver_col] = candidate
        else:
            candidate = (value, op == "<=")
            current = upper_bounds.get(silver_col)
            if current is None or _is_stricter_upper(candidate, current):
                upper_bounds[silver_col] = candidate

    tests: list[dict[str, Any]] = []
    for column in sorted(set(lower_bounds) | set(upper_bounds)):
        entry: dict[str, Any] = {"column": column}
        inclusive = True
        if column in lower_bounds:
            min_value, min_inclusive = lower_bounds[column]
            entry["min_value"] = min_value
            inclusive = inclusive and min_inclusive
        if column in upper_bounds:
            max_value, max_inclusive = upper_bounds[column]
            entry["max_value"] = max_value
            inclusive = inclusive and max_inclusive
        entry["inclusive"] = inclusive
        tests.append(entry)
    return tests


def build_context(manifest: DatasetManifest) -> dict[str, Any]:
    context = manifest.model_dump(mode="python")
    context["columns"] = [column.model_dump(mode="python") for column in manifest.columns]
    context["max"] = max
    context["source_name"] = f"raw_{manifest.name}"
    context["source_table_name"] = f"cleaned_{manifest.name}"
    context["silver_schema"] = manifest.silver_schema
    context["silver_table_name"] = manifest.silver_table_name
    context["event_ts_silver_col"] = manifest.event_ts_silver_col
    context["surrogate_key_name"] = manifest.resolved_surrogate_key_name
    context["range_tests"] = extract_range_tests(manifest)
    return context


def update_registry(dataset_name: str, manifest_relpath: str, output_files: list[Path]) -> None:
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH, encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
    else:
        payload = {}

    version = payload.get("version", 1)
    datasets = payload.get("datasets", [])
    if not isinstance(datasets, list):
        datasets = []

    by_name: dict[str, dict[str, Any]] = {}
    for item in datasets:
        if isinstance(item, dict) and item.get("name"):
            by_name[str(item["name"])] = item

    existing = by_name.get(dataset_name, {})
    scaffolded_at: str | None
    if isinstance(existing, dict) and "scaffolded_at" in existing:
        scaffolded_at = existing.get("scaffolded_at")
    else:
        scaffolded_at = datetime.now(tz=UTC).isoformat()

    by_name[dataset_name] = {
        "name": dataset_name,
        "manifest": manifest_relpath,
        "scaffolded_at": scaffolded_at,
        "outputs": [str(path.relative_to(REPO_ROOT)) for path in output_files],
    }

    updated_payload = {
        "version": version,
        "datasets": [by_name[name] for name in sorted(by_name)],
    }
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REGISTRY_PATH, "w", encoding="utf-8") as handle:
        yaml.safe_dump(updated_payload, handle, sort_keys=False)
    print(f"  wrote  {REGISTRY_PATH.relative_to(REPO_ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate SQL templates + dbt model/source/tests from a dataset manifest."
    )
    parser.add_argument(
        "dataset_name", help="Dataset folder name under datasets/<name>/dataset.yml"
    )
    parser.add_argument(
        "--with-mart-stub",
        action="store_true",
        help="Also generate dbt/models/marts/core/mart_<name>.sql stub.",
    )
    args = parser.parse_args()

    dataset_name = args.dataset_name
    print(f"\nScaffolding dataset: {dataset_name}")
    print(f"Manifest:  datasets/{dataset_name}/dataset.yml")
    print("Templates: datasets/_template/")
    print()

    try:
        manifest_path, manifest = parse_manifest(dataset_name)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(f"ERROR: Invalid manifest at datasets/{dataset_name}/dataset.yml", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    context = build_context(manifest)
    env = build_jinja_env()

    outputs: list[tuple[str, Path]] = [
        ("bronze.sql.j2", FLINK_SQL_DIR / "05_bronze_batch.sql.tmpl"),
        ("silver.sql.j2", FLINK_SQL_DIR / "06_silver.sql.tmpl"),
        ("streaming.sql.j2", FLINK_SQL_DIR / "07_bronze_streaming.sql.tmpl"),
        ("stg.sql.j2", DBT_STAGING_DIR / f"stg_{manifest.name}.sql"),
        ("source.yml.j2", DBT_SOURCES_DIR / f"{manifest.name}_sources.yml"),
        ("staging_tests.yml.j2", DBT_STAGING_DIR / f"stg_{manifest.name}.yml"),
    ]
    if args.with_mart_stub:
        outputs.append(("mart.sql.j2", DBT_MARTS_CORE_DIR / f"mart_{manifest.name}.sql"))

    written_files: list[Path] = []
    for template_name, output_path in outputs:
        try:
            content = render(env, template_name, context)
        except Exception as exc:
            print(f"ERROR rendering {template_name}: {exc}", file=sys.stderr)
            sys.exit(1)
        write_file(output_path, content)
        written_files.append(output_path)

    update_registry(
        dataset_name=manifest.name,
        manifest_relpath=str(manifest_path.relative_to(REPO_ROOT)),
        output_files=written_files,
    )

    print()
    print("Done. Next steps:")
    print("  make build-sql        # render .tmpl -> build/sql/*.sql")
    print("  make validate-config  # check active .env is consistent")
    print("  make show-sql         # inspect rendered output before starting Flink")


if __name__ == "__main__":
    main()
