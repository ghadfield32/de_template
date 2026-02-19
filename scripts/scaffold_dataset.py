"""
scripts/scaffold_dataset.py — Generate SQL templates + dbt staging from a dataset manifest.

Usage:
    uv run python scripts/scaffold_dataset.py taxi
    make scaffold DATASET=taxi

What it generates (overwrites existing files):
    flink/sql/05_bronze_batch.sql.tmpl    ← Bronze Kafka→Iceberg batch job
    flink/sql/06_silver.sql.tmpl          ← Silver dedup + clean + partitioned
    flink/sql/07_bronze_streaming.sql.tmpl ← Bronze streaming job (runs forever)
    dbt/models/staging/stg_<name>.sql     ← dbt passthrough staging model

After running, execute:
    make build-sql    ← renders .tmpl → build/sql/*.sql
    make validate-config
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

REPO_ROOT = Path(__file__).parent.parent
DATASETS_DIR = REPO_ROOT / "datasets"
TEMPLATE_DIR = DATASETS_DIR / "_template"
FLINK_SQL_DIR = REPO_ROOT / "flink" / "sql"
DBT_STAGING_DIR = REPO_ROOT / "dbt" / "models" / "staging"


def load_manifest(dataset_name: str) -> dict:
    manifest_path = DATASETS_DIR / dataset_name / "dataset.yml"
    if not manifest_path.exists():
        available = sorted(
            p.name for p in DATASETS_DIR.iterdir() if p.is_dir() and not p.name.startswith("_")
        )
        print(
            f"ERROR: Dataset manifest not found: {manifest_path}\n"
            f"Available datasets: {', '.join(available) or '(none yet)'}",
            file=sys.stderr,
        )
        sys.exit(1)
    with open(manifest_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_jinja_env() -> Environment:
    env = Environment(
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        undefined=StrictUndefined,  # fail loudly on missing variables
        trim_blocks=True,
        lstrip_blocks=True,
        keep_trailing_newline=True,
    )
    # Custom filter: left-justify a string (for SQL column alignment)
    env.filters["ljust"] = lambda s, width: str(s).ljust(width)
    return env


def render(env: Environment, template_name: str, context: dict) -> str:
    tmpl = env.get_template(template_name)
    return tmpl.render(**context)


def write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    print(f"  wrote  {path.relative_to(REPO_ROOT)}")


def validate_manifest(manifest: dict) -> None:
    required = [
        "name",
        "topic",
        "dlq_topic",
        "bronze_table",
        "silver_table",
        "event_ts_col",
        "ts_format",
        "partition_date_col",
        "partition_date_expr",
        "columns",
        "dedup_key",
        "surrogate_key_fields",
        "quality_filters",
    ]
    missing = [k for k in required if k not in manifest]
    if missing:
        print(
            f"ERROR: dataset.yml is missing required keys: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    event_ts_cols = [c for c in manifest["columns"] if c.get("is_event_ts")]
    if len(event_ts_cols) != 1:
        print(
            f"ERROR: Exactly one column must have is_event_ts: true (found {len(event_ts_cols)})",
            file=sys.stderr,
        )
        sys.exit(1)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1].startswith("-"):
        print("Usage: python scripts/scaffold_dataset.py <dataset_name>", file=sys.stderr)
        print("Example: python scripts/scaffold_dataset.py taxi", file=sys.stderr)
        sys.exit(1)

    dataset_name = sys.argv[1]
    print(f"\nScaffolding dataset: {dataset_name}")
    print(f"Manifest:  datasets/{dataset_name}/dataset.yml")
    print("Templates: datasets/_template/")
    print()

    manifest = load_manifest(dataset_name)
    validate_manifest(manifest)

    # Add a helper for max() used in the silver template's padding expression
    # We pass it as a function in context rather than a Jinja filter
    manifest["max"] = max

    env = build_jinja_env()

    outputs = [
        ("bronze.sql.j2", FLINK_SQL_DIR / "05_bronze_batch.sql.tmpl"),
        ("silver.sql.j2", FLINK_SQL_DIR / "06_silver.sql.tmpl"),
        ("streaming.sql.j2", FLINK_SQL_DIR / "07_bronze_streaming.sql.tmpl"),
        ("stg.sql.j2", DBT_STAGING_DIR / f"stg_{manifest['name']}.sql"),
    ]

    for template_name, output_path in outputs:
        try:
            content = render(env, template_name, manifest)
        except Exception as exc:
            print(f"ERROR rendering {template_name}: {exc}", file=sys.stderr)
            sys.exit(1)
        write_file(output_path, content)

    print()
    print("Done. Next steps:")
    print("  make build-sql        # render .tmpl -> build/sql/*.sql")
    print("  make validate-config  # check active .env is consistent")
    print("  make show-sql         # inspect rendered output before starting Flink")


if __name__ == "__main__":
    main()
