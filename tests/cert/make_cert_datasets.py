"""Manifest-driven certification dataset generator.

Generates deterministic Parquet fixtures used by `make certify-real` and
`make certify-edge-cases` to verify data correctness end-to-end.

Usage:
    uv run python tests/cert/make_cert_datasets.py [--dataset taxi]

Output (gitignored, in build/cert/):
    build/cert/{dataset}_clean.parquet       -- 1 000 valid rows, all pass quality_filters
    build/cert/{dataset}_edge_cases.parquet  -- 50 valid + 1 violation/filter + 20 dups
    build/cert/cert_manifest.json            -- expected counts per scenario
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Add repo root to sys.path so scripts.dataset_manifest is importable
REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.dataset_manifest import DatasetManifest, ManifestColumn, available_datasets, parse_manifest

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SEED = 42
N_VALID_CLEAN = 1_000  # rows in the clean dataset
N_VALID_BASE = 50      # valid base rows in edge-case dataset
N_DUPS = 20            # exact duplicate rows appended to edge-case dataset


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------

def _pa_type(bronze_type: str) -> pa.DataType:
    """Map Flink bronze_type string → pyarrow DataType."""
    bt = bronze_type.strip().upper()
    if bt == "BIGINT":
        return pa.int64()
    if bt == "INT":
        return pa.int32()
    if bt in ("DOUBLE", "FLOAT"):
        return pa.float64()
    if bt.startswith("DECIMAL"):
        return pa.float64()
    if bt.startswith("TIMESTAMP"):
        return pa.timestamp("us")
    return pa.string()


# ---------------------------------------------------------------------------
# Valid date-range extraction from quality_filters
# ---------------------------------------------------------------------------

_GTE_DATE = re.compile(
    r"CAST\s*\(\s*(\w+)\s+AS\s+DATE\s*\)\s*>=\s*DATE\s+'(\d{4}-\d{2}-\d{2})'",
    re.IGNORECASE,
)
_LT_DATE = re.compile(
    r"CAST\s*\(\s*(\w+)\s+AS\s+DATE\s*\)\s*<\s*DATE\s+'(\d{4}-\d{2}-\d{2})'",
    re.IGNORECASE,
)


def _extract_date_range(filters: list[str]) -> tuple[datetime, datetime]:
    """
    Scan quality_filters for CAST(col AS DATE) range constraints.
    Returns (valid_from, valid_before) as datetimes.
    Falls back to Jan 2024 if no date patterns are found.
    """
    valid_from = datetime(2024, 1, 1)
    valid_before = datetime(2024, 2, 1)

    for f in filters:
        m = _GTE_DATE.search(f)
        if m:
            valid_from = datetime.strptime(m.group(2), "%Y-%m-%d")
        m = _LT_DATE.search(f)
        if m:
            valid_before = datetime.strptime(m.group(2), "%Y-%m-%d")

    if valid_from >= valid_before:
        # Degenerate range — use safe fallback
        valid_from = datetime(2024, 1, 1)
        valid_before = datetime(2024, 2, 1)

    return valid_from, valid_before


# ---------------------------------------------------------------------------
# Valid value generation
# ---------------------------------------------------------------------------

def _valid_value(
    col: ManifestColumn,
    idx: int,
    rng: random.Random,
    valid_from: datetime,
    valid_before: datetime,
) -> object:
    """Return a value for `col` that passes all quality_filters."""
    bt = col.bronze_type.strip().upper()

    if bt.startswith("TIMESTAMP"):
        delta_s = int((valid_before - valid_from).total_seconds())
        if delta_s < 1:
            delta_s = 1
        return valid_from + timedelta(seconds=rng.randint(0, delta_s - 1))

    if bt == "BIGINT":
        return rng.randint(1, 100)

    if bt == "INT":
        return rng.randint(1, 10)

    if bt in ("DOUBLE", "FLOAT") or bt.startswith("DECIMAL"):
        # Keep > 0 to pass `col > 0` and `col >= 0` quality filters
        return round(rng.uniform(1.0, 99.0), 2)

    # STRING
    return f"val_{idx}"


# ---------------------------------------------------------------------------
# Quality filter violation helpers
# ---------------------------------------------------------------------------

_IS_NOT_NULL = re.compile(r"^(\w+)\s+IS\s+NOT\s+NULL$", re.IGNORECASE)
_COL_GT_N = re.compile(r"^(\w+)\s*>\s*([\d.]+)$")
_COL_GTE_N = re.compile(r"^(\w+)\s*>=\s*([\d.]+)$")
_CAST_GTE_DATE = re.compile(
    r"CAST\s*\(\s*(\w+)\s+AS\s+DATE\s*\)\s*>=\s*DATE\s+'(\d{4}-\d{2}-\d{2})'",
    re.IGNORECASE,
)
_CAST_LT_DATE = re.compile(
    r"CAST\s*\(\s*(\w+)\s+AS\s+DATE\s*\)\s*<\s*DATE\s+'(\d{4}-\d{2}-\d{2})'",
    re.IGNORECASE,
)


def _violation_override(
    filter_sql: str,
    col_map: dict[str, ManifestColumn],
) -> dict | None:
    """
    Return a {col_name: bad_value} dict that violates exactly this filter,
    or None if the filter pattern is not recognized.
    """
    f = filter_sql.strip()

    m = _IS_NOT_NULL.match(f)
    if m:
        col = m.group(1)
        if col in col_map:
            return {col: None}

    m = _COL_GT_N.match(f)
    if m:
        col, threshold = m.group(1), float(m.group(2))
        if col in col_map:
            # value == threshold violates `col > threshold`
            return {col: threshold}

    m = _COL_GTE_N.match(f)
    if m:
        col, threshold = m.group(1), float(m.group(2))
        if col in col_map:
            # value = threshold - 1 violates `col >= threshold`
            return {col: threshold - 1.0}

    m = _CAST_GTE_DATE.search(f)
    if m:
        col, date_str = m.group(1), m.group(2)
        if col in col_map:
            dt = datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=1)
            return {col: dt}

    m = _CAST_LT_DATE.search(f)
    if m:
        col, date_str = m.group(1), m.group(2)
        if col in col_map:
            # the excluded upper bound itself violates the strict-less-than filter
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            return {col: dt}

    return None  # unrecognized pattern — skip this filter


# ---------------------------------------------------------------------------
# Cross-column constraint fixup
# ---------------------------------------------------------------------------

def _apply_cross_column_constraints(
    row: dict,
    manifest: DatasetManifest,
    rng: random.Random,
    overrides: dict | None = None,
) -> dict:
    """
    Fix cross-column constraints not captured in quality_filters (e.g. dbt tests).
    Skips fixup if either column in a pair was explicitly overridden (violation rows).
    """
    col_names = {c.name for c in manifest.columns}
    ov = overrides or {}

    # Ensure total_amount >= fare_amount
    # (dbt singular test: fare_amount > total_amount + 0.01 should return 0 rows)
    if (
        "fare_amount" in col_names
        and "total_amount" in col_names
        and "fare_amount" not in ov
        and "total_amount" not in ov
    ):
        fare = row.get("fare_amount")
        total = row.get("total_amount")
        if fare is not None and total is not None and fare > total:
            row["total_amount"] = round(fare + round(rng.uniform(0.1, 10.0), 2), 2)

    # Ensure tpep_dropoff_datetime > tpep_pickup_datetime
    # (dbt singular test: trip_duration_minutes < 0 should return 0 rows)
    if (
        "tpep_pickup_datetime" in col_names
        and "tpep_dropoff_datetime" in col_names
        and "tpep_pickup_datetime" not in ov
        and "tpep_dropoff_datetime" not in ov
    ):
        pickup = row.get("tpep_pickup_datetime")
        dropoff = row.get("tpep_dropoff_datetime")
        if pickup is not None and dropoff is not None and dropoff <= pickup:
            row["tpep_dropoff_datetime"] = pickup + timedelta(minutes=rng.randint(1, 60))

    return row


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------

def _build_row(
    manifest: DatasetManifest,
    idx: int,
    rng: random.Random,
    valid_from: datetime,
    valid_before: datetime,
    overrides: dict | None = None,
) -> dict:
    """Build one row dict. `overrides` replaces specific column values."""
    row: dict = {}
    for col in manifest.columns:
        row[col.name] = _valid_value(col, idx, rng, valid_from, valid_before)
    if overrides:
        row.update(overrides)
    _apply_cross_column_constraints(row, manifest, rng, overrides)
    return row


def _rows_to_table(rows: list[dict], manifest: DatasetManifest) -> pa.Table:
    """Convert list of row dicts → pyarrow Table with schema matching manifest."""
    arrays = []
    schema_fields = []
    for col in manifest.columns:
        pa_type = _pa_type(col.bronze_type)
        values = [r[col.name] for r in rows]
        arrays.append(pa.array(values, type=pa_type))
        schema_fields.append(pa.field(col.name, pa_type))
    schema = pa.schema(schema_fields)
    return pa.table(
        {f.name: arr for f, arr in zip(schema_fields, arrays)},
        schema=schema,
    )


# ---------------------------------------------------------------------------
# Public dataset generators
# ---------------------------------------------------------------------------

def make_clean(
    manifest: DatasetManifest,
    n: int = N_VALID_CLEAN,
) -> tuple[pa.Table, int]:
    """
    Generate a clean dataset: `n` rows all passing quality_filters.
    Returns (table, row_count).
    """
    rng = random.Random(SEED)
    valid_from, valid_before = _extract_date_range(manifest.quality_filters)
    rows = [_build_row(manifest, i, rng, valid_from, valid_before) for i in range(n)]
    return _rows_to_table(rows, manifest), n


def make_edge_cases(manifest: DatasetManifest) -> tuple[pa.Table, dict]:
    """
    Generate an edge-case dataset composed of three groups:
      Group A: N_VALID_BASE valid rows (all pass quality_filters)
      Group B: one violation row per quality_filter (each fails exactly one filter)
      Group C: N_DUPS exact duplicates of the first N_DUPS valid rows

    Returns (table, meta) where meta contains expected row counts for each layer.
    """
    rng = random.Random(SEED + 1)
    valid_from, valid_before = _extract_date_range(manifest.quality_filters)
    col_map = {c.name: c for c in manifest.columns}

    # --- Group A: valid base rows ---
    valid_rows = [
        _build_row(manifest, i, rng, valid_from, valid_before)
        for i in range(N_VALID_BASE)
    ]

    # --- Group B: one violation per quality_filter ---
    violation_rows: list[dict] = []
    for filt in manifest.quality_filters:
        override = _violation_override(filt, col_map)
        if override is None:
            continue  # filter pattern not recognized — skip
        base_idx = N_VALID_BASE + len(violation_rows)
        row = _build_row(
            manifest, base_idx, rng, valid_from, valid_before, overrides=override
        )
        violation_rows.append(row)

    # --- Group C: exact duplicates of first N_DUPS valid rows ---
    dup_rows = [dict(r) for r in valid_rows[:N_DUPS]]

    all_rows = valid_rows + violation_rows + dup_rows
    table = _rows_to_table(all_rows, manifest)

    n_violations = len(violation_rows)
    meta = {
        "parquet_rows": len(all_rows),
        "expected_bronze": len(all_rows),
        # Valid rows survive quality filters; dups are deduped away by ROW_NUMBER()
        "expected_silver": N_VALID_BASE,
        "expected_dropped": n_violations,
        "expected_deduped": N_DUPS,
        # Breakdown for test assertions
        "n_valid_base": N_VALID_BASE,
        "n_violation_rows": n_violations,
        "n_dup_rows": N_DUPS,
    }
    return table, meta


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate deterministic certification Parquet datasets."
    )
    ap.add_argument(
        "--dataset",
        default=None,
        help="Dataset name (default: first available in datasets/)",
    )
    args = ap.parse_args()

    dataset_name = args.dataset
    if dataset_name is None:
        datasets = available_datasets()
        if not datasets:
            print("ERROR: no datasets found in datasets/", file=sys.stderr)
            sys.exit(1)
        dataset_name = datasets[0]

    print(f"Generating certification datasets for '{dataset_name}'...")
    _, manifest = parse_manifest(dataset_name)

    out_dir = REPO_ROOT / "build" / "cert"
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Clean dataset ---
    clean_table, n_clean = make_clean(manifest)
    clean_path = out_dir / f"{dataset_name}_clean.parquet"
    pq.write_table(clean_table, clean_path, compression="snappy")
    print(f"  clean:      {clean_path.relative_to(REPO_ROOT)}  ({n_clean:,} rows)")

    # --- Edge-case dataset ---
    edge_table, edge_meta = make_edge_cases(manifest)
    edge_path = out_dir / f"{dataset_name}_edge_cases.parquet"
    pq.write_table(edge_table, edge_path, compression="snappy")
    print(
        f"  edge_cases: {edge_path.relative_to(REPO_ROOT)}"
        f"  ({edge_meta['parquet_rows']:,} rows"
        f" = {edge_meta['n_valid_base']} valid"
        f" + {edge_meta['n_violation_rows']} violations"
        f" + {edge_meta['n_dup_rows']} dups)"
    )

    # --- cert_manifest.json (merge with existing if present) ---
    manifest_path = out_dir / "cert_manifest.json"
    existing: dict = {}
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text())

    existing[dataset_name] = {
        "cert_clean": {
            "parquet_rows": n_clean,
            "expected_bronze": n_clean,
            "expected_silver": n_clean,
            "expected_dropped": 0,
            "expected_deduped": 0,
        },
        "cert_edge_cases": edge_meta,
    }
    manifest_path.write_text(json.dumps(existing, indent=2))
    print(f"  manifest:   {manifest_path.relative_to(REPO_ROOT)}")
    print(f"\nDone. CERT_DATA_DIR={out_dir.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
