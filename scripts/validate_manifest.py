#!/usr/bin/env python3
"""
Validate datasets/<name>/dataset.yml against the typed manifest schema.

Usage:
    uv run python scripts/validate_manifest.py taxi
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

from scripts.dataset_manifest import parse_manifest  # noqa: E402


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1].startswith("-"):
        print("Usage: python scripts/validate_manifest.py <dataset_name>", file=sys.stderr)
        print("Example: python scripts/validate_manifest.py taxi", file=sys.stderr)
        sys.exit(1)

    dataset_name = sys.argv[1]
    try:
        path, manifest = parse_manifest(dataset_name)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    except ValueError as exc:
        print(f"ERROR: Invalid manifest at datasets/{dataset_name}/dataset.yml", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        sys.exit(1)

    print("Manifest validation passed.")
    print(f"  Dataset:          {manifest.name}")
    print(f"  File:             {path}")
    print(f"  Manifest version: {manifest.manifest_version}")
    print(f"  Columns:          {len(manifest.columns)}")
    print(f"  Bronze table:     {manifest.bronze_table}")
    print(f"  Silver table:     {manifest.silver_table}")
    print(f"  Event timestamp:  {manifest.event_ts_col} -> {manifest.event_ts_silver_col}")


if __name__ == "__main__":
    main()
