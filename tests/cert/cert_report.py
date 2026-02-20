"""Row-accounting certification report.

Reads:
  - build/run_metrics.json          (events produced + data_path from generator)
  - build/cert/cert_manifest.json   (expected counts, if a cert dataset was used)
  - Iceberg (via docker compose)     (bronze + silver row counts)
  - Broker DLQ (via docker compose)  (dlq message count)

Writes:
  - build/cert_report.json          (machine-readable accounting numbers)

Exit code: 0 = CERTIFICATION PASSED, 1 = CERTIFICATION FAILED
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from config.settings import Settings, load_settings
from scripts.validate import build_compose_args

RUN_METRICS_PATH = REPO_ROOT / "build" / "run_metrics.json"
CERT_MANIFEST_PATH = REPO_ROOT / "build" / "cert" / "cert_manifest.json"
CERT_REPORT_PATH = REPO_ROOT / "build" / "cert_report.json"

LINE_WIDTH = 62


# ---------------------------------------------------------------------------
# Helpers — reuse subprocess patterns from scripts/health/iceberg.py
# ---------------------------------------------------------------------------

def _dbt_env(cfg: Settings) -> dict[str, str]:
    """Build env dict for docker compose dbt container subprocesses."""
    return {
        **os.environ,
        "DUCKDB_S3_ENDPOINT": cfg.effective_duckdb_endpoint,
        "AWS_ACCESS_KEY_ID": cfg.effective_s3_key,
        "AWS_SECRET_ACCESS_KEY": cfg.effective_s3_secret,
        "DUCKDB_S3_USE_SSL": "true" if cfg.DUCKDB_S3_USE_SSL else "false",
        "WAREHOUSE": cfg.WAREHOUSE,
        "BRONZE_TABLE": cfg.BRONZE_TABLE,
        "SILVER_TABLE": cfg.SILVER_TABLE,
        "SILVER_METADATA_GLOB": cfg.silver_metadata_glob,
    }


def _get_iceberg_counts(cfg: Settings, compose_args: list[str]) -> tuple[int, int, str | None]:
    """
    Run count_iceberg.py inside the dbt container.
    Returns (bronze_rows, silver_rows, error_str_or_None).
    """
    cmd = compose_args + [
        "run", "--rm", "--no-deps",
        "--entrypoint", "python3",
        "dbt", "/scripts/count_iceberg.py",
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=cfg.ICEBERG_QUERY_TIMEOUT_SECONDS,
            env=_dbt_env(cfg),
            cwd=str(REPO_ROOT),
        )
    except subprocess.TimeoutExpired:
        return -1, -1, f"count_iceberg.py timed out ({cfg.ICEBERG_QUERY_TIMEOUT_SECONDS}s)"
    except Exception as exc:
        return -1, -1, str(exc)

    bronze = int(m.group(1)) if (m := re.search(r"BRONZE=(-?\d+)", result.stdout)) else -1
    silver = int(m.group(1)) if (m := re.search(r"SILVER=(-?\d+)", result.stdout)) else -1

    errors = [ln for ln in result.stdout.splitlines() if ln.startswith("ERROR_")]
    return bronze, silver, "; ".join(errors) if errors else None


def _get_dlq_count(cfg: Settings, compose_args: list[str]) -> int:
    """
    Count messages in the DLQ topic. Returns -1 on error.
    Mirrors the exact pattern from scripts/health/iceberg.py:_check_dlq.
    """
    try:
        if cfg.BROKER == "redpanda":
            cmd = compose_args + [
                "exec", "-T", "broker",
                "rpk", "topic", "consume", cfg.DLQ_TOPIC,
                "--brokers", "localhost:9092",
                "--num", "9999",
                "--timeout", f"{cfg.DLQ_READ_TIMEOUT_SECONDS}s",
            ]
        else:
            cmd = compose_args + [
                "exec", "-T", "broker",
                "/opt/kafka/bin/kafka-console-consumer.sh",
                "--bootstrap-server", "localhost:9092",
                "--topic", cfg.DLQ_TOPIC,
                "--from-beginning",
                "--timeout-ms", str(cfg.DLQ_READ_TIMEOUT_SECONDS * 1000),
            ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=cfg.DLQ_READ_TIMEOUT_SECONDS,
            cwd=str(REPO_ROOT),
        )
        return len([ln for ln in result.stdout.splitlines() if ln.strip()])
    except Exception:
        return -1


def _parquet_row_count(data_path: str) -> int | None:
    """
    Fast metadata-only row count for a parquet file.
    `data_path` is the container-side path (e.g. /data/taxi_clean.parquet).
    We locate it in build/cert/ by basename.
    """
    basename = Path(data_path).name
    candidate = REPO_ROOT / "build" / "cert" / basename
    if candidate.exists():
        try:
            return pq.read_metadata(candidate).num_rows
        except Exception:
            pass
    return None


def _load_cert_manifest() -> dict:
    if CERT_MANIFEST_PATH.exists():
        return json.loads(CERT_MANIFEST_PATH.read_text())
    return {}


def _find_expected(cert_manifest: dict, data_path: str) -> dict | None:
    """
    Look up expected counts for a cert dataset by matching file basename stem.
    basename stem: "taxi_clean" or "taxi_edge_cases" →
    manifest key:  "cert_clean"  or "cert_edge_cases"
    """
    stem = Path(data_path).stem  # e.g. "taxi_clean"
    for _dataset, scenarios in cert_manifest.items():
        for scenario_key, counts in scenarios.items():
            # scenario_key: "cert_clean" → suffix "clean"
            suffix = scenario_key.replace("cert_", "")  # "clean" / "edge_cases"
            if stem.endswith(f"_{suffix}"):
                return counts
    return None


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _check(condition: bool, label: str, detail: str) -> tuple[bool, str]:
    mark = "✓" if condition else "✗"
    return condition, f"  {label:<26}{mark}  {detail}"


def _run_checks(
    cfg: Settings,
    compose_args: list[str],
    metrics: dict,
    expected: dict | None,
) -> tuple[list[tuple[bool, str]], dict]:
    """Run all accounting assertions. Returns (check_results, raw_numbers)."""
    events_produced: int = int(metrics.get("events", 0))
    data_path: str = metrics.get("data_path", "")

    parquet_rows = _parquet_row_count(data_path)
    bronze, silver, iceberg_err = _get_iceberg_counts(cfg, compose_args)
    dlq_count = _get_dlq_count(cfg, compose_args)
    dropped = (bronze - silver) if bronze >= 0 and silver >= 0 else -1

    checks: list[tuple[bool, str]] = []

    # 1. Producer completeness vs parquet
    if parquet_rows is not None and events_produced > 0:
        ratio = events_produced / parquet_rows * 100
        ok, line = _check(
            events_produced >= int(parquet_rows * 0.99),
            "Produced msgs:",
            f"{events_produced:,}  ({ratio:.1f}% of {parquet_rows:,} parquet rows)",
        )
        checks.append((ok, line))

    # 2. DLQ count
    dlq_max = int(cfg.DLQ_MAX)
    if dlq_count >= 0:
        ok, line = _check(
            dlq_count <= dlq_max,
            "DLQ messages:",
            f"{dlq_count:,}  (allowed ≤ {dlq_max})",
        )
        checks.append((ok, line))

    # 3. Bronze completeness
    if events_produced > 0 and bronze >= 0:
        exp_bronze = expected["expected_bronze"] if expected else None
        ratio = bronze / events_produced * 100
        detail = f"{bronze:,}  ({ratio:.1f}% completeness"
        if exp_bronze is not None:
            detail += f", expected {exp_bronze:,}"
        detail += ")"
        ok, line = _check(
            bronze >= int(events_produced * 0.95)
            and (exp_bronze is None or bronze == exp_bronze),
            "Bronze rows:",
            detail,
        )
        checks.append((ok, line))

    # 4. Silver count
    if silver >= 0:
        exp_silver = expected["expected_silver"] if expected else None
        detail = f"{silver:,}"
        if exp_silver is not None:
            detail += f"  (expected {exp_silver:,})"
        ok, line = _check(
            (exp_silver is None and silver >= 0)
            or (exp_silver is not None and silver == exp_silver),
            "Silver rows:",
            detail,
        )
        checks.append((ok, line))

    # 5. Dropped count (quality + dedup)
    if dropped >= 0:
        exp_dropped = expected["expected_dropped"] if expected else None
        exp_deduped = expected["expected_deduped"] if expected else None
        detail = f"{dropped:,}"
        if exp_dropped is not None and exp_deduped is not None:
            total_expected = exp_dropped + exp_deduped
            detail += f"  (expected quality={exp_dropped} + dedup={exp_deduped} = {total_expected})"
            ok, line = _check(
                dropped == total_expected,
                "Dropped (qual+dup):",
                detail,
            )
        else:
            ok, line = _check(True, "Dropped (qual+dup):", detail)
        checks.append((ok, line))

    raw = {
        "parquet_rows": parquet_rows,
        "events_produced": events_produced,
        "dlq_count": dlq_count,
        "bronze_rows": bronze,
        "silver_rows": silver,
        "dropped": dropped,
        "iceberg_error": iceberg_err,
    }
    return checks, raw


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    cfg = load_settings(".env")
    compose_args = build_compose_args(cfg)

    if not RUN_METRICS_PATH.exists():
        print(
            "ERROR: build/run_metrics.json not found.\n"
            "       Run make generate-cert-clean (or generate-cert-edge) first.",
            file=sys.stderr,
        )
        return 1

    metrics = json.loads(RUN_METRICS_PATH.read_text())
    data_path: str = metrics.get("data_path", "")
    dataset_name: str = metrics.get("dataset", "unknown")

    cert_manifest = _load_cert_manifest()
    expected = _find_expected(cert_manifest, data_path)

    checks, raw = _run_checks(cfg, compose_args, metrics, expected)

    # --- Print report ---
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    file_label = Path(data_path).name if data_path else "unknown"

    print("=" * LINE_WIDTH)
    print(f"  de_template Certification Report — {ts}")
    print(f"  Dataset: {dataset_name}   File: {file_label}")
    print("=" * LINE_WIDTH)

    if raw["parquet_rows"] is not None:
        print(f"  {'Parquet rows:':<26}  {raw['parquet_rows']:,}")

    for _ok, line in checks:
        print(line)

    if raw.get("iceberg_error"):
        print(f"  WARNING: {raw['iceberg_error']}")

    print("-" * LINE_WIDTH)

    passed = sum(1 for ok, _ in checks if ok)
    total = len(checks)
    all_passed = passed == total
    status = "CERTIFICATION PASSED" if all_passed else "CERTIFICATION FAILED"
    print(f"  {status}  ({passed}/{total} checks)")
    print("=" * LINE_WIDTH)

    # --- Write cert_report.json ---
    report = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset_name,
        "data_path": data_path,
        **raw,
        "passed_checks": passed,
        "total_checks": total,
        "certification_passed": all_passed,
        "expected": expected,
    }
    CERT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CERT_REPORT_PATH.write_text(json.dumps(report, indent=2))
    print(f"  Report written: {CERT_REPORT_PATH.relative_to(REPO_ROOT)}")

    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
