#!/usr/bin/env python3
"""
scripts/validate.py — Python implementation of the 4-stage smoke test.

Replaces validate.sh with a structured, importable, testable version.
Each stage delegates to a composable health module in scripts/health/.

Stages:
  1. Infrastructure health  (broker + storage)
  2. Flink job state        (batch: FINISHED>=2 | streaming: RUNNING>=1)
  3. Iceberg row counts     (config-driven completeness + non-vacuous checks)
  4. dbt test               (all dbt tests pass)

Usage:
    python3 scripts/validate.py          # reads .env from repo root
    make validate-py                     # via Makefile

Importable (used by integration tests):
    from scripts.validate import run_validation, build_compose_args
    results = run_validation(cfg)
"""

from __future__ import annotations

import json
import pathlib
import sys
from datetime import UTC, datetime

# Add project root to path so health modules and config are importable
_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))
RUN_METRICS_PATH = _ROOT / "build" / "run_metrics.json"

try:
    from config.settings import Settings, load_settings
except ImportError:
    print(
        "ERROR: pydantic-settings not installed.\n"
        "Run: pip install -r requirements-dev.txt\n"
        "  or: make setup-dev"
    )
    sys.exit(1)

from scripts.health import CheckResult  # noqa: E402
from scripts.health import broker as health_broker  # noqa: E402
from scripts.health import dbt_check as health_dbt  # noqa: E402
from scripts.health import flink as health_flink  # noqa: E402
from scripts.health import iceberg as health_iceberg  # noqa: E402
from scripts.health import storage as health_storage  # noqa: E402


def build_compose_args(cfg: Settings) -> list[str]:
    """Build docker compose argument list based on config axes (mirrors Makefile logic)."""
    args = ["docker", "compose", "-f", "infra/base.yml"]
    if cfg.BROKER == "redpanda":
        args += ["-f", "infra/broker.redpanda.yml"]
    else:
        args += ["-f", "infra/broker.kafka.yml"]
    if cfg.STORAGE == "minio":
        args += ["-f", "infra/storage.minio.yml"]
    return args


def _parse_timestamp(value: str) -> datetime | None:
    """Parse ISO timestamps with either '+00:00' or trailing 'Z' UTC designator."""
    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def _validate_local_run_metrics(cfg: Settings) -> tuple[list[CheckResult], int | None]:
    """
    Validate build/run_metrics.json freshness and metadata.

    Returns:
      - a list of stage-3 CheckResult items
      - events count if metrics are trusted for completeness checks, else None
    """
    if not RUN_METRICS_PATH.exists():
        if cfg.REQUIRE_RUN_METRICS:
            return [
                CheckResult(
                    "3-row-counts",
                    "Run metrics file",
                    False,
                    "missing build/run_metrics.json (run make generate or make persist-run-metrics)",
                )
            ], None
        return [
            CheckResult(
                "3-row-counts",
                "Run metrics file",
                True,
                "missing build/run_metrics.json [allowed by REQUIRE_RUN_METRICS=false]",
            )
        ], None

    checks: list[CheckResult] = []
    try:
        with open(RUN_METRICS_PATH, encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        checks.append(
            CheckResult(
                "3-row-counts",
                "Run metrics file",
                False,
                f"could not parse {RUN_METRICS_PATH.name}",
                detail=str(exc),
            )
        )
        return checks, None

    events_raw = payload.get("events")
    if isinstance(events_raw, (int, float)):
        events = max(0, int(events_raw))
    else:
        checks.append(
            CheckResult(
                "3-row-counts",
                "Run metrics events",
                False,
                "missing/invalid numeric 'events' field",
            )
        )
        return checks, None

    allow_stale = cfg.ALLOW_STALE_RUN_METRICS
    usable = True

    produced_at_raw = payload.get("produced_at")
    produced_at = produced_at_raw if isinstance(produced_at_raw, str) else ""
    produced_dt = _parse_timestamp(produced_at) if produced_at else None
    if produced_dt is None:
        passed = allow_stale
        checks.append(
            CheckResult(
                "3-row-counts",
                "Run metrics freshness",
                passed,
                "missing/invalid produced_at timestamp"
                if not passed
                else "missing/invalid produced_at timestamp [allowed by ALLOW_STALE_RUN_METRICS=true]",
            )
        )
        usable = usable and passed
    else:
        age_minutes = (datetime.now(tz=UTC) - produced_dt.astimezone(UTC)).total_seconds() / 60.0
        is_fresh = age_minutes <= cfg.RUN_METRICS_MAX_AGE_MINUTES
        passed = is_fresh or allow_stale
        checks.append(
            CheckResult(
                "3-row-counts",
                "Run metrics freshness",
                passed,
                f"{age_minutes:.1f}m old (max {cfg.RUN_METRICS_MAX_AGE_MINUTES}m)"
                if is_fresh
                else (
                    f"{age_minutes:.1f}m old exceeds max {cfg.RUN_METRICS_MAX_AGE_MINUTES}m"
                    if not allow_stale
                    else (
                        f"{age_minutes:.1f}m old exceeds max {cfg.RUN_METRICS_MAX_AGE_MINUTES}m "
                        "[allowed by ALLOW_STALE_RUN_METRICS=true]"
                    )
                ),
            )
        )
        usable = usable and passed

    expected_dataset = cfg.expected_dataset_name

    topic_raw = payload.get("topic")
    topic = topic_raw if isinstance(topic_raw, str) else ""
    topic_match = topic == cfg.TOPIC
    topic_passed = topic_match or allow_stale
    checks.append(
        CheckResult(
            "3-row-counts",
            "Run metrics topic",
            topic_passed,
            f"topic matches active config ({cfg.TOPIC})"
            if topic_match
            else (
                f"metrics topic '{topic or '<missing>'}' != active '{cfg.TOPIC}'"
                if not allow_stale
                else (
                    f"metrics topic '{topic or '<missing>'}' != active '{cfg.TOPIC}' "
                    "[allowed by ALLOW_STALE_RUN_METRICS=true]"
                )
            ),
        )
    )
    usable = usable and topic_passed

    dataset_raw = payload.get("dataset")
    dataset = dataset_raw.strip() if isinstance(dataset_raw, str) else ""
    dataset_match = dataset == expected_dataset
    dataset_passed = dataset_match or allow_stale
    checks.append(
        CheckResult(
            "3-row-counts",
            "Run metrics dataset",
            dataset_passed,
            f"dataset matches active config ({expected_dataset})"
            if dataset_match
            else (
                f"metrics dataset '{dataset or '<missing>'}' != active '{expected_dataset}'"
                if not allow_stale
                else (
                    f"metrics dataset '{dataset or '<missing>'}' != active '{expected_dataset}' "
                    "[allowed by ALLOW_STALE_RUN_METRICS=true]"
                )
            ),
        )
    )
    usable = usable and dataset_passed

    data_path_raw = payload.get("data_path")
    metrics_data_path = data_path_raw if isinstance(data_path_raw, str) else ""
    data_path_match = metrics_data_path == cfg.DATA_PATH
    data_path_passed = data_path_match or allow_stale
    checks.append(
        CheckResult(
            "3-row-counts",
            "Run metrics data path",
            data_path_passed,
            f"data path matches active config ({cfg.DATA_PATH})"
            if data_path_match
            else (
                f"metrics data_path '{metrics_data_path or '<missing>'}' != active '{cfg.DATA_PATH}'"
                if not allow_stale
                else (
                    f"metrics data_path '{metrics_data_path or '<missing>'}' != active '{cfg.DATA_PATH}' "
                    "[allowed by ALLOW_STALE_RUN_METRICS=true]"
                )
            ),
        )
    )
    usable = usable and data_path_passed

    return checks, events if usable else None


def run_validation(cfg: Settings | None = None) -> list[CheckResult]:
    """Run all 4 stages and return a flat list of CheckResult objects."""
    if cfg is None:
        cfg = load_settings()

    compose_args = build_compose_args(cfg)
    all_results: list[CheckResult] = []

    # Stage 1: Infrastructure
    all_results.extend(health_broker.run_checks(cfg, compose_args))
    all_results.extend(health_storage.run_checks(cfg, compose_args))

    # Stage 2: Flink jobs
    all_results.extend(health_flink.run_checks(cfg, compose_args))

    # Stage 3: Row counts + run metrics contract checks
    metrics_results, events_from_local_metrics = _validate_local_run_metrics(cfg)
    all_results.extend(metrics_results)
    events_produced = events_from_local_metrics if events_from_local_metrics is not None else 0
    all_results.extend(
        health_iceberg.run_checks(
            cfg,
            compose_args,
            events_produced=events_produced,
            allow_empty=cfg.ALLOW_EMPTY,
        )
    )

    # Stage 4: dbt test
    skip_dbt = any(
        r.stage == "3-row-counts"
        and r.name == "Silver rows"
        and "allowed by ALLOW_EMPTY=true" in r.message
        for r in all_results
    )
    if skip_dbt:
        all_results.append(
            CheckResult(
                "4-dbt-tests",
                "dbt test",
                True,
                "skipped because ALLOW_EMPTY=true and Silver is empty",
            )
        )
    else:
        all_results.extend(health_dbt.run_checks(cfg, compose_args))

    return all_results


def _print_results(results: list[CheckResult], cfg: Settings) -> bool:
    """Print formatted results. Returns True if all passed."""
    broker_upper = cfg.BROKER.upper()
    mode_upper = cfg.MODE.upper()
    storage_upper = cfg.STORAGE.upper()

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("  de_template: 4-Stage Smoke Test (Python)")
    print(f"  BROKER={broker_upper}  MODE={mode_upper}  STORAGE={storage_upper}")
    print("╚══════════════════════════════════════════════════════════╝")

    current_stage = None
    STAGE_LABELS = {
        "1-infrastructure": "Stage 1: Infrastructure Health",
        "2-flink-jobs": "Stage 2: Flink Job State",
        "3-row-counts": "Stage 3: Row Counts",
        "4-dbt-tests": "Stage 4: dbt Test",
    }

    for r in results:
        if r.stage != current_stage:
            current_stage = r.stage
            label = STAGE_LABELS.get(r.stage, r.stage)
            print(f"\n━━━ {label} ━━━")
        print(r)

    passed = sum(1 for r in results if r.passed)
    total = len(results)
    all_passed = passed == total

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print(f"  Smoke Test Complete: {passed}/{total} passed")
    if all_passed:
        print("  All checks passed ✓")
    else:
        failed = total - passed
        print(f"  FAILED: {failed} check(s) — see [FAIL] lines above")
    print("╚══════════════════════════════════════════════════════════╝")

    return all_passed


if __name__ == "__main__":
    cfg = load_settings()
    results = run_validation(cfg)
    all_passed = _print_results(results, cfg)
    sys.exit(0 if all_passed else 1)
