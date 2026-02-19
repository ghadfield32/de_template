#!/usr/bin/env python3
"""
scripts/validate.py — Python implementation of the 4-stage smoke test.

Replaces validate.sh with a structured, importable, testable version.
Each stage delegates to a composable health module in scripts/health/.

Stages:
  1. Infrastructure health  (broker + storage)
  2. Flink job state        (batch: FINISHED>=2 | streaming: RUNNING>=1)
  3. Iceberg row counts     (bronze >= 95% of produced, silver > 0)
  4. dbt test               (all dbt tests pass)

Usage:
    python3 scripts/validate.py          # reads .env from repo root
    make validate-py                     # via Makefile

Importable (used by integration tests):
    from scripts.validate import run_validation, build_compose_args
    results = run_validation(cfg)
"""

from __future__ import annotations

import pathlib
import sys

# Add project root to path so health modules and config are importable
_ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(_ROOT))

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


def _read_events_produced(cfg: Settings, compose_args: list[str]) -> int:
    """Read events_produced from generator metrics volume via dbt container."""
    script = (
        "import json, sys\n"
        "try:\n"
        "    data = json.load(open('/metrics/latest.json'))\n"
        "    print(data.get('events', 0))\n"
        "except Exception:\n"
        "    print(0)\n"
    )
    try:
        import subprocess

        result = subprocess.run(
            compose_args
            + ["run", "--rm", "--no-deps", "--entrypoint", "python3", "dbt", "-c", script],
            capture_output=True,
            text=True,
            timeout=15,
        )
        lines = result.stdout.strip().splitlines()
        return int(lines[-1]) if lines and lines[-1].isdigit() else 0
    except Exception:
        return 0


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

    # Stage 3: Row counts (needs events_produced from generator metrics)
    events_produced = _read_events_produced(cfg, compose_args)
    all_results.extend(health_iceberg.run_checks(cfg, compose_args, events_produced))

    # Stage 4: dbt test
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
