"""
scripts/health/dbt_check.py — dbt test health check.

Runs `dbt test` inside the dbt container and checks the exit code.
"""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from scripts.health import CheckResult

if TYPE_CHECKING:
    from config.settings import Settings

STAGE = "4-dbt-tests"


def run_checks(cfg: Settings, compose_args: list[str]) -> list[CheckResult]:
    return [_run_dbt_test(compose_args, cfg.DBT_TEST_TIMEOUT_SECONDS)]


def _run_dbt_test(compose_args: list[str], timeout_seconds: int) -> CheckResult:
    try:
        result = subprocess.run(
            compose_args
            + [
                "run",
                "--rm",
                "--no-deps",
                "--entrypoint",
                "/bin/sh",
                "dbt",
                "-c",
                "dbt test --profiles-dir . 2>&1",
            ],
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
        )
        passed = result.returncode == 0
        # Extract summary line from dbt output
        lines = (result.stdout + result.stderr).splitlines()
        summary = next(
            (
                ln.strip()
                for ln in reversed(lines)
                if "passed" in ln.lower() or "failed" in ln.lower()
            ),
            f"exit code {result.returncode}",
        )
        return CheckResult(
            stage=STAGE,
            name="dbt test",
            passed=passed,
            message=summary if passed else f"FAILED — {summary}",
            detail="\n".join(ln for ln in lines if "error" in ln.lower() or "fail" in ln.lower())[
                :500
            ]
            if not passed
            else None,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(STAGE, "dbt test", False, f"timed out ({timeout_seconds}s)")
    except Exception as e:
        return CheckResult(STAGE, "dbt test", False, f"error: {e}")
