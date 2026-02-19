"""
scripts/health/storage.py — Object storage health checks.

For MinIO: uses the health live endpoint (localhost:9000).
For cloud storage: skipped (auth is environment-level, not a live check here).
"""
from __future__ import annotations

import urllib.error
import urllib.request
from typing import TYPE_CHECKING

from scripts.health import CheckResult

if TYPE_CHECKING:
    from config.settings import Settings

STAGE = "1-infrastructure"


def run_checks(cfg: Settings, compose_args: list[str]) -> list[CheckResult]:  # noqa: ARG001
    if cfg.STORAGE == "minio":
        return [_check_minio()]
    # Cloud storage auth is validated at pipeline runtime, not here
    return [
        CheckResult(
            STAGE,
            f"Storage ({cfg.STORAGE})",
            True,
            "cloud storage — skipping live check (validated at runtime)",
        )
    ]


def _check_minio() -> CheckResult:
    url = "http://localhost:9000/minio/health/live"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            passed = resp.status == 200
            return CheckResult(
                STAGE,
                "MinIO",
                passed,
                "healthy" if passed else f"unexpected status {resp.status}",
            )
    except urllib.error.URLError as e:
        return CheckResult(
            STAGE,
            "MinIO",
            False,
            "not reachable at http://localhost:9000",
            detail=str(e),
        )
    except Exception as e:
        return CheckResult(STAGE, "MinIO", False, f"error: {e}")
