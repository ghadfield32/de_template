"""
scripts/health/flink.py — Flink JobManager health and job state checks.

Uses the Flink REST API (localhost:8081) — no docker exec needed since
the port is published to the host.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from typing import TYPE_CHECKING

from scripts.health import CheckResult

if TYPE_CHECKING:
    from config.settings import Settings

STAGE_INFRA = "1-infrastructure"
STAGE_JOBS = "2-flink-jobs"
FLINK_URL = "http://localhost:8081"


def run_checks(cfg: Settings, compose_args: list[str]) -> list[CheckResult]:  # noqa: ARG001
    results = []
    results.append(_check_dashboard_reachable(cfg.HEALTH_HTTP_TIMEOUT_SECONDS))
    results.extend(_check_job_state(cfg.MODE, cfg.HEALTH_HTTP_TIMEOUT_SECONDS))
    return results


def _check_dashboard_reachable(timeout_seconds: int) -> CheckResult:
    try:
        with urllib.request.urlopen(f"{FLINK_URL}/overview", timeout=timeout_seconds) as resp:
            data = json.loads(resp.read())
            version = data.get("flink-version", "?")
            return CheckResult(
                STAGE_INFRA,
                "Flink Dashboard",
                True,
                f"reachable (Flink {version})",
            )
    except urllib.error.URLError as e:
        return CheckResult(
            STAGE_INFRA,
            "Flink Dashboard",
            False,
            f"not reachable at {FLINK_URL}",
            detail=str(e),
        )
    except Exception as e:
        return CheckResult(STAGE_INFRA, "Flink Dashboard", False, f"error: {e}")


def _check_job_state(mode: str, timeout_seconds: int) -> list[CheckResult]:
    results = []
    try:
        with urllib.request.urlopen(f"{FLINK_URL}/jobs/overview", timeout=timeout_seconds) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return [CheckResult(STAGE_JOBS, "Flink jobs", False, f"could not query API: {e}")]

    jobs = data.get("jobs", [])
    finished = [j for j in jobs if j.get("state") == "FINISHED"]
    running = [j for j in jobs if j.get("state") == "RUNNING"]

    if mode == "streaming_bronze":
        results.append(
            CheckResult(
                STAGE_JOBS,
                "Flink streaming jobs",
                len(running) >= 1,
                f"{len(running)} RUNNING"
                if running
                else f"expected >=1 RUNNING, found {len(running)}",
            )
        )
        # Streaming stability: zero restarts
        for job in running:
            jid = job.get("jid", "?")
            short_id = jid[:8]
            try:
                with urllib.request.urlopen(
                    f"{FLINK_URL}/jobs/{jid}", timeout=timeout_seconds
                ) as resp:
                    detail = json.loads(resp.read())
                restarts = detail.get("numRestarts", 0)
                results.append(
                    CheckResult(
                        STAGE_JOBS,
                        f"Job {short_id}... stability",
                        restarts == 0,
                        f"{restarts} restart(s)" if restarts > 0 else "0 restarts (stable)",
                    )
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    CheckResult(
                        STAGE_JOBS,
                        f"Job {short_id}... stability",
                        False,
                        f"could not read restart metrics: {exc}",
                    )
                )
    else:
        results.append(
            CheckResult(
                STAGE_JOBS,
                "Flink batch jobs",
                len(finished) >= 2,
                f"{len(finished)} FINISHED (bronze + silver)"
                if len(finished) >= 2
                else f"expected >=2 FINISHED, found {len(finished)}",
            )
        )

    return results
