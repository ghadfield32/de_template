"""
scripts/health/broker.py — Broker (Redpanda / Kafka) health checks.

Calls docker compose exec to run broker-native CLI health commands.
"""
from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

from scripts.health import CheckResult

if TYPE_CHECKING:
    from config.settings import Settings

STAGE = "1-infrastructure"


def run_checks(cfg: Settings, compose_args: list[str]) -> list[CheckResult]:
    results = []

    if cfg.BROKER == "redpanda":
        results.append(_check_redpanda(compose_args))
    else:
        results.append(_check_kafka(compose_args))

    return results


def _check_redpanda(compose_args: list[str]) -> CheckResult:
    try:
        result = subprocess.run(
            compose_args + ["exec", "-T", "broker", "rpk", "cluster", "health"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        healthy = "Healthy:  true" in result.stdout or "Healthy: true" in result.stdout
        return CheckResult(
            stage=STAGE,
            name="Redpanda cluster",
            passed=healthy,
            message="healthy" if healthy else "not healthy",
            detail=result.stderr.strip() if not healthy else None,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(STAGE, "Redpanda cluster", False, "timed out (15s)")
    except Exception as e:
        return CheckResult(STAGE, "Redpanda cluster", False, f"error: {e}")


def _check_kafka(compose_args: list[str]) -> CheckResult:
    try:
        result = subprocess.run(
            compose_args
            + [
                "exec",
                "-T",
                "broker",
                "/opt/kafka/bin/kafka-topics.sh",
                "--list",
                "--bootstrap-server",
                "localhost:9092",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        passed = result.returncode == 0
        return CheckResult(
            stage=STAGE,
            name="Kafka cluster",
            passed=passed,
            message="healthy (topic list OK)" if passed else "not healthy",
            detail=result.stderr.strip() if not passed else None,
        )
    except subprocess.TimeoutExpired:
        return CheckResult(STAGE, "Kafka cluster", False, "timed out (15s)")
    except Exception as e:
        return CheckResult(STAGE, "Kafka cluster", False, f"error: {e}")
