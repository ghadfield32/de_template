"""
scripts/health/iceberg.py — Iceberg table row count and metadata checks.

Runs count_iceberg.py inside the dbt container (which has DuckDB + iceberg
extension) via docker compose run. Also checks DLQ message count.
"""

from __future__ import annotations

import os
import re
import subprocess
from typing import TYPE_CHECKING

from scripts.health import CheckResult

if TYPE_CHECKING:
    from config.settings import Settings

STAGE = "3-row-counts"


def run_checks(
    cfg: Settings,
    compose_args: list[str],
    events_produced: int = 0,
) -> list[CheckResult]:
    results = []
    results.extend(_check_table_counts(cfg, compose_args, events_produced))
    results.extend(_check_iceberg_metadata(cfg, compose_args))
    results.extend(_check_dlq(cfg, compose_args))
    return results


def _check_table_counts(
    cfg: Settings,
    compose_args: list[str],
    events_produced: int,
) -> list[CheckResult]:
    results = []

    env_override = {
        **os.environ,
        "DUCKDB_S3_ENDPOINT": cfg.effective_duckdb_endpoint,
        "AWS_ACCESS_KEY_ID": cfg.effective_s3_key,
        "AWS_SECRET_ACCESS_KEY": cfg.effective_s3_secret,
        "DUCKDB_S3_USE_SSL": "true" if cfg.DUCKDB_S3_USE_SSL else "false",
        "WAREHOUSE": cfg.WAREHOUSE,
    }

    try:
        result = subprocess.run(
            compose_args
            + [
                "run",
                "--rm",
                "--no-deps",
                "--entrypoint",
                "python3",
                "dbt",
                "/scripts/count_iceberg.py",
            ],
            capture_output=True,
            text=True,
            timeout=90,
            env=env_override,
        )

        bronze_match = re.search(r"BRONZE=(-?\d+)", result.stdout)
        silver_match = re.search(r"SILVER=(-?\d+)", result.stdout)

        bronze = int(bronze_match.group(1)) if bronze_match else -1
        silver = int(silver_match.group(1)) if silver_match else -1

    except subprocess.TimeoutExpired:
        results.append(CheckResult(STAGE, "Iceberg row count", False, "timed out (90s)"))
        return results
    except Exception as e:
        results.append(CheckResult(STAGE, "Iceberg row count", False, f"error: {e}"))
        return results

    if bronze < 0:
        results.append(
            CheckResult(
                STAGE,
                "Bronze row count",
                False,
                "could not read (DuckDB error)",
                detail=result.stderr[:300] if result.stderr else None,
            )
        )
    elif events_produced > 0:
        threshold = int(events_produced * 0.95)
        results.append(
            CheckResult(
                STAGE,
                "Bronze completeness",
                bronze >= threshold,
                f"{bronze:,} rows (>= 95% of {events_produced:,} events)"
                if bronze >= threshold
                else f"{bronze:,} rows < 95% of {events_produced:,} (threshold {threshold:,})",
            )
        )
    else:
        results.append(
            CheckResult(
                STAGE,
                "Bronze rows",
                bronze > 0,
                f"{bronze:,} rows" if bronze > 0 else "empty (0 rows)",
            )
        )

    if silver < 0:
        results.append(
            CheckResult(STAGE, "Silver row count", False, "could not read (DuckDB error)")
        )
    else:
        valid = 0 < silver <= max(bronze, 1)
        results.append(
            CheckResult(
                STAGE,
                "Silver rows",
                valid,
                f"{silver:,} rows (dedup from {bronze:,} bronze)"
                if valid
                else (
                    "empty (0 rows)"
                    if silver == 0
                    else f"{silver:,} > bronze {bronze:,} (impossible)"
                ),
            )
        )

    return results


def _check_iceberg_metadata(
    cfg: Settings,
    compose_args: list[str],
) -> list[CheckResult]:
    """Verify Silver Iceberg metadata/ directory has committed snapshot JSON."""

    env_override = {
        **os.environ,
        "DUCKDB_S3_ENDPOINT": cfg.effective_duckdb_endpoint,
        "AWS_ACCESS_KEY_ID": cfg.effective_s3_key,
        "AWS_SECRET_ACCESS_KEY": cfg.effective_s3_secret,
    }

    script = (
        "import duckdb, os\n"
        "endpoint=os.environ.get('DUCKDB_S3_ENDPOINT','minio:9000')\n"
        "key=os.environ.get('AWS_ACCESS_KEY_ID','')\n"
        "secret=os.environ.get('AWS_SECRET_ACCESS_KEY','')\n"
        "con=duckdb.connect()\n"
        "con.execute('INSTALL httpfs; LOAD httpfs;')\n"
        "con.execute(f\"SET s3_endpoint='{endpoint}';\")\n"
        "con.execute(f\"SET s3_access_key_id='{key}';\")\n"
        "con.execute(f\"SET s3_secret_access_key='{secret}';\")\n"
        "con.execute(\"SET s3_url_style='path';\")\n"
        "r=con.execute(\"SELECT count(*) FROM glob('s3://warehouse/silver/cleaned_trips/metadata/*.json')\").fetchone()\n"
        "print(r[0] if r else 0)\n"
    )

    try:
        result = subprocess.run(
            compose_args
            + ["run", "--rm", "--no-deps", "--entrypoint", "python3", "dbt", "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
            env=env_override,
        )
        count_str = result.stdout.strip().split("\n")[-1]
        count = int(count_str) if count_str.isdigit() else 0
        return [
            CheckResult(
                STAGE,
                "Iceberg metadata",
                count > 0,
                f"{count} snapshot file(s) committed"
                if count > 0
                else "no snapshot metadata found",
            )
        ]
    except Exception as e:
        return [CheckResult(STAGE, "Iceberg metadata", False, f"error: {e}")]


def _check_dlq(cfg: Settings, compose_args: list[str]) -> list[CheckResult]:
    """Check DLQ message count against DLQ_MAX threshold."""
    try:
        if cfg.BROKER == "redpanda":
            result = subprocess.run(
                compose_args
                + [
                    "exec",
                    "-T",
                    "broker",
                    "rpk",
                    "topic",
                    "consume",
                    cfg.DLQ_TOPIC,
                    "--brokers",
                    "localhost:9092",
                    "--num",
                    "9999",
                    "--timeout",
                    "3s",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            count = len([ln for ln in result.stdout.splitlines() if ln.strip()])
        else:
            result = subprocess.run(
                compose_args
                + [
                    "exec",
                    "-T",
                    "broker",
                    "/opt/kafka/bin/kafka-console-consumer.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--topic",
                    cfg.DLQ_TOPIC,
                    "--from-beginning",
                    "--timeout-ms",
                    "3000",
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            count = len([ln for ln in result.stdout.splitlines() if ln.strip()])

        passed = count <= cfg.DLQ_MAX
        return [
            CheckResult(
                STAGE,
                "DLQ messages",
                passed,
                f"{count} messages (threshold: {cfg.DLQ_MAX})"
                if passed
                else f"{count} messages exceeds DLQ_MAX={cfg.DLQ_MAX}",
            )
        ]
    except Exception as e:
        return [CheckResult(STAGE, "DLQ messages", False, f"error reading DLQ: {e}")]
