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
    allow_empty: bool = False,
) -> list[CheckResult]:
    results = []
    table_results = _check_table_counts(cfg, compose_args, events_produced, allow_empty)
    results.extend(table_results)

    skip_metadata_for_empty = any(
        r.name == "Silver rows" and "allowed by ALLOW_EMPTY=true" in r.message
        for r in table_results
    )
    skip_metadata_for_non_empty = any(
        r.name == "Silver rows" and r.passed and "dedup from" in r.message
        for r in table_results
    )

    if skip_metadata_for_empty:
        results.append(
            CheckResult(
                STAGE,
                "Iceberg metadata",
                True,
                "skipped because ALLOW_EMPTY=true and Silver is empty",
            )
        )
    elif skip_metadata_for_non_empty:
        results.append(
            CheckResult(
                STAGE,
                "Iceberg metadata",
                True,
                "skipped because Silver table readability is already proven by row-count checks",
            )
        )
    else:
        results.extend(_check_iceberg_metadata(cfg, compose_args))

    results.extend(_check_dlq(cfg, compose_args))
    return results


def _dbt_env(cfg: Settings) -> dict[str, str]:
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


def _check_table_counts(
    cfg: Settings,
    compose_args: list[str],
    events_produced: int,
    allow_empty: bool,
) -> list[CheckResult]:
    results = []

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
            timeout=cfg.ICEBERG_QUERY_TIMEOUT_SECONDS,
            env=_dbt_env(cfg),
        )

        bronze_match = re.search(r"BRONZE=(-?\d+)", result.stdout)
        silver_match = re.search(r"SILVER=(-?\d+)", result.stdout)
        bronze_error_match = re.search(r"ERROR_BRONZE=(.+)", result.stdout)
        silver_error_match = re.search(r"ERROR_SILVER=(.+)", result.stdout)
        bronze = int(bronze_match.group(1)) if bronze_match else -1
        silver = int(silver_match.group(1)) if silver_match else -1
        bronze_error = bronze_error_match.group(1).strip() if bronze_error_match else None
        silver_error = silver_error_match.group(1).strip() if silver_error_match else None
    except subprocess.TimeoutExpired:
        results.append(
            CheckResult(
                STAGE,
                "Iceberg row count",
                False,
                f"timed out ({cfg.ICEBERG_QUERY_TIMEOUT_SECONDS}s)",
            )
        )
        return results
    except Exception as exc:  # noqa: BLE001
        results.append(CheckResult(STAGE, "Iceberg row count", False, f"error: {exc}"))
        return results

    if bronze < 0:
        results.append(
            CheckResult(
                STAGE,
                "Bronze row count",
                False,
                "could not read (DuckDB error)",
                detail=bronze_error or (result.stderr[:300] if result.stderr else None),
            )
        )
    elif bronze == 0:
        results.append(
            CheckResult(
                STAGE,
                "Bronze rows",
                allow_empty,
                "empty (0 rows) [allowed by ALLOW_EMPTY=true]" if allow_empty else "empty (0 rows)",
            )
        )
    elif events_produced > 0:
        threshold = int(events_produced * cfg.BRONZE_COMPLETENESS_RATIO)
        pct = cfg.bronze_completeness_pct
        results.append(
            CheckResult(
                STAGE,
                "Bronze completeness",
                bronze >= threshold,
                f"{bronze:,} rows (>= {pct:.1f}% of {events_produced:,} events)"
                if bronze >= threshold
                else (
                    f"{bronze:,} rows < {pct:.1f}% of {events_produced:,} (threshold {threshold:,})"
                ),
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
            CheckResult(
                STAGE,
                "Silver row count",
                False,
                "could not read (DuckDB error)",
                detail=silver_error,
            )
        )
    elif silver == 0 and allow_empty:
        results.append(
            CheckResult(
                STAGE,
                "Silver rows",
                True,
                "empty (0 rows) [allowed by ALLOW_EMPTY=true]",
            )
        )
    elif silver < cfg.WAIT_FOR_SILVER_MIN_ROWS:
        results.append(
            CheckResult(
                STAGE,
                "Silver rows",
                False,
                f"{silver:,} rows < WAIT_FOR_SILVER_MIN_ROWS={cfg.WAIT_FOR_SILVER_MIN_ROWS}",
            )
        )
    else:
        valid = silver <= max(bronze, 1)
        results.append(
            CheckResult(
                STAGE,
                "Silver rows",
                valid,
                f"{silver:,} rows (dedup from {bronze:,} bronze)"
                if valid
                else f"{silver:,} > bronze {bronze:,} (impossible)",
            )
        )

    return results


def _check_iceberg_metadata(
    cfg: Settings,
    compose_args: list[str],
) -> list[CheckResult]:
    """Verify Silver Iceberg metadata/ directory has committed snapshot JSON."""

    script = (
        "import duckdb, os\n"
        "def req(name):\n"
        "    value = os.environ.get(name, '').strip()\n"
        "    if not value:\n"
        "        raise ValueError(f'{name} is required')\n"
        "    return value\n"
        "def esc(value):\n"
        '    return value.replace("\'", "\'\'")\n'
        "endpoint = req('DUCKDB_S3_ENDPOINT')\n"
        "glob_path = req('SILVER_METADATA_GLOB')\n"
        "access_key = os.environ.get('AWS_ACCESS_KEY_ID', '').strip()\n"
        "secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '').strip()\n"
        "use_ssl = os.environ.get('DUCKDB_S3_USE_SSL', 'false').lower() in "
        "('1','true','yes','on')\n"
        "con = duckdb.connect()\n"
        "try:\n"
        "    con.execute('INSTALL httpfs; LOAD httpfs;')\n"
        "    con.execute(f\"SET s3_endpoint='{esc(endpoint)}';\")\n"
        "    con.execute(f\"SET s3_use_ssl={'true' if use_ssl else 'false'};\")\n"
        "    con.execute(\"SET s3_url_style='path';\")\n"
        "    if access_key:\n"
        "        con.execute(f\"SET s3_access_key_id='{esc(access_key)}';\")\n"
        "    if secret_key:\n"
        "        con.execute(f\"SET s3_secret_access_key='{esc(secret_key)}';\")\n"
        "    r = con.execute(f\"SELECT count(*) FROM glob('{glob_path}')\").fetchone()\n"
        "    print(r[0] if r else 0)\n"
        "finally:\n"
        "    con.close()\n"
    )

    try:
        result = subprocess.run(
            compose_args
            + ["run", "--rm", "--no-deps", "--entrypoint", "python3", "dbt", "-c", script],
            capture_output=True,
            text=True,
            timeout=cfg.ICEBERG_METADATA_TIMEOUT_SECONDS,
            env=_dbt_env(cfg),
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
    except Exception as exc:  # noqa: BLE001
        return [CheckResult(STAGE, "Iceberg metadata", False, f"error: {exc}")]


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
                    f"{cfg.DLQ_READ_TIMEOUT_SECONDS}s",
                ],
                capture_output=True,
                text=True,
                timeout=cfg.DLQ_READ_TIMEOUT_SECONDS,
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
                    str(cfg.DLQ_READ_TIMEOUT_SECONDS * 1000),
                ],
                capture_output=True,
                text=True,
                timeout=cfg.DLQ_READ_TIMEOUT_SECONDS,
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
    except Exception as exc:  # noqa: BLE001
        return [CheckResult(STAGE, "DLQ messages", False, f"error reading DLQ: {exc}")]
