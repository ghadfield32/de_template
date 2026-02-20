#!/usr/bin/env python3
"""
wait_for_iceberg.py — Polling gate for Silver Iceberg table readiness.

Runs inside the dbt container (has duckdb Python package via dbt-duckdb).
Polls configured Silver Iceberg table via iceberg_scan() until rows reach the
WAIT_FOR_SILVER_MIN_ROWS threshold (default 1) or configured timeout.

Replaces fixed sleep-based gating. Called by: make wait-for-silver
"""

from __future__ import annotations

import os
import sys
import time

try:
    import duckdb
except ImportError:  # pragma: no cover - dbt container always has duckdb installed
    duckdb = None

ALLOWED_TRUE_VALUES = {"1", "true", "yes", "on"}


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in ALLOWED_TRUE_VALUES


def _parse_positive_int(name: str) -> int:
    raw = _required_env(name)
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got '{raw}'") from exc
    if value < 1:
        raise ValueError(f"{name} must be >= 1")
    return value


def _parse_non_negative_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got '{raw}'") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _table_to_path(warehouse: str, table_name: str) -> str:
    parts = table_name.split(".", 1)
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"SILVER_TABLE must be '<schema>.<table>', got '{table_name}'")
    schema, table = parts
    return f"{warehouse.rstrip('/')}/{schema}/{table}"


def _allow_empty_bypass_reason(
    allow_empty: bool,
    successful_queries: int,
    saw_zero_row_success: bool,
    saw_non_zero_row_success: bool,
    timeout_seconds: int,
    min_rows: int,
) -> tuple[bool, str | None]:
    if not allow_empty:
        return False, None

    if successful_queries == 0:
        return (
            False,
            f"\nTimeout after {timeout_seconds}s. "
            "ALLOW_EMPTY=true cannot bypass because Silver was never queryable "
            "(all polls returned query errors).",
        )

    if saw_non_zero_row_success:
        return (
            False,
            f"\nTimeout after {timeout_seconds}s. "
            "ALLOW_EMPTY=true only bypasses intentional zero-row runs; "
            f"observed non-zero rows below threshold {min_rows}.",
        )

    if saw_zero_row_success:
        return (
            True,
            f"\nTimeout after {timeout_seconds}s with stable zero-row reads. "
            "ALLOW_EMPTY=true bypassing readiness gate.",
        )

    return False, None


def check_silver_count(
    silver_path: str,
    endpoint: str,
    use_ssl: bool,
    access_key: str,
    secret_key: str,
) -> tuple[int, str | None]:
    if duckdb is None:
        return -1, "duckdb module is not installed"

    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{_sql_escape(endpoint)}';")
        if access_key:
            con.execute(f"SET s3_access_key_id='{_sql_escape(access_key)}';")
        if secret_key:
            con.execute(f"SET s3_secret_access_key='{_sql_escape(secret_key)}';")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
        con.execute("SET s3_url_style='path';")
        result = con.execute(
            f"SELECT COUNT(*) AS n FROM iceberg_scan('{silver_path}', allow_moved_paths=true)"
        ).fetchone()
        return (result[0] if result else 0), None
    except Exception as exc:  # noqa: BLE001
        return -1, str(exc)
    finally:
        con.close()


def main() -> int:
    warehouse = _required_env("WAREHOUSE").replace("s3a://", "s3://")
    silver_table = _required_env("SILVER_TABLE")
    silver_path = _table_to_path(warehouse, silver_table)

    endpoint = _required_env("DUCKDB_S3_ENDPOINT")
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    use_ssl = _parse_bool(os.environ.get("DUCKDB_S3_USE_SSL", "false"))
    allow_empty = _parse_bool(os.environ.get("ALLOW_EMPTY", "false"))

    timeout_seconds = _parse_positive_int("WAIT_FOR_SILVER_TIMEOUT_SECONDS")
    poll_interval = _parse_positive_int("WAIT_FOR_SILVER_POLL_SECONDS")
    min_rows = _parse_non_negative_int("WAIT_FOR_SILVER_MIN_ROWS", default=1)
    if poll_interval >= timeout_seconds:
        raise ValueError(
            "WAIT_FOR_SILVER_POLL_SECONDS must be less than WAIT_FOR_SILVER_TIMEOUT_SECONDS"
        )

    deadline = time.time() + timeout_seconds
    print(f"Waiting for Silver table: {silver_path}")
    print(
        f"  S3 endpoint: {endpoint}  |  timeout: {timeout_seconds}s"
        f"  |  min rows: {min_rows}"
    )

    successful_queries = 0
    saw_zero_row_success = False
    saw_non_zero_row_success = False
    last_error: str | None = None
    last_success_count: int | None = None

    while time.time() < deadline:
        count, error = check_silver_count(
            silver_path=silver_path,
            endpoint=endpoint,
            use_ssl=use_ssl,
            access_key=access_key,
            secret_key=secret_key,
        )
        remaining = int(deadline - time.time())
        if error:
            last_error = error
            print(f"  waiting... ({remaining}s remaining) [query not ready: {error[:140]}]")
        else:
            successful_queries += 1
            last_success_count = count
            if count == 0:
                saw_zero_row_success = True
            if count > 0:
                saw_non_zero_row_success = True
            if count >= min_rows:
                print(
                    "  Silver table ready: "
                    f"{count:,} rows (threshold {min_rows}, {timeout_seconds - remaining}s elapsed)"
                )
                return 0
            print(
                f"  waiting... ({remaining}s remaining) "
                f"[rows={count:,} < threshold {min_rows}]"
            )
        time.sleep(poll_interval)

    bypass, reason = _allow_empty_bypass_reason(
        allow_empty=allow_empty,
        successful_queries=successful_queries,
        saw_zero_row_success=saw_zero_row_success,
        saw_non_zero_row_success=saw_non_zero_row_success,
        timeout_seconds=timeout_seconds,
        min_rows=min_rows,
    )
    if reason:
        print(reason)
    if bypass:
        return 0

    print(f"\nTIMEOUT: Silver table did not populate within {timeout_seconds}s")
    if last_success_count is not None:
        print(f"  Last successful Silver count: {last_success_count:,} rows")
    if last_error:
        print(f"  Last query error: {last_error[:240]}")
    print("  Check Flink Dashboard -> Jobs -> Exceptions for errors.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
