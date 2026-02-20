#!/usr/bin/env python3
"""
count_iceberg.py — Count rows in Bronze and Silver Iceberg tables via DuckDB.

Runs INSIDE the dbt container (which has duckdb + iceberg extension installed).
Mounted at /scripts/count_iceberg.py by infra/base.yml.

Output (stdout):
    BRONZE=<n>
    SILVER=<n>
    ERROR_BRONZE=<message>   (optional)
    ERROR_SILVER=<message>   (optional)

Exit code: 0 always (errors produce -1 counts so the caller can detect them).
"""

import os
import re

import duckdb


def _required_env(name: str) -> str:
    raw = os.environ.get(name, "").strip()
    if not raw:
        raise ValueError(f"{name} is required")
    return raw


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _table_to_path(warehouse_s3: str, table_name: str) -> str:
    parts = table_name.split(".", 1)
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"Invalid table name '{table_name}' (expected <schema>.<table>)")
    schema, table = parts
    return f"{warehouse_s3.rstrip('/')}/{schema}/{table}"


def _resolve_paths() -> tuple[str, str]:
    warehouse = _required_env("WAREHOUSE").replace("s3a://", "s3://")
    bronze_table = _required_env("BRONZE_TABLE")
    silver_table = _required_env("SILVER_TABLE")
    return _table_to_path(warehouse, bronze_table), _table_to_path(warehouse, silver_table)


def _configure_duckdb(con: duckdb.DuckDBPyConnection) -> None:
    endpoint = _required_env("DUCKDB_S3_ENDPOINT")
    use_ssl = _parse_bool(os.environ.get("DUCKDB_S3_USE_SSL", "false"))
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()

    con.execute("INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs;")
    con.execute(f"SET s3_endpoint='{_sql_escape(endpoint)}';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
    con.execute("SET s3_url_style='path';")
    if access_key:
        con.execute(f"SET s3_access_key_id='{_sql_escape(access_key)}';")
    if secret_key:
        con.execute(f"SET s3_secret_access_key='{_sql_escape(secret_key)}';")


def _normalize_error(exc: Exception) -> str:
    message = str(exc).strip()
    message = re.sub(r"\s+", " ", message)
    return message[:500] if message else exc.__class__.__name__


def count(path: str) -> tuple[int, str | None]:
    con = duckdb.connect()
    try:
        _configure_duckdb(con)
        r = con.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{path}', allow_moved_paths=true)"
        ).fetchone()
        return (r[0] if r else 0), None
    except Exception as exc:
        return -1, _normalize_error(exc)
    finally:
        con.close()


try:
    BRONZE_PATH, SILVER_PATH = _resolve_paths()
except Exception:
    BRONZE_PATH, SILVER_PATH = "", ""

bronze, bronze_error = count(BRONZE_PATH) if BRONZE_PATH else (-1, "missing BRONZE_PATH")
silver, silver_error = count(SILVER_PATH) if SILVER_PATH else (-1, "missing SILVER_PATH")
print(f"BRONZE={bronze}")
print(f"SILVER={silver}")
if bronze_error:
    print(f"ERROR_BRONZE={bronze_error}")
if silver_error:
    print(f"ERROR_SILVER={silver_error}")
