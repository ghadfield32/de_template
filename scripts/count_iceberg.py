#!/usr/bin/env python3
"""
count_iceberg.py — Count rows in Bronze and Silver Iceberg tables via DuckDB.

Runs INSIDE the dbt container (which has duckdb + iceberg extension installed).
Mounted at /scripts/count_iceberg.py by infra/base.yml.

Output (stdout):
    BRONZE=<n>
    SILVER=<n>

Exit code: 0 always (errors produce -1 counts so the caller can detect them).
"""
import os

import duckdb

endpoint = os.environ.get("DUCKDB_S3_ENDPOINT", "minio:9000")
key = os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin")
secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin")
use_ssl = os.environ.get("DUCKDB_S3_USE_SSL", "false").lower() == "true"

# WAREHOUSE uses s3a:// (Flink convention); DuckDB iceberg_scan needs s3://
warehouse = os.environ.get("WAREHOUSE", "s3a://warehouse/").replace("s3a://", "s3://")
BRONZE_PATH = f"{warehouse}bronze/raw_trips"
SILVER_PATH = f"{warehouse}silver/cleaned_trips"


def count(path: str) -> int:
    con = duckdb.connect()
    try:
        con.execute("INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{endpoint}';")
        con.execute(f"SET s3_access_key_id='{key}';")
        con.execute(f"SET s3_secret_access_key='{secret}';")
        con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
        con.execute("SET s3_url_style='path';")
        r = con.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{path}', allow_moved_paths=true)"
        ).fetchone()
        return r[0] if r else 0
    except Exception:
        return -1
    finally:
        con.close()


bronze = count(BRONZE_PATH)
silver = count(SILVER_PATH)
print(f"BRONZE={bronze}")
print(f"SILVER={silver}")
