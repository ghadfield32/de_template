# Local Quickstart

Local path: Redpanda + MinIO + Batch mode.

## Prerequisites

- Docker Desktop (Compose v2)
- `make`
- `uv`
- Parquet input file

## Step-by-Step

```bash
make setup-dev
make env-select ENV=env/local.env
mkdir -p ../data
cp /path/to/your_file.parquet ../data/
make doctor
make up
make create-topics
make generate-limited
make process
make wait-for-silver
make dbt-build
make validate
```

## What to Expect

- Flink UI: `http://localhost:8081`
- MinIO console: `http://localhost:9001`
- Redpanda console: `http://localhost:8085`

## Most Useful Checks

```bash
make print-config
make show-sql
make health
make check-lag
make validate
```

## Clean Up

```bash
make down
# or:
make clean
```

## Common Problems

- Data file not found:
  - Confirm `HOST_DATA_DIR` and `DATA_PATH` match.
- Silver readiness timeout:
  - Check Flink job errors and storage auth.
  - Tune `WAIT_FOR_SILVER_MIN_ROWS`, `WAIT_FOR_SILVER_TIMEOUT_SECONDS`, `WAIT_FOR_SILVER_POLL_SECONDS`.
- Empty run behavior:
  - `ALLOW_EMPTY=true` only bypasses if Silver is queryable and remains at `0` rows.
