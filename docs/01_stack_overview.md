# Stack Overview

This doc explains what each component does in the default template flow.

## End-to-End Flow

```text
Parquet file
  -> data-generator
  -> broker (Redpanda or Kafka)
  -> Flink Bronze SQL (batch or streaming)
  -> Iceberg bronze table
  -> Flink Silver SQL (batch)
  -> Iceberg silver table
  -> dbt (DuckDB + iceberg_scan)
  -> staging/intermediate/marts
```

## Core Components

### Broker (`redpanda` or `kafka`)

- Internal address for Flink/generator: `broker:9092`
- Local default profile uses Redpanda + console at `http://localhost:8085`

Typical failures:
- Topic missing -> run `make create-topics`
- Producer succeeds but Flink sees nothing -> verify topic names and mode

### Flink

- Runs SQL templates rendered to `build/sql/`
- Bronze templates:
  - `05_bronze_batch.sql` (batch)
  - `07_bronze_streaming.sql` (streaming)
- Silver template:
  - `06_silver.sql`
- Dashboard: `http://localhost:8081`

Typical failures:
- Job failures due to storage auth/path mismatches
- Bronze writes succeed but Silver empty due to filter/dedup logic

### Iceberg Tables

- Bronze: raw append-oriented layer
- Silver: cleaned/deduplicated layer
- Warehouse location controlled by `WAREHOUSE`

Typical failures:
- Table path mismatch between warehouse and dbt source config
- Metadata/snapshot issues when storage credentials are wrong

### dbt (DuckDB adapter)

- Reads Iceberg via `iceberg_scan`
- Runs models/tests from `dbt/`
- Triggered via `make dbt-build` / `make dbt-test`

Typical failures:
- Empty or unreadable Silver source
- Wrong storage endpoint/auth settings for DuckDB

### Validation Pipeline

`make validate` runs four stages:
1. Infra health
2. Flink job state
3. Iceberg counts + DLQ threshold
4. dbt tests

## Optional Layers

### Observability (Prometheus + Grafana)

```bash
make obs-up
```

### REST Catalog (Lakekeeper)

Advanced/manual path; combine compose files directly when needed.

```bash
docker compose \
  -f infra/base.yml \
  -f infra/broker.redpanda.yml \
  -f infra/storage.minio.yml \
  -f infra/catalog.rest-lakekeeper.yml \
  --profile catalog-rest up -d
```
