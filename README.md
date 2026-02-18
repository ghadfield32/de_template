# de_template — Production-Grade Streaming Pipeline Template

A self-contained, dual-broker pipeline template: **Parquet → Broker → Flink → Iceberg → dbt**.

Works out of the box with **Redpanda** (default) or **Kafka** via a single env var change.
Includes all 16 dbt models from the NYC Taxi reference pipeline as a working example.

---

## Quick Start (< 30 minutes)

```bash
# 1. Put your Parquet data in data/
mkdir -p data
# cp your_file.parquet data/

# 2. Configure
cp .env.example .env
# Edit .env: DATA_PATH, TOPIC, MAX_EVENTS if needed

# 3. Render SQL templates
make build-sql

# 4. Start infrastructure
make up && make health

# 5. Create topics
make create-topics

# 6. Send data
make generate-limited   # 10,000 events

# 7. Process (batch)
make process            # Bronze + Silver via Flink SQL

# 8. Wait for Silver
make wait-for-silver

# 9. Build dbt models
make dbt-build

# 10. Validate end-to-end
make validate           # Expect: 4/4 PASS
```

For a full walkthrough: [docs/00_learning_path.md](docs/00_learning_path.md)

---

## Four-Axis Configuration

Edit `.env` to change any axis. Zero SQL changes required:

| Axis | Variable | Options | Default |
|------|----------|---------|---------|
| Message broker | `BROKER` | `redpanda` \| `kafka` | `redpanda` |
| Iceberg catalog | `CATALOG` | `hadoop` \| `rest` | `hadoop` |
| Object storage | `STORAGE` | `minio` \| `aws_s3` \| `gcs` \| `azure` | `minio` |
| Processing mode | `MODE` | `batch` \| `streaming_bronze` | `batch` |

### Switch to Kafka

```bash
# Edit .env: BROKER=kafka
make down && make up && make create-topics && make generate-limited && make process && make validate
```

### Switch to AWS S3

```bash
cp .env.aws.example .env.aws
# Edit .env.aws with your bucket and region
# Edit .env: STORAGE=aws_s3 and set S3 vars
make down && make up
```

### Enable REST catalog (Lakekeeper)

```bash
# Edit .env: CATALOG=rest
make up EXTRA_PROFILES="--profile catalog-rest"
```

---

## Architecture

```
[Parquet data]
      │
      ▼ generator (burst/realtime/batch)
[Broker: Redpanda | Kafka]  (broker:9092)
      │
      ▼ Flink SQL (batch or streaming)
[Iceberg bronze.raw_trips]         ← raw, unpartitioned
      │
      ▼ Flink SQL (dedup + clean)
[Iceberg silver.cleaned_trips]     ← partitioned by pickup_date
      │
      ▼ DuckDB iceberg_scan()
[dbt: 16 models]
  staging/     → passthrough + type casts
  intermediate/→ trip metrics, daily/hourly aggregations
  marts/core/  → fct_trips, dim_dates, dim_locations, ...
  marts/analytics/ → revenue, demand, location performance
```

---

## File Structure

```
de_template/
├── .env.example              ← local MinIO defaults (safe to commit)
├── .env.aws.example          ← AWS S3 config pattern
├── .env.gcs.example          ← GCS config pattern
├── .env.azure.example        ← Azure ADLS config pattern
├── Makefile                  ← all targets
├── README.md
│
├── infra/                    ← Docker Compose overlays
│   ├── base.yml              ← Flink JM+TM + dbt + generator
│   ├── broker.redpanda.yml   ← Redpanda (hostname: broker)
│   ├── broker.kafka.yml      ← Kafka KRaft (hostname: broker)
│   ├── storage.minio.yml     ← MinIO + mc-init
│   ├── catalog.rest-lakekeeper.yml  ← REST catalog (--profile catalog-rest)
│   ├── observability.optional.yml   ← Prometheus + Grafana (--profile obs)
│   └── prometheus.yml        ← Prometheus scrape config
│
├── docker/
│   ├── flink.Dockerfile      ← Flink 2.0.1 + 7 JARs (Iceberg, Kafka, S3A)
│   └── dbt.Dockerfile        ← Python 3.12 + dbt-duckdb + pyarrow
│
├── flink/
│   ├── conf/
│   │   ├── config.yaml       ← Flink 2.0 config (RocksDB, checkpoints)
│   │   └── core-site.xml     ← Hadoop S3A config (update for cloud)
│   └── sql/                  ← SQL templates (*.sql.tmpl)
│       ├── 00_catalog.sql.tmpl
│       ├── 00_catalog_rest.sql.tmpl
│       ├── 01_source.sql.tmpl        ← batch Kafka source
│       ├── 01_source_streaming.sql.tmpl
│       ├── 05_bronze.sql.tmpl
│       ├── 06_silver.sql.tmpl
│       └── 07_streaming_bronze.sql.tmpl
│
├── generator/                ← Parquet-to-Kafka producer
│   ├── generator.py          ← burst/realtime/batch modes
│   ├── Dockerfile
│   └── requirements.txt
│
├── schemas/
│   └── taxi_trip.json        ← JSON Schema (update for your dataset)
│
├── scripts/
│   ├── render_sql.py         ← strict template rendering (fails on ${VAR})
│   ├── wait_for_iceberg.py   ← polling gate: Silver rows > 0, 90s timeout
│   ├── validate.sh           ← 4-stage smoke test
│   └── iceberg_maintenance.sh
│
├── dbt/                      ← dbt project (de_pipeline)
│   ├── dbt_project.yml
│   ├── profiles.yml          ← DuckDB + env_var() config
│   ├── packages.yml
│   ├── models/               ← 16 models (5+3+5+3)
│   ├── macros/               ← duration_minutes, dayname_compat, test_positive_value
│   ├── seeds/                ← 4 lookup CSVs
│   └── tests/                ← 2 singular tests
│
├── data/                     ← mount your Parquet file here (gitignored)
├── build/                    ← rendered SQL output (gitignored)
└── docs/                     ← documentation
    ├── 00_learning_path.md
    ├── 01_stack_overview.md
    ├── 02_local_quickstart.md
    ├── 03_add_new_dataset.md
    ├── 04_batch_to_streaming.md
    ├── 05_prod_deploy_notes.md
    ├── 06_cloud_storage.md
    ├── 06_secrets_and_auth.md
    └── 07_observability.md
```

---

## Key Make Targets

```bash
# Config
make print-config        # show all resolved env vars
make validate-config     # fail on incompatible combinations

# Lifecycle
make up                  # start infrastructure
make down                # stop + remove volumes
make health              # check all services healthy

# SQL
make build-sql           # render *.sql.tmpl → build/sql/ (strict)

# Topics
make create-topics       # BROKER-aware (rpk or kafka-topics.sh)

# Data
make generate            # full dataset
make generate-limited    # MAX_EVENTS=10000

# Processing
make process             # batch: bronze + silver
make process-streaming   # streaming: continuous bronze

# Wait
make wait-for-silver     # poll Silver until rows > 0 (90s timeout)

# dbt
make dbt-build           # dbt deps + dbt build --full-refresh
make dbt-test            # dbt test only

# Validation
make validate            # 4-stage: health + jobs + counts + dbt
make check-lag           # consumer lag + DLQ check

# Iceberg maintenance
make compact-silver      # merge small files
make expire-snapshots    # remove old snapshot metadata
make vacuum              # remove orphaned data files

# Benchmark
make benchmark           # full automated run with timing
```

---

## Version Stack

| Component | Version |
|-----------|---------|
| Flink | 2.0.1 (Java 17) |
| Iceberg | 1.10.1 |
| Kafka connector | 4.0.1-2.0 |
| Kafka (KRaft) | 4.0.0 |
| Redpanda | v24.3.1 |
| dbt-core | ≥1.8.0 |
| dbt-duckdb | ≥1.8.0 |
| DuckDB | latest |

---

## Adapting for Your Dataset

Change exactly 5 files: see [docs/03_add_new_dataset.md](docs/03_add_new_dataset.md)

## Production Deployment

See [docs/05_prod_deploy_notes.md](docs/05_prod_deploy_notes.md)

## Cloud Storage (S3/GCS/Azure)

See [docs/06_cloud_storage.md](docs/06_cloud_storage.md)

---

## Production Patterns Embedded

- RocksDB state backend + S3 checkpoints (fault-tolerant streaming)
- `classloader.check-leaked-classloader: false` (required for Flink + Iceberg)
- Prometheus metrics endpoint on Flink JM (`:9249`)
- CPU limits: TM `2.0`, JM `1.0`
- Polling gate instead of `sleep N` (`wait_for_iceberg.py`)
- DLQ topic with configurable `DLQ_MAX` threshold
- `allow_moved_paths=true` in iceberg_scan (handles Iceberg file moves)
- `DUCKDB_S3_ENDPOINT` without `http://` prefix (DuckDB httpfs requirement)
- `+schema` directives (staging, intermediate, marts schemas separated)
- Iceberg maintenance targets (compact, expire-snapshots, vacuum)
- `MSYS_NO_PATHCONV=1` on all docker exec calls (Windows Git Bash)
- Strict SQL rendering (fails fast on unsubstituted `${VAR}`)
- Explicit DATE column for Iceberg partitioning (NOT transform expressions)
- `dbt deps` before `dbt build` (auto-installs packages)
