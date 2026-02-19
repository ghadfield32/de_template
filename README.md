# de_template — Production-Grade Data Pipeline Template

A self-contained template for building **batch and real-time data pipelines** on a lakehouse stack:
**Parquet → Broker → Flink → Iceberg → dbt**

Works out of the box with **Redpanda** (default) or **Kafka**. Stores data in **MinIO** locally
or **AWS S3 / GCS / Azure** in the cloud. Ships with 16 dbt models as a working reference.
Designed to be adapted for any tabular dataset — scaffold a new dataset from a YAML manifest
in under 30 seconds.

---

## What This Template Does

```
[Parquet file]
      │
      ▼  data-generator (reads Parquet, publishes JSON rows to Kafka topic)
[Broker: Redpanda | Kafka]     ← hostname: "broker", port 9092 — same for all SQL
      │
      ├─ BATCH ──────────────────────────────────────────────────────────────────────┐
      │  Flink SQL (bounded scan: reads to end, finishes, exits)                    │
      │  make process → process-bronze → process-silver                             │
      │                                                                              │
      └─ STREAMING ──────────────────────────────────────────────────────────────────┤
         Flink SQL (unbounded stream: runs forever, checkpoints every 10s)          │
         make process-streaming                                                      │
                                                                                     │
[Iceberg bronze.raw_trips]        ← raw, unpartitioned, append-only Parquet/Snappy  │
      │                                                                              │
      ▼  Flink Silver job (batch dedup + quality filter + type casts)               │
[Iceberg silver.cleaned_trips]    ← clean, deduplicated, DATE-partitioned           │
      │                                                                              │
      ▼  DuckDB iceberg_scan() via dbt                                              │
[dbt models]                                                                         │
  staging/       → column renames + minimal filters          ◄───────────────────────┘
  intermediate/  → trip metrics, daily aggregations, payments
  marts/core/    → fct_trips, dim_locations, dim_vendors, dim_payment_types
  marts/analytics/ → mart_daily_revenue, mart_location_performance, ...
      │
      ▼  4-stage smoke test
  Stage 1: broker health + Flink REST API + MinIO/S3
  Stage 2: FINISHED/RUNNING job count + 0 restarts
  Stage 3: bronze ≥ 95% of events produced, silver ≤ bronze, DLQ ≤ DLQ_MAX
  Stage 4: dbt test (model contracts + 2 singular tests)
```

---

## Quick Start (Local — 5 Minutes to Running)

```bash
# 1. One-time setup
make setup-dev                            # install Python deps into .venv

# 2. Activate the local profile (MinIO + Redpanda)
make env-select ENV=env/local.env

# 3. Put your Parquet file in place (default: ../data/ relative to this repo)
mkdir -p ../data && cp /path/to/your_file.parquet ../data/

# 4. Start infrastructure
make up                                   # validates config, renders SQL, starts Docker

# 5. Create broker topics
make create-topics

# 6. Load data into the broker
make generate-limited                     # 10,000 events (smoke test)
# make generate                           # full dataset (all rows)

# 7. Run Flink batch jobs (Bronze → Silver)
make process                              # blocks until both jobs finish

# 8. Build dbt models
make wait-for-silver && make dbt-build

# 9. Validate
make validate                             # 4-stage smoke test
```

**Scaffold your own dataset (skip manual SQL editing):**

```bash
# Create a dataset manifest for your data
cp -r datasets/taxi datasets/orders       # start from the reference example
$EDITOR datasets/orders/dataset.yml       # describe your schema

# Generate SQL templates + dbt staging model automatically
make scaffold DATASET=orders
make build-sql
```

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Docker Desktop | ≥ 4.x | Compose v2 included |
| GNU Make | any | Windows: Git Bash or WSL |
| uv | ≥ 0.4 | `pip install uv` or `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| A Parquet file | — | Your data source; place in `../data/` relative to this repo |

`uv` is only needed for host-side developer tooling (`make setup-dev`, `make test-unit`,
`make env-select`). All pipeline containers manage their own runtimes independently.

---

## Developer Setup

```bash
# Install Python deps into .venv (pydantic-settings, pytest, ruff, pyright, jinja2, pyyaml)
make setup-dev

# Verify
uv run python -c "import pydantic; print(pydantic.__version__)"
```

If your IDE shows "Cannot import pydantic" — point it at `.venv`:
- VS Code: select the `.venv/Scripts/python.exe` interpreter
- Pyright/Pylance: `pyrightconfig.json` at the repo root is already configured

---

## Configuration: The Four Axes

Everything is controlled by **4 environment variables** in your active `.env` file.
Zero SQL changes are required when switching axes.

| Axis | Variable | Options | Default | Effect |
|------|----------|---------|---------|--------|
| Message broker | `BROKER` | `redpanda` \| `kafka` | `redpanda` | Which broker.*.yml overlay is loaded |
| Iceberg catalog | `CATALOG` | `hadoop` \| `rest` | `hadoop` | Which 00_catalog.sql template is used |
| Object storage | `STORAGE` | `minio` \| `aws_s3` \| `gcs` \| `azure` | `minio` | Which core-site.xml is rendered |
| Processing mode | `MODE` | `batch` \| `streaming_bronze` | `batch` | Used by validate Stage 2 |

### Env Profiles

Named profiles live in `env/` and are committed to git (they are templates, not secrets).
The active `.env` at the repo root is gitignored and generated by `make env-select`.

```
env/
├── local.env          ← MinIO + Redpanda (no cloud credentials)
├── local_kafka.env    ← MinIO + Kafka
├── aws.env            ← AWS S3 (IAM auth, no hardcoded keys)
├── gcs.env            ← Google Cloud Storage
└── azure.env          ← Azure ADLS Gen2
```

**Activate a profile:**

```bash
make env-select ENV=env/local.env        # local MinIO + Redpanda (default)
make env-select ENV=env/local_kafka.env  # local MinIO + Kafka
make env-select ENV=env/aws.env          # AWS S3
```

This copies the selected profile to `.env` and prints the resolved config.

**Verify your configuration before starting:**

```bash
make print-config    # show all resolved values
make debug-env       # same with [brackets] to expose trailing whitespace
make validate-config # fail if required vars are missing or axes are incompatible
```

---

## Workflow A: Batch Processing

Batch is the default mode. Flink reads the broker topic to the current end-of-partition,
processes all records, writes to Iceberg, and exits. Runs in minutes for any finite dataset.

**When to use batch:**
- Data is loaded once per day, hour, or on a schedule
- Historical backfill
- Development and smoke testing
- You need deterministic, auditable, repeatable runs

### Step 1 — Select Environment Profile

```bash
make env-select ENV=env/local.env
```

This sets `BROKER=redpanda`, `STORAGE=minio`, `CATALOG=hadoop`, `MODE=batch`.

**Decision point: which broker?**
- **Redpanda** (default): single binary, faster startup, compatible Kafka API, includes a web
  console at `localhost:8085`. Best for local development.
- **Kafka** (KRaft mode): if your production uses Kafka or you need to test Kafka-specific configs.
  Switch with `make env-select ENV=env/local_kafka.env`.

### Step 2 — Put Your Data in Place

```bash
mkdir -p ../data
cp /path/to/your/file.parquet ../data/
```

The Makefile defaults `HOST_DATA_DIR` to `../data` (one level above this repo).
Override in `.env` if your file lives elsewhere:

```ini
HOST_DATA_DIR=/absolute/path/to/your/data
DATA_PATH=/data/your_file.parquet
```

**Decision point: `DATA_PATH` vs `HOST_DATA_DIR`**
- `HOST_DATA_DIR` is the **host directory** that Docker mounts as `/data/` in the generator container.
- `DATA_PATH` is the **container-internal path** to the file (`/data/<filename>`).
- These two must agree: if `HOST_DATA_DIR=/home/user/datasets` and your file is
  `yellow_tripdata.parquet`, then `DATA_PATH=/data/yellow_tripdata.parquet`.

### Step 3 — Validate Config

```bash
make validate-config
```

This checks: required vars are set, minio credentials are present, the Parquet file exists
on the host at the expected path. **Run this before `make up`** — it catches the most common
misconfiguration (wrong `DATA_PATH` or missing credentials) before wasting time on Docker startup.

### Step 4 — Start Infrastructure

```bash
make up
```

This runs `validate-config` + `build-sql`, then starts Docker services. The services that start
depend on your axis values:

| Always | BROKER=redpanda | BROKER=kafka | STORAGE=minio |
|--------|----------------|--------------|---------------|
| Flink JobManager | Redpanda broker | Kafka broker (KRaft) | MinIO + mc-init |
| Flink TaskManager | Redpanda Console | — | — |
| dbt container | — | — | — |

After startup:

```
Flink Dashboard:    http://localhost:8081
MinIO Console:      http://localhost:9001  (minioadmin/minioadmin)
Redpanda Console:   http://localhost:8085
```

Wait 20–30 seconds for all services to be ready, then:

```bash
make health   # verifies broker, Flink REST, MinIO all respond
```

### Step 5 — Create Topics

```bash
make create-topics
```

Creates two topics:
- `taxi.raw_trips` — 3 partitions, 3-day retention (primary data)
- `taxi.raw_trips.dlq` — 1 partition, 7-day retention (Dead Letter Queue)

**Decision point: topic configuration**
Edit `.env` to change topic names:
```ini
TOPIC=your_domain.raw_events
DLQ_TOPIC=your_domain.raw_events.dlq
```
Partition count (default 3) is set in the Makefile `create-topics` target.
For production: match partition count to your expected parallelism (number of Flink task slots).

### Step 6 — Produce Data to the Broker

```bash
make generate-limited   # 10,000 events (smoke test)
make generate           # full dataset (all rows in the Parquet file)
```

**Decision point: `MAX_EVENTS`**

| Scenario | `MAX_EVENTS` | Command |
|----------|-------------|---------|
| Smoke test (fast) | `10000` | `make generate-limited` |
| Full load | `0` (all) | `make generate` |
| Custom count | `50000` | `MAX_EVENTS=50000` in `.env` + `make generate` |

`DLQ_MAX` in `.env` sets how many DLQ messages are tolerated before `make validate` fails.
Default is `0` — any malformed record sent to the DLQ causes validation to fail. Raise this
only if your data has known bad rows you choose to tolerate.

**Optional: enable schema validation (data contracts at the edge)**

By default, the generator produces every row without validation. To enforce the JSON Schema
in `schemas/taxi_trip.json` before rows enter the broker:

```ini
# In .env:
VALIDATE_SCHEMA=true
SCHEMA_PATH=/schemas/taxi_trip.json   # default — update for your schema
DLQ_MAX=0                             # fail validate if any row is rejected
```

With validation enabled:
- Each row is checked against the JSON Schema before producing.
- Invalid rows go to `DLQ_TOPIC` with an error envelope: `{error, row, timestamp}`.
- The generator prints a summary: `Events produced: N / Invalid (→ DLQ): M`.
- `make validate` Stage 3 checks that `DLQ count ≤ DLQ_MAX`.

When to enable: when your source data may have schema drift (nullable columns becoming
required, numeric fields arriving as strings, etc.). Keeps the DLQ auditable and the
broker topic clean.

### Step 7 — Process: Bronze Layer

```bash
make process-bronze
```

Flink SQL (`05_bronze_batch.sql`):
- Sets `execution.runtime-mode = 'batch'` and `table.dml-sync = 'true'`
- Creates a Kafka source table (bounded: reads to `latest-offset`, then stops)
- Creates the Iceberg `bronze.raw_trips` table (if not exists)
- INSERTs all records, parsing ISO timestamps with `TO_TIMESTAMP(col, 'yyyy-MM-dd''T''HH:mm:ss')`
- Adds `ingestion_ts = CURRENT_TIMESTAMP`

The terminal **blocks** until the INSERT finishes (this is `dml-sync=true` behavior).
Typical runtime: 30s–5min depending on data volume and host resources.

**What Bronze stores:**
- Raw field values with minimal transformation (timestamp parsing only)
- Original column names from Parquet (case-sensitive — `VendorID`, `RatecodeID`, not `vendor_id`)
- Unpartitioned, append-only, Iceberg v2 Parquet/Snappy
- No deduplication — Bronze is a faithful copy of what arrived in the broker

**Decision point: why keep Bronze raw?**
Bronze is the audit trail. If Silver logic changes (dedup key, filters, casts), you can
re-run the Silver job against the same Bronze data without re-ingesting from the broker.
Never add business logic or dedup to Bronze.

### Step 8 — Process: Silver Layer

```bash
make process-silver
```

Flink SQL (`06_silver.sql`):
- Sets `execution.runtime-mode = 'batch'`
- Creates the Iceberg `silver.cleaned_trips` table (partitioned by `pickup_date DATE`)
- Reads from `iceberg_catalog.bronze.raw_trips`
- Applies data quality filters (null timestamps, `total_amount > 0`, date range)
- Deduplicates using `ROW_NUMBER() OVER (PARTITION BY <natural_key> ORDER BY ingestion_ts DESC)`
- Renames columns to `snake_case` (e.g. `VendorID` → `vendor_id`)
- Casts monetary columns to `DECIMAL(10, 2)`
- Computes an `MD5` surrogate key (`trip_id`) from the dedup fields

**Decision points in Silver:**

**Dedup key:** Use a natural event ID if available (`ride_id`, `transaction_id`). If not, use
the minimal combination of fields that uniquely identify one real-world event. Avoid `ingestion_ts`
(changes on replay) or mutable status fields.

**Partition column:** Always use an explicit `DATE` column. Flink SQL does not support
`PARTITIONED BY (days(ts_col))` — the parser rejects transform expressions. Add an explicit
`pickup_date DATE` column, compute it in the INSERT (`CAST(ts AS DATE)`), and use
`PARTITIONED BY (pickup_date)` in the DDL.

**Date range filter:** The Silver job filters to a specific date range. Adjust this to match
your actual data. Without this filter, out-of-range test data can inflate Silver row counts.

### Step 9 — Wait for Silver

```bash
make wait-for-silver
```

Polls `iceberg_scan()` inside the dbt container until `silver.cleaned_trips` has rows,
with a 90-second timeout. This replaces `sleep N` with a real readiness gate.

### Step 10 — Build dbt Models

```bash
make dbt-build
```

Runs `dbt deps` + `dbt build --full-refresh` inside the dbt container. dbt reads Silver data
via DuckDB's `iceberg_scan()` function — no Spark or Trino required.

The 16 reference models:
```
staging/         stg_yellow_trips.sql  (passthrough + column aliases from Silver)
intermediate/    int_trip_metrics.sql, int_daily_trip_counts.sql, int_payment_stats.sql
marts/core/      fct_trips.sql, dim_locations.sql, dim_vendors.sql, dim_payment_types.sql,
                 dim_rate_codes.sql
marts/analytics/ mart_daily_revenue.sql, mart_hourly_patterns.sql,
                 mart_location_performance.sql
```

For your dataset: replace `stg_yellow_trips.sql` with `stg_<your_table>.sql` and update
`dbt/models/sources/sources.yml` to point `iceberg_scan()` at your Silver table path.

### Step 11 — Validate End-to-End

```bash
make validate
```

Runs a 4-stage smoke test:

| Stage | What it checks | Pass condition |
|-------|---------------|----------------|
| 1 — Health | Broker, Flink REST, MinIO/S3 respond | All `200 OK` |
| 2 — Jobs | Flink job states and restart counts | ≥2 `FINISHED` (batch) or ≥1 `RUNNING` (streaming), 0 restarts |
| 3 — Counts | Bronze ≥ 95% of `MAX_EVENTS`, Silver ≤ Bronze, DLQ ≤ `DLQ_MAX` | All thresholds met |
| 4 — dbt | `dbt test` passes all model contracts + singular tests | 0 failures |

**Shortcut: run all batch steps at once**

```bash
make up && make create-topics && make generate-limited && make process && make wait-for-silver && make dbt-build && make validate
```

Or use the built-in benchmark target which times the entire run:

```bash
make benchmark   # down → up → topics → generate → process → wait → dbt → validate → down
```

### Tear Down

```bash
make down     # stop services and remove volumes
make clean    # stop + remove volumes + delete build/
```

---

## Workflow B: Streaming (Real-Time Bronze)

Streaming mode keeps the Flink Bronze job running indefinitely, continuously consuming from
the broker as new events arrive. Silver is still run on-demand (batch) whenever you want a
fresh snapshot.

**When to use streaming:**
- Events arrive continuously (IoT sensors, transactions, clickstream)
- You need near-real-time visibility (< 5 minute latency to Iceberg)
- You want Flink to handle broker backpressure automatically
- Your topic retains data for hours or days (not just the current load)

**Key differences from batch:**

| Aspect | Batch (`MODE=batch`) | Streaming (`MODE=streaming_bronze`) |
|--------|---------------------|-------------------------------------|
| Flink runtime mode | `BATCH` | `STREAMING` |
| Kafka bounded mode | `latest-offset` (stops) | absent (runs forever) |
| `table.dml-sync` | `true` (terminal blocks) | absent (async submission) |
| Bronze job state | `FINISHED` | `RUNNING` |
| Iceberg writes | One commit at end of job | Commit every checkpoint (10s) |
| Silver | Run once after Bronze finishes | Run on-demand against current Bronze |
| `make validate` Stage 2 | ≥2 FINISHED | ≥1 RUNNING |

### Step 1 — Enable Streaming Mode

Edit `env/local.env` (or your active profile) and change `MODE`:

```ini
MODE=streaming_bronze
```

Then re-activate:

```bash
make env-select ENV=env/local.env
make build-sql   # re-renders 07_bronze_streaming.sql with streaming settings
```

Or use the `MODE` override directly without editing the file:

```bash
make env-select ENV=env/local.env
# Then for just this session:
MODE=streaming_bronze make process-streaming
```

### Step 2 — Start Infrastructure

Same as batch:

```bash
make up
make create-topics
```

### Step 3 — Start the Continuous Bronze Job

```bash
make process-streaming
```

This submits `07_bronze_streaming.sql` to Flink:
- Sets `execution.runtime-mode = 'streaming'` (no `dml-sync`, no `scan.bounded.mode`)
- The Kafka source reads continuously from `earliest-offset`
- The terminal **does not block** — the job is submitted async and the command returns
- The job appears as `RUNNING` in the Flink Dashboard at `http://localhost:8081`

**Verify the job is running:**
```bash
make flink-jobs   # lists all jobs with state + name
```

### Step 4 — Produce Events (Continuously or in Bursts)

```bash
# One burst:
make generate-limited      # 10,000 events

# Or repeatedly to simulate ongoing traffic:
make generate-limited      # first burst
# ... wait, send more producers, repeat
make generate              # second batch from same file
```

The streaming Bronze job picks up each new burst as events arrive — no re-submission needed.
Watch Iceberg file growth at `http://localhost:9001` (MinIO console, `warehouse/` bucket).

**Checkpoint behavior:** Flink checkpoints every 10 seconds (configured in `flink/conf/config.yaml`).
Each completed checkpoint creates a new Iceberg snapshot. This means you'll see many small files
in the Bronze table — run maintenance periodically:

```bash
make compact-silver      # merge small Silver files to 128MB target
make expire-snapshots    # remove snapshot metadata older than 7 days
```

### Step 5 — Monitor Consumer Lag

```bash
make check-lag
```

This shows the consumer group lag (how far behind Flink is reading from the broker).
In a healthy streaming pipeline, lag should be low and decreasing after a burst.

**From the Flink Dashboard:**
- Jobs → your Bronze job → Metrics → `numRecordsInPerSecond` and `numRecordsOutPerSecond`
- Jobs → your Bronze job → Checkpoints → completed count should increment, failed count = 0

### Step 6 — Run Silver on Demand

The streaming Bronze job writes continuously to `iceberg_catalog.bronze.raw_trips`.
Silver is run separately, on demand, as a bounded batch job:

```bash
make process-silver
make wait-for-silver
make dbt-build
```

Silver reads whatever is currently in Bronze (a point-in-time snapshot — Iceberg provides
snapshot isolation so there's no conflict between the running streaming job and the Silver read).

**How often to run Silver:**
- Every N minutes via a scheduler (Airflow, Dagster, cron)
- On demand before a reporting deadline
- After a burst completes

For fully-continuous Silver (streaming Bronze → streaming Silver), you would need a second
streaming job reading from the Bronze Iceberg table using the Iceberg streaming source connector.
This pattern is not included in the template by default but the foundation is in place.

### Step 7 — Validate (Streaming-Aware)

```bash
make validate
```

Stage 2 in streaming mode checks for `≥1 RUNNING` job (not `FINISHED`).
If the Bronze job has restarted due to transient S3 errors, Stage 2 warns but does not fail
(restarts are expected during network hiccups; it only fails if restarts are ongoing).

### Job Lifecycle Management

```bash
make flink-jobs                       # list all jobs: state + name
make flink-cancel JOB=<job-id>        # cancel a specific job (get ID from flink-jobs)
make flink-restart-streaming          # cancel all RUNNING + start fresh streaming Bronze
```

**Back-filling historical data alongside streaming:**
If you started streaming for new data but also have historical data:

```bash
# 1. Streaming job handles new data (already running)

# 2. Adjust date range in 06_silver.sql.tmpl for historical period
# Re-render and run Silver for historical range
make build-sql
make process-silver
```

Iceberg supports concurrent writers with snapshot isolation — the streaming Bronze job and
the batch Silver job can run simultaneously against the same table.

---

## Switching Between Batch and Streaming

```bash
# Switch to batch:
# Edit your env profile: MODE=batch
make env-select ENV=env/local.env
make flink-cancel JOB=<streaming-job-id>   # stop the streaming job first
make process                                # blocking batch run

# Switch to streaming:
# Edit your env profile: MODE=streaming_bronze
make env-select ENV=env/local.env
make process-streaming                      # start continuous job
```

---

## Adapting for Your Dataset

Replace the NYC Taxi example with your own data. The recommended path is to use the
**scaffold system**, which generates all four files from a YAML manifest.

### Option A: Scaffold from a manifest (recommended)

```bash
# 1. Create a dataset directory with a manifest
mkdir datasets/orders
$EDITOR datasets/orders/dataset.yml
```

The manifest describes your schema, topic, and data quality rules:

```yaml
# datasets/orders/dataset.yml
name: orders
topic: "${TOPIC}"
dlq_topic: "${DLQ_TOPIC}"
bronze_table: bronze.raw_orders
silver_table: silver.cleaned_orders
event_ts_col: created_at
ts_format: "yyyy-MM-dd''T''HH:mm:ss"
partition_date_col: order_date
partition_date_expr: "CAST(created_at AS DATE)"

columns:
  - name: order_id
    kafka_type: STRING
    bronze_type: STRING
    silver_name: order_id
    silver_type: STRING
    is_event_ts: false

  - name: created_at
    kafka_type: STRING
    bronze_type: TIMESTAMP(3)
    silver_name: created_at
    silver_type: "TIMESTAMP(3)"
    is_event_ts: true          # marks this as the watermark/event-time column

  - name: total_amount
    kafka_type: DOUBLE
    bronze_type: DOUBLE
    silver_name: total_amount
    silver_type: "DECIMAL(10, 2)"
    comment: "Order total in USD"

  # ... all other columns

dedup_key:
  - order_id
  - created_at

quality_filters:
  - "created_at IS NOT NULL"
  - "total_amount > 0"

surrogate_key_fields:
  - order_id
  - created_at
```

```bash
# 2. Generate SQL + dbt files
make scaffold DATASET=orders

# Output:
#   wrote  flink/sql/05_bronze_batch.sql.tmpl
#   wrote  flink/sql/06_silver.sql.tmpl
#   wrote  flink/sql/07_bronze_streaming.sql.tmpl
#   wrote  dbt/models/staging/stg_orders.sql

# 3. Render templates to build/
make build-sql

# 4. Verify the generated SQL looks correct
make show-sql
```

### Option B: Edit templates manually

If you need custom logic beyond what the scaffold generates, edit the templates directly:

| # | File | What to change |
|---|------|---------------|
| 1 | `.env` (via `env/local.env`) | `TOPIC`, `DLQ_TOPIC`, `DATA_PATH` |
| 2 | `flink/sql/05_bronze_batch.sql.tmpl` | Kafka source DDL + Bronze DDL + INSERT |
| 3 | `flink/sql/06_silver.sql.tmpl` | Silver DDL + dedup key + quality filters + INSERT |
| 4 | `dbt/models/staging/stg_<your_table>.sql` | Column aliases from Silver |

Also update `dbt/models/sources/sources.yml` to point `iceberg_scan()` at your Silver path.

### Key decisions when customizing:

**Kafka source column names:** Must match Parquet column names **exactly** (case-sensitive).
The generator serializes Parquet column names as-is into JSON keys. Do all renaming in Silver.

**Timestamp format:** The generator uses Python `datetime.isoformat()` →
`'2024-01-01T00:32:47'` (T-separated). Parse with:
```sql
TO_TIMESTAMP(col, 'yyyy-MM-dd''T''HH:mm:ss')
```
Using `'yyyy-MM-dd HH:mm:ss'` (space separator) produces all-NULL timestamps.

**Partition column:** Always use an explicit `DATE` column.
```sql
-- WRONG (Flink parser error):
PARTITIONED BY (days(event_ts))

-- RIGHT:
event_date  DATE,           -- explicit column in Silver DDL
...
PARTITIONED BY (event_date)
-- And in INSERT: CAST(event_ts AS DATE) AS event_date
```

**Dedup key:** If your source has a natural event ID (`ride_id`, `transaction_id`):
```sql
ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY ingestion_ts DESC)
```
If not, use the minimal field combination that uniquely identifies one real event.
Avoid `ingestion_ts` (changes on replay) and mutable status fields.

Full step-by-step example (ride-share dataset): [docs/03_add_new_dataset.md](docs/03_add_new_dataset.md)

---

## Cloud Storage Profiles

### AWS S3

```bash
make env-select ENV=env/aws.env
# Edit .env: set WAREHOUSE=s3a://your-bucket/warehouse/ and AWS_REGION
make up
```

`STORAGE=aws_s3` causes `make build-sql` to render `core-site.aws.xml.tmpl` which uses
`DefaultAWSCredentialsProviderChain` — no hardcoded credentials for EC2/ECS/EKS with IAM roles.

For local testing with AWS credentials:
```ini
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

### GCS / Azure

Start from `env/gcs.env` or `env/azure.env`. These profiles set the correct endpoint format.
You may need to add the appropriate Hadoop connector JARs to `docker/flink.Dockerfile`.
See [docs/06_cloud_storage.md](docs/06_cloud_storage.md) and
[docs/06_secrets_and_auth.md](docs/06_secrets_and_auth.md).

### REST Catalog (Lakekeeper)

```bash
# Edit .env: CATALOG=rest
make up EXTRA_PROFILES="--profile catalog-rest"
```

This adds the Lakekeeper REST catalog container. The Flink jobs use `00_catalog_rest.sql`
instead of `00_catalog.sql`. See [docs/01_stack_overview.md](docs/01_stack_overview.md).

---

## Observability (Prometheus + Grafana)

The observability stack is optional. Start it alongside the main pipeline:

```bash
# Start main pipeline (if not already running)
make up

# Add Prometheus + Grafana
make obs-up
```

After startup:

```
Grafana:    http://localhost:3000  (admin/admin)
Prometheus: http://localhost:9090
```

The Grafana **Flink Pipeline** dashboard opens automatically as the default home dashboard
(provisioned from `infra/grafana/dashboards/flink_pipeline.json`). It shows:

| Panel | Metric | Notes |
|-------|--------|-------|
| Running Jobs | `flink_jobmanager_numRunningJobs` | Red=0, Green≥1 |
| Last Checkpoint Duration | `flink_jobmanager_job_lastCheckpointDuration` | Yellow>5s, Red>30s |
| Failed Checkpoints | `flink_jobmanager_job_numberOfFailedCheckpoints` | Red≥1 |
| TaskManagers | `flink_jobmanager_numRegisteredTaskManagers` | Red=0, Green≥1 |
| Records In/Out per Second | `flink_taskmanager_job_task_numRecordsIn/OutPerSecond` | Timeseries |
| JVM Heap Usage | Heap used / Heap max × 100 | Yellow>70%, Red>90% |
| Checkpoint Duration History | Last duration + size | Timeseries |
| Task Slots | Total vs available | Timeseries |

**How it works:** Flink's JobManager exposes Prometheus metrics on port `9249` (configured in
`flink/conf/config.yaml`). Prometheus scrapes every 15 seconds. The Grafana datasource and
dashboard are provisioned automatically from `infra/grafana/provisioning/` — no manual
setup required.

**Customizing the dashboard:**
Edit `infra/grafana/dashboards/flink_pipeline.json` directly, or save a modified version
from the Grafana UI (dashboard → Save → overwrite the JSON file).

```bash
# Stop observability stack only (pipeline keeps running)
make obs-down

# Restart with fresh Grafana state
make obs-down && make obs-up
```

---

## Code Quality & CI

### Local pre-push checks

```bash
make fmt     # auto-format with ruff (modifies files)
make lint    # lint with ruff, auto-fix safe violations
make type    # type-check with pyright
make ci      # lint + type + test-unit (fast pre-push gate)
```

All three tools are configured in `pyproject.toml`:
- **ruff**: line-length=100, select E/F/I/UP/W, ignore E501
- **pyright**: strict import resolution via `.venv`
- **pre-commit**: ruff + standard hooks (trailing-whitespace, end-of-file-fixer, check-yaml, check-merge-conflict)

To install pre-commit hooks (optional — runs `make lint` on every `git commit`):
```bash
uv run pre-commit install
```

### GitHub Actions CI

Two workflows ship with the template:

**`.github/workflows/ci.yml`** — Runs on every push and pull request to `main`:
1. `uv run pytest tests/unit/ -v` — 40 unit tests (no Docker)
2. `uv run ruff check .` — lint
3. `uv run ruff format --check .` — format check
4. `uv run pyright` — type check
5. SQL render smoke test — copies `env/local.env` to `.env` and renders all templates

**`.github/workflows/e2e.yml`** — Runs nightly (02:00 UTC) and on `workflow_dispatch`:
1. Generates a 100-row deterministic Parquet fixture (`tests/fixtures/make_parquet.py`)
2. Starts the Docker stack (MinIO + Redpanda + Flink)
3. Produces 100 events, runs Bronze + Silver Flink jobs, runs dbt, runs `make validate`
4. Tears down (`make down`)

---

## Testing

```bash
# Fast unit tests — no Docker required, runs in < 1s
make test-unit
# or: uv run pytest tests/unit/ -v

# Full E2E integration tests — requires Docker + active .env
make test-integration
# or: uv run pytest tests/integration/ -v -m integration --timeout=600
```

### Unit test coverage

`tests/unit/test_settings.py` — 20 tests for Pydantic Settings validation:
- All 4 axis values and defaults
- Whitespace stripping (GNU make `include .env` leaves trailing whitespace)
- `STORAGE=minio` requires `S3_ENDPOINT`, `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`
- `STORAGE=aws_s3` with a MinIO endpoint raises an error
- Convenience properties (`warehouse_s3_path`, `effective_s3_key`, etc.)

`tests/unit/test_render_sql.py` — 16 tests for SQL template rendering:
- Substitution, inline comments, unsubstituted placeholders (strict failure)
- Real template smoke tests (catalog, bronze, silver)

### Integration test phases

`tests/integration/test_e2e_local.py` (requires Docker + `.env`):
- Phase 1: `env-select` correctly copies a profile
- Phase 2: settings load from `.env` without errors
- Phase 3: `make build-sql` produces the expected output files
- Phase 4 (slow, marked `@pytest.mark.slow`): full Docker stack — up → topics → generate →
  process → wait → dbt → validate → down

### CI fixture generator

`tests/fixtures/make_parquet.py` generates a 100-row, deterministic NYC Taxi Parquet file
for use in CI without committing binary data. Seed is fixed (42) so the file is identical
across runs:

```bash
uv run python tests/fixtures/make_parquet.py
# Written: tests/fixtures/taxi_fixture.parquet  (100 rows, ~12KB)
```

---

## Make Targets Reference

```bash
# ── Setup ───────────────────────────────────────────────────────────────────
make setup-dev           # uv sync — create .venv and install all deps

# ── Config ──────────────────────────────────────────────────────────────────
make env-select ENV=env/local.env  # activate an env profile
make print-config        # show all resolved env vars
make debug-env           # same with [brackets] — exposes whitespace issues
make validate-config     # fail on missing/incompatible config

# ── Lifecycle ────────────────────────────────────────────────────────────────
make up                  # validate-config + build-sql + docker compose up
make down                # stop all services, remove volumes
make clean               # down + remove build/
make restart             # down + up

# ── SQL Templates ────────────────────────────────────────────────────────────
make build-sql           # render *.sql.tmpl → build/sql/*.sql + build/conf/core-site.xml
make show-sql            # print rendered SQL + active config

# ── Dataset Scaffold ──────────────────────────────────────────────────────────
make scaffold DATASET=taxi     # generate SQL + dbt files from datasets/taxi/dataset.yml

# ── Topics ───────────────────────────────────────────────────────────────────
make create-topics       # create primary + DLQ topics (BROKER-aware)

# ── Data ─────────────────────────────────────────────────────────────────────
make generate            # produce all events (full Parquet file)
make generate-limited    # produce 10,000 events (smoke test)

# ── Processing ───────────────────────────────────────────────────────────────
make process             # batch: process-bronze + process-silver (blocking)
make process-bronze      # batch Bronze only: Kafka → Iceberg (blocking)
make process-silver      # batch Silver only: Bronze → Cleaned Iceberg (blocking)
make process-streaming   # streaming Bronze: submit continuous job (async)

# ── Monitoring ───────────────────────────────────────────────────────────────
make health              # quick health check (broker + Flink + MinIO)
make check-lag           # consumer group lag + DLQ message count
make status              # docker ps + Flink job list
make flink-jobs          # list Flink jobs with state + name
make flink-cancel JOB=<id>           # cancel a specific job
make flink-restart-streaming         # cancel all RUNNING + start fresh Bronze

# ── dbt ──────────────────────────────────────────────────────────────────────
make wait-for-silver     # poll Silver until rows > 0 (90s timeout)
make dbt-build           # dbt deps + dbt build --full-refresh
make dbt-test            # dbt test only
make dbt-docs            # dbt docs generate

# ── Validation ───────────────────────────────────────────────────────────────
make validate            # 4-stage smoke test (bash)
make validate-py         # 4-stage smoke test (Python, via uv)

# ── Code Quality ──────────────────────────────────────────────────────────────
make fmt                 # auto-format all Python files (ruff format)
make lint                # lint + auto-fix safe violations (ruff check --fix)
make type                # type-check all Python files (pyright)
make ci                  # lint + type + test-unit — fast pre-push gate

# ── Tests ────────────────────────────────────────────────────────────────────
make test                # alias for test-unit
make test-unit           # unit tests, no Docker (< 1s)
make test-integration    # E2E integration tests (requires Docker, ~5 min)

# ── Observability ────────────────────────────────────────────────────────────
make obs-up              # start Prometheus + Grafana (auto-provisioned dashboard)
make obs-down            # stop Prometheus + Grafana

# ── Iceberg Maintenance ───────────────────────────────────────────────────────
make compact-silver      # merge small files to 128MB target
make expire-snapshots    # remove snapshots older than 7 days
make vacuum              # remove orphaned data files
make maintain            # compact-silver + expire-snapshots

# ── Benchmark ────────────────────────────────────────────────────────────────
make benchmark           # full timed run: down→up→topics→generate→process→validate→down

# ── Logs ─────────────────────────────────────────────────────────────────────
make logs                # tail all service logs
make logs-broker         # tail broker logs
make logs-flink          # tail Flink JobManager logs
make logs-flink-tm       # tail Flink TaskManager logs
make ps                  # docker compose ps
```

---

## File Structure

```
de_template/
│
├── .env.example               ← example config (local MinIO defaults)
├── .gitignore                 ← .env.* denied; env/*.env committed; *.parquet denied
├── .pre-commit-config.yaml    ← ruff + standard hooks
├── Makefile                   ← all make targets
├── pyproject.toml             ← Python deps + pytest + ruff + pyright config
├── pyrightconfig.json         ← IDE import resolution (points Pyright at .venv)
├── uv.lock                    ← committed for reproducible installs
├── CHANGES.log                ← change log
│
├── .github/
│   └── workflows/
│       ├── ci.yml             ← PR gate: unit tests + lint + type + render smoke
│       └── e2e.yml            ← nightly/manual: full Docker stack E2E test
│
├── env/                       ← env profiles (committed — templates, not secrets)
│   ├── local.env              ← MinIO + Redpanda (default)
│   ├── local_kafka.env        ← MinIO + Kafka
│   ├── aws.env                ← AWS S3 (IAM auth)
│   ├── gcs.env                ← Google Cloud Storage
│   └── azure.env              ← Azure ADLS Gen2
│
├── config/
│   ├── __init__.py
│   └── settings.py            ← Pydantic Settings contract (all env vars, validated)
│                                 Settings() = pure validation (kwargs only, no env bleed)
│                                 load_settings() = reads .env + os.environ → Settings()
│
├── datasets/                  ← dataset manifests + Jinja2 scaffold templates
│   ├── taxi/
│   │   └── dataset.yml        ← reference manifest (NYC Yellow Taxi, 19 columns)
│   └── _template/
│       ├── bronze.sql.j2      ← → flink/sql/05_bronze_batch.sql.tmpl
│       ├── silver.sql.j2      ← → flink/sql/06_silver.sql.tmpl
│       ├── streaming.sql.j2   ← → flink/sql/07_bronze_streaming.sql.tmpl
│       └── stg.sql.j2         ← → dbt/models/staging/stg_<name>.sql
│
├── infra/                     ← Docker Compose overlays (assembled per axis)
│   ├── base.yml               ← Flink JM+TM + dbt + generator (always loaded)
│   ├── broker.redpanda.yml    ← Redpanda v25.3.7
│   ├── broker.kafka.yml       ← Kafka 4.0.0 (KRaft)
│   ├── storage.minio.yml      ← MinIO + mc-init (STORAGE=minio only)
│   ├── catalog.rest-lakekeeper.yml  ← Lakekeeper REST catalog (--profile catalog-rest)
│   ├── observability.optional.yml   ← Prometheus + Grafana (--profile obs)
│   ├── prometheus.yml         ← Prometheus scrape config (Flink JM :9249)
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/prometheus.yml  ← auto-provisions Prometheus datasource
│       │   └── dashboards/dashboards.yml   ← tells Grafana where to load dashboards
│       └── dashboards/
│           └── flink_pipeline.json         ← 8-panel Flink metrics dashboard
│
├── docker/
│   ├── flink.Dockerfile       ← Flink 2.0.1 + JARs (Iceberg, Kafka, S3A) + curl
│   └── dbt.Dockerfile         ← Python 3.12 + dbt-duckdb + pyarrow
│
├── flink/
│   ├── conf/
│   │   ├── config.yaml                  ← Flink 2.0 config (RocksDB, checkpoints, Prometheus)
│   │   ├── core-site.minio.xml.tmpl     ← S3A for MinIO (rendered when STORAGE=minio)
│   │   └── core-site.aws.xml.tmpl       ← S3A for AWS/cloud (rendered otherwise)
│   └── sql/                             ← SQL templates — edit these, not build/sql/
│       ├── 00_catalog.sql.tmpl          ← Iceberg Hadoop catalog init
│       ├── 00_catalog_rest.sql.tmpl     ← Iceberg REST catalog init (CATALOG=rest)
│       ├── 05_bronze_batch.sql.tmpl     ← EDIT: Kafka source DDL + Bronze DDL + INSERT
│       ├── 06_silver.sql.tmpl           ← EDIT: Silver DDL + dedup + quality filter + INSERT
│       ├── 07_bronze_streaming.sql.tmpl ← streaming Bronze (unbounded, runs forever)
│       ├── 01_source.sql.tmpl           ← (reference only)
│       └── 01_source_streaming.sql.tmpl ← (reference only)
│
├── generator/
│   ├── generator.py           ← Parquet → Kafka producer (burst/realtime/batch modes)
│   │                             opt-in schema validation: VALIDATE_SCHEMA=true
│   ├── Dockerfile
│   └── requirements.txt
│
├── schemas/
│   └── taxi_trip.json         ← JSON Schema for VALIDATE_SCHEMA=true (update for your dataset)
│
├── scripts/
│   ├── render_sql.py          ← strict template renderer (fails on unsubstituted ${VAR})
│   ├── scaffold_dataset.py    ← generates SQL + dbt files from datasets/<name>/dataset.yml
│   ├── validate.py            ← Python 4-stage smoke test (importable by integration tests)
│   ├── validate.sh            ← bash 4-stage smoke test (used by make validate)
│   ├── wait_for_iceberg.py    ← polling gate: Silver rows > 0, 90s timeout
│   ├── count_iceberg.py       ← runs inside dbt container, outputs BRONZE=N SILVER=N
│   ├── iceberg_maintenance.sh
│   └── health/
│       ├── __init__.py        ← CheckResult dataclass
│       ├── broker.py          ← broker health check
│       ├── flink.py           ← Flink REST health check
│       ├── storage.py         ← MinIO/S3 health check
│       ├── iceberg.py         ← Iceberg row count check
│       └── dbt_check.py       ← dbt test health check
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml           ← DuckDB + env_var() (no hardcoded creds)
│   ├── packages.yml           ← dbt_utils ≥1.1
│   ├── models/
│   │   ├── staging/           ← passthrough + column aliases from Silver
│   │   ├── intermediate/      ← metrics, aggregations, joins
│   │   ├── marts/core/        ← fct_trips, dim_* tables
│   │   └── marts/analytics/   ← mart_daily_revenue, mart_location_performance, ...
│   ├── macros/                ← duration_minutes, dayname_compat, test_positive_value
│   ├── seeds/                 ← 4 lookup CSVs (taxi zones, payment types, etc.)
│   └── tests/                 ← 2 singular tests
│
├── tests/
│   ├── conftest.py            ← adds project root to sys.path
│   ├── fixtures/
│   │   └── make_parquet.py    ← generates 100-row deterministic taxi fixture for CI
│   ├── unit/
│   │   ├── test_settings.py   ← 20 settings validation tests (no Docker)
│   │   └── test_render_sql.py ← 16 SQL template rendering tests (no Docker)
│   └── integration/
│       └── test_e2e_local.py  ← full E2E pipeline test (requires Docker)
│
├── build/                     ← generated by make build-sql (gitignored)
│   ├── sql/                   ← rendered .sql files (Flink mounts read-only)
│   └── conf/                  ← rendered core-site.xml (Flink mounts read-only)
│
├── data/                      ← your Parquet file(s) go here (gitignored)
│
└── docs/
    ├── 00_learning_path.md        ← linear walkthrough
    ├── 01_stack_overview.md       ← what each component does
    ├── 02_local_quickstart.md     ← 30-min start to first validate pass
    ├── 03_add_new_dataset.md      ← 4-file changeset recipe + decision guidance
    ├── 04_batch_to_streaming.md   ← batch → streaming upgrade path
    ├── 05_prod_deploy_notes.md    ← external broker, TLS, k8s Flink
    ├── 06_cloud_storage.md        ← MinIO vs S3/GCS/Azure endpoints
    ├── 06_secrets_and_auth.md     ← IAM-first patterns
    └── 07_observability.md        ← Flink checkpoints, broker lag, Iceberg file counts
```

---

## Version Stack

| Component | Version |
|-----------|---------|
| Flink | 2.0.1 (Java 17) |
| Iceberg | 1.10.1 |
| Kafka connector | 4.0.1-2.0 |
| Kafka (KRaft) | 4.0.0 |
| Redpanda | v25.3.7 |
| dbt-core | ≥1.8.0 |
| dbt-duckdb | ≥1.8.0 |
| MinIO | RELEASE.2025-04-22 |
| Python | ≥3.11 |
| pydantic-settings | ≥2.0 |
| Grafana | 10.3.1 |
| Prometheus | 2.50.1 |

---

## Production Patterns Embedded

| Pattern | Where |
|---------|-------|
| `broker:9092` abstraction — same SQL for Redpanda and Kafka | `infra/broker.*.yml` |
| STORAGE-aware `core-site.xml` rendering | `scripts/render_sql.py` + `flink/conf/` |
| Self-contained Bronze SQL (Kafka DDL inside single `-f` script) | `flink/sql/05_bronze_batch.sql.tmpl` |
| RocksDB state backend + S3 checkpoints | `flink/conf/config.yaml` |
| `classloader.check-leaked-classloader: false` (Flink + Iceberg) | `flink/conf/config.yaml` |
| Prometheus metrics on Flink JM `:9249` | `flink/conf/config.yaml` |
| CPU limits: TM 2.0, JM 1.0 | `infra/base.yml` |
| Polling gate instead of `sleep N` for Silver readiness | `scripts/wait_for_iceberg.py` |
| DLQ topic with configurable `DLQ_MAX` threshold | Makefile + `scripts/validate.sh` |
| Opt-in JSON Schema validation — invalid rows → DLQ | `generator/generator.py` |
| Flink restart-count check in validate | `scripts/validate.sh` Stage 2 |
| Iceberg snapshot metadata committed check | `scripts/validate.sh` Stage 3 |
| `allow_moved_paths=true` in `iceberg_scan` | `dbt/models/sources/sources.yml` |
| `DUCKDB_S3_ENDPOINT` without `http://` prefix | `dbt/profiles.yml` |
| `+schema` directives for staging/intermediate/marts separation | `dbt/dbt_project.yml` |
| Iceberg maintenance targets (compact, expire, vacuum) | Makefile |
| `MSYS_NO_PATHCONV=1` on all docker exec calls (Windows Git Bash) | Makefile |
| Strict SQL rendering — fails fast on unsubstituted `${VAR}` | `scripts/render_sql.py` |
| Explicit DATE column for partitioning (not transform expressions) | `flink/sql/06_silver.sql.tmpl` |
| Pydantic Settings contract — all env vars validated at startup | `config/settings.py` |
| Unit tests isolated from os.environ (settings_customise_sources) | `config/settings.py` |
| Cross-platform env activation (Python shutil, not cp) | Makefile `env-select` |
| No dangerous credential defaults (no `:-minioadmin` fallback) | `infra/base.yml` |
| `COMPOSE_PROJECT_NAME` isolation — multiple stacks on same host | Makefile |
| Jinja2 scaffold + YAML manifest — 30-second dataset onboarding | `datasets/` + `scripts/scaffold_dataset.py` |
| Grafana dashboard provisioned as code — zero manual setup | `infra/grafana/` |
| GitHub Actions CI — PR gate + nightly E2E with Parquet fixture | `.github/workflows/` |

---

## Common Gotchas

**`make env-select` — always activate before `make up`**
Forgetting this means `.env` is missing or stale. If `make up` says "TOPIC not set",
run `make env-select ENV=env/local.env` first.

**`make validate-config` — run before `make up`**
This checks that your Parquet file exists at the expected host path. A common mistake is
setting `DATA_PATH=/data/my_file.parquet` but forgetting to set `HOST_DATA_DIR` to the
directory containing `my_file.parquet`.

**Bronze silent failure → empty Silver → vacuous dbt PASS**
If the Bronze DDL has a wrong column name or type, the INSERT writes 0 rows silently.
Silver then reads 0 rows, and dbt tests PASS vacuously (0 rows = 0 null violations).
Always check `make validate` Stage 3 for actual row counts, not just dbt test status.

**Timestamp parsing NULL results**
If Silver has all-NULL timestamp columns, the Bronze SQL is using `'yyyy-MM-dd HH:mm:ss'`
(space separator) instead of `'yyyy-MM-dd''T''HH:mm:ss'` (T separator with escaped quotes).
The Python generator uses `datetime.isoformat()` which always produces the T format.

**Streaming job not processing — checkpoint failing**
Check Flink Dashboard → Jobs → your job → Checkpoints tab. If checkpoints are failing,
look at the TaskManager logs (`make logs-flink-tm`) for S3 connectivity errors. For MinIO,
verify `S3_ENDPOINT=http://minio:9000` (not `localhost` — the container hostname is `minio`).

**IDE import errors for pydantic / yaml / jinja2**
The IDE is using global Python, not `.venv`. `pyrightconfig.json` is already present — reload
the VS Code window or explicitly select the `.venv/Scripts/python.exe` interpreter.

**Scaffold comma errors in generated SQL**
The scaffold system always emits a trailing comma after every Bronze column (because
`ingestion_ts` always follows). If you add custom columns manually after scaffolding, follow
the same pattern: every column in the DDL except `ingestion_ts` gets a trailing comma.

**Parallel stacks on the same Docker host**
By default, `COMPOSE_PROJECT_NAME=de_template`. Two checkouts would collide on container
and network names. Run with a different name: `COMPOSE_PROJECT_NAME=de_v2 make up`.
