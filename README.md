# de_template

A local-first data engineering template: **Parquet → Broker → Flink → Iceberg → dbt**.

Copy this repo, write a dataset manifest, run `make scaffold`, and you have a fully wired Bronze/Silver/mart pipeline. No SQL boilerplate, no hardcoded column names.

| Axis | Options |
|---|---|
| **Broker** | `redpanda` (default) · `kafka` |
| **Storage** | `minio` (default) · `aws_s3` · `gcs` · `azure` |
| **Pipeline mode** | `batch` (default) · `streaming_bronze` |
| **Generator mode** | `burst` (default) · `realtime` · `batch` |
| **Observability** | Prometheus + Grafana — on by default |
| **Orchestration** | Airflow CeleryExecutor — opt-in (`make orch-up`) |
| **Broker UI** | Redpanda Console / Kafka UI — opt-in (`make console-up`) |

---

## Contents

1. [Template Fit](#1-template-fit)
2. [Architecture](#2-architecture)
3. [Quick Start](#3-quick-start)
4. [Adding a New Dataset](#4-adding-a-new-dataset)
5. [Python Dependencies](#5-python-dependencies)
6. [Operational Playbooks](#6-operational-playbooks)
7. [Observability](#7-observability)
8. [Broker Topic UI](#8-broker-topic-ui)
9. [Orchestration — Airflow](#9-orchestration--airflow)
10. [Env Profiles](#10-env-profiles)
11. [Container Reference](#11-container-reference)
12. [Command Reference](#12-command-reference)
13. [Troubleshooting](#13-troubleshooting)
14. [Repository Map](#14-repository-map)

---

## 1. Template Fit

**Good fit when:**
- Source data is tabular and mostly append-only (events, transactions, sensor readings)
- You want a Bronze/Silver Iceberg pattern with dbt mart models downstream
- You want to start fully local and have a clear path to cloud object storage (S3/GCS/Azure)

**Not the right fit for:**
- Full CDC upserts/deletes with strict exactly-once end-to-end semantics
- Heavy nested/semi-structured transformation patterns (e.g. deeply nested JSON arrays)
- Enterprise governance requirements beyond what this starter provides

---

## 2. Architecture

```
Parquet file  (or API — see §5 data-connectors)
      │
      ▼
data-generator ──► broker (Redpanda | Kafka)
                         │
                         ▼
                   Flink JobManager / TaskManager
                         │
                    ┌────┴─────────────┐
                    ▼                  ▼
               Bronze (Iceberg)   ──► Silver (Iceberg, deduped + typed)
                                          │
                                          ▼
                                    dbt build
                                          │
                                    mart models + dbt tests
```

**Monitoring** (always-on with `make up`):

| URL | Service |
|---|---|
| `http://localhost:8081` | Flink Dashboard |
| `http://localhost:3000` | Grafana (admin/admin) |
| `http://localhost:9090` | Prometheus |
| `http://localhost:9001` | MinIO Console — local only (minioadmin/minioadmin) |

**Opt-in add-ons:**

| URL | Service | Start |
|---|---|---|
| `http://localhost:8085` | Broker topic UI (Redpanda Console or Kafka UI) | `make console-up` |
| `http://localhost:8080` | Airflow (admin/admin) | `make orch-up` |

---

## 3. Quick Start

### Prerequisites

- Docker Desktop with Compose v2
- `make`
- `uv` — `pip install uv` or [install guide](https://docs.astral.sh/uv/getting-started/installation/)
- A Parquet file for your dataset

### Step 1 — Install dev tooling

```bash
make setup-dev
```

Installs host-side Python tools (pytest, ruff, pyright) into `.venv`. Does **not** install Docker container deps — those are managed inside the image (see [§5 Python Dependencies](#5-python-dependencies)).

> **Windows + WSL users:** if you get `Access is denied` on `.venv\lib64`, run `make venv-reset`.
> This happens when the `.venv` was created in WSL and has a Linux symlink that Windows can't remove.

### Step 2 — Activate a profile

```bash
make env-select ENV=env/local.env
```

Copies `env/local.env` → `.env`. Available profiles: `local` · `local_kafka` · `aws` · `gcs` · `azure`.

### Step 3 — Place your Parquet file

Default data directory is `../data/` (one level above the repo):

```bash
mkdir -p ../data
cp /path/to/your_file.parquet ../data/
```

Override with `HOST_DATA_DIR=/absolute/path` in your `.env` if the file lives elsewhere.

### Step 4 — Run diagnostics

```bash
make doctor
```

Checks: settings schema · dataset manifests · SQL/conf rendering · data-path presence · Docker daemon · compose config. Add `DOCTOR_DBT_PARSE=true` for a deeper dbt parse check.

### Step 5 — Start infrastructure

```bash
make up
```

Starts broker + Flink + MinIO + Prometheus + Grafana. Output:

```
Flink Dashboard:    http://localhost:8081
Grafana:            http://localhost:3000  (admin/admin)
Prometheus:         http://localhost:9090
MinIO Console:      http://localhost:9001  (minioadmin/minioadmin)

Optional add-ons:
  make console-up   → Broker topic UI on http://localhost:8085
  make orch-up      → Airflow UI on http://localhost:8080  (admin/admin)
```

### Step 6 — Create topics

```bash
make create-topics
```

### Step 7 — Produce data

```bash
make generate-limited    # 10 000 events — smoke test
make generate            # all events in the Parquet file
```

### Step 8 — Process with Flink

```bash
make process    # Bronze batch → Silver batch (both in one command)
```

### Step 9 — Wait for Silver readiness

```bash
make wait-for-silver
```

Polls Iceberg Silver via DuckDB until `WAIT_FOR_SILVER_MIN_ROWS` rows appear (configurable timeout).

### Step 10 — Build dbt models

```bash
make dbt-build
```

### Step 11 — End-to-end validation

```bash
make validate
```

Runs 4 stages in sequence: infra health → Flink job state → Iceberg row counts + DLQ threshold → dbt test.

### Step 12 — Tear down

```bash
make down    # stops all services, removes volumes
```

---

## 4. Adding a New Dataset

The scaffold system generates all Flink SQL and dbt boilerplate from a single YAML manifest. You never write raw DDL or staging SQL by hand.

### Step 1 — Create the manifest

```bash
mkdir -p datasets/mydata
cp datasets/taxi/dataset.yml datasets/mydata/dataset.yml
# edit datasets/mydata/dataset.yml
```

**Manifest reference** — all fields are Pydantic-validated at scaffold time:

```yaml
manifest_version: 1
name: mydata
description: "Optional description"
topic: mydata.raw
dlq_topic: mydata.raw.dlq
bronze_table: bronze.raw_mydata
silver_table: silver.cleaned_mydata

# Timestamps
event_ts_col: event_timestamp        # must match a column name below
ts_format: "yyyy-MM-dd'T'HH:mm:ss"
partition_date_col: event_date
partition_date_expr: "DATE_FORMAT(event_timestamp, 'yyyy-MM-dd')"

# Kafka key — drives partition affinity (same entity → same partition)
# Set to a column name for ordered delivery. Leave null for round-robin.
key_field: entity_id

# Streaming mode — how late an event can arrive before being dropped
watermark_interval_seconds: 10

# Silver deduplication + surrogate key
dedup_key: [entity_id, event_timestamp]
surrogate_key_fields: [entity_id, event_timestamp]

# Silver quality gate — at least one filter required, no semicolons
quality_filters:
  - "entity_id IS NOT NULL"
  - "event_timestamp IS NOT NULL"

columns:
  - name: entity_id
    kafka_type: STRING
    bronze_type: STRING
    silver_type: VARCHAR

  - name: event_timestamp
    kafka_type: STRING
    bronze_type: TIMESTAMP(3)
    silver_type: TIMESTAMP
    is_event_ts: true             # exactly one column must have this

  - name: value
    kafka_type: DOUBLE
    bronze_type: DOUBLE
    silver_type: DOUBLE
    comment: "Optional inline docs"
```

Cross-validated automatically: `event_ts_col`, `dedup_key`, `surrogate_key_fields`, and `key_field` must all reference column names that exist in the `columns` list.

### Step 2 — Validate the manifest

```bash
make scaffold-validate DATASET=mydata
```

Fails fast with a clear Pydantic error before any files are generated.

### Step 3 — Generate SQL + dbt assets

```bash
make scaffold DATASET=mydata
# optional mart stub:
make scaffold DATASET=mydata MART_STUB=true
```

**Generated files:**

| File | Purpose |
|---|---|
| `flink/sql/05_bronze_batch.sql.tmpl` | Bronze batch DDL + INSERT (Kafka → Iceberg) |
| `flink/sql/06_silver.sql.tmpl` | Silver DDL + dedup + surrogate key INSERT |
| `flink/sql/07_bronze_streaming.sql.tmpl` | Streaming Bronze DDL (watermark-aware) |
| `dbt/models/staging/stg_mydata.sql` | dbt staging model |
| `dbt/models/staging/stg_mydata.yml` | dbt column tests |
| `dbt/models/sources/mydata_sources.yml` | dbt source definition |
| `dbt/models/marts/mart_mydata.sql` | mart stub (if `MART_STUB=true`) |

### Step 4 — Update your env profile

Edit `env/local.env` (or a copy for this dataset):

```bash
TOPIC=mydata.raw
DLQ_TOPIC=mydata.raw.dlq
BRONZE_TABLE=bronze.raw_mydata
SILVER_TABLE=silver.cleaned_mydata
DATA_PATH=/data/mydata.parquet
KEY_FIELD=entity_id
DATASET_NAME=mydata
```

Then re-activate:

```bash
make env-select ENV=env/local.env
```

### Step 5 — Render and run

```bash
make build-sql          # string.Template pass → build/sql/ + build/conf/
make show-sql           # inspect rendered SQL before touching Docker
make up
make create-topics
make generate-limited
make process
make wait-for-silver
make dbt-build
make validate
```

---

## 5. Python Dependencies

All Python dependencies for both the **host environment** and the **Docker tooling container** are declared in `pyproject.toml`. There is no separate requirements file.

### Dependency groups

```
pyproject.toml
├── [project.dependencies]       ← shared base (host + container)
│     pydantic, pydantic-settings, pyyaml, jinja2
│
├── [dependency-groups.dev]      ← host-only dev tools (NOT in Docker)
│     pytest, ruff, pyright, pyarrow (for test fixtures)
│
├── [dependency-groups.container] ← baked into docker/tooling.Dockerfile
│     pyarrow, confluent-kafka, orjson, jsonschema,
│     dbt-core, dbt-duckdb, duckdb, pandas
│
└── [dependency-groups.data-connectors]  ← your extension point
      empty by default — add API clients and DB drivers here
```

**Host sync (`make setup-dev`)** installs `[project.dependencies]` + `dev` group only.
Container builds (`docker compose build`) install `[project.dependencies]` + `container` + `data-connectors` groups.

### Adding a new pipeline dependency

```bash
# New HTTP API client (goes into tooling container):
make add-dep PKG="httpx>=0.27"

# New DB driver for data ingestion:
make add-dep GROUP=data-connectors PKG="sqlalchemy>=2.0"

# New AWS connector:
make add-dep GROUP=data-connectors PKG="boto3>=1.34"

# Host dev tool only (pytest plugin, etc.):
make add-dep GROUP=dev PKG="pytest-mock"
```

After adding any package, rebuild the tooling image:

```bash
docker compose build
# or simply:
make up    # rebuilds if pyproject.toml / uv.lock changed
```

### Pre-wired data-connector examples

Uncomment any of these in `pyproject.toml` under `[dependency-groups.data-connectors]`:

| Package | Use case |
|---|---|
| `httpx>=0.27` | Async REST API calls |
| `requests>=2.31` | Sync HTTP calls |
| `boto3>=1.34` | AWS (S3, Kinesis, DynamoDB, SQS) |
| `sqlalchemy>=2.0` | SQL databases (generic) |
| `psycopg2-binary>=2.9` | PostgreSQL |
| `pymysql>=1.1` | MySQL / MariaDB |
| `google-cloud-bigquery>=3.0` | BigQuery source reads |
| `google-cloud-storage>=2.0` | GCS (alternative to boto3) |
| `azure-storage-blob>=12.0` | Azure Blob Storage |
| `snowflake-connector-python>=3.0` | Snowflake source reads |

### Lock file

`uv.lock` is committed and covers all groups and all platforms (universal lock). The container installs Linux wheels; the host installs Windows/Mac wheels. Updating the lock file happens automatically on `uv add` or `uv lock`.

---

## 6. Operational Playbooks

### A. Batch (default)

```bash
make env-select ENV=env/local.env
make up
make create-topics
make generate-limited
make process
make wait-for-silver
make dbt-build
make validate
```

### B. Streaming Bronze

Edit your env profile to set `MODE=streaming_bronze`, then:

```bash
make env-select ENV=env/local.env
make up
make create-topics
make process-streaming          # starts long-running Bronze job (continuous)
make generate-limited           # events flow into Bronze as they're produced
make process-silver             # batch Silver job on top of accumulated Bronze
make wait-for-silver
make dbt-build
make validate
```

Streaming operational controls:

```bash
make flink-jobs                       # list running jobs + state
make flink-cancel JOB=<job-id>        # cancel a specific job
make flink-restart-streaming          # cancel all running jobs, restart Bronze
make check-lag                        # consumer group lag + DLQ status
```

| Aspect | Batch | Streaming Bronze |
|---|---|---|
| Bronze job lifecycle | Finite run | Long-running job |
| Simplicity | Higher | Lower |
| Latency | Higher | Lower |
| Best for | Backfills, CI, dev | Near-real-time ingestion |

### C. One-command benchmark

```bash
make benchmark
```

Full cycle: down → up → topics → generate-limited → process → wait-for-silver → dbt-build → down. Prints total elapsed time and saves to `benchmark_results/latest.json`.

---

## 7. Observability

Prometheus and Grafana start automatically with `make up` — no extra flags.

| URL | Service | Default creds |
|---|---|---|
| `http://localhost:3000` | Grafana | admin / admin |
| `http://localhost:9090` | Prometheus | — |

**Grafana** auto-provisions `infra/grafana/dashboards/flink_pipeline.json` as the home dashboard on first start.

**Prometheus scrape targets:**
- `flink-jobmanager:9249/` — JVM heap, checkpoint duration, records in/out, backpressure
- `broker:9644/public_metrics` — Redpanda topic throughput, consumer lag (Redpanda only; Kafka requires a JMX exporter sidecar)

```bash
make obs-up     # restart prometheus + grafana (if stopped independently)
make obs-down   # stop prometheus + grafana only
make health     # curl-checks broker, Flink, Prometheus, Grafana, MinIO
```

---

## 8. Broker Topic UI

Both broker types expose a topic browser on port 8085, gated behind `--profile console`.

```bash
make console-up     # http://localhost:8085
make console-down
```

- **Redpanda** → Redpanda Console v3 (topics, consumer groups, schema registry, message browser)
- **Kafka** → Kafka UI by Provectus (topics, consumer groups, message browser)

---

## 9. Orchestration — Airflow

CeleryExecutor stack (postgres + redis + webserver + scheduler + worker). Opt-in.

```bash
make orch-up     # starts all Airflow services → http://localhost:8080  (admin/admin)
make orch-down   # stops Airflow (pipeline services keep running)
```

**`make up` must run before `make orch-up`** so the tooling image is built before Airflow attempts to launch DockerOperator tasks.

### `de_pipeline` DAG

The pre-built DAG runs the full pipeline in sequence:

```
create_topics → generate_data → flink_bronze → flink_silver → dbt_build → validate
```

- **`create_topics`** — `BashOperator` + `docker exec` into the running broker container
- **`generate_data`** — `DockerOperator` using the tooling image (ephemeral container)
- **`flink_bronze` / `flink_silver`** — `BashOperator` + `docker exec` into Flink JobManager
- **`dbt_build`** — `DockerOperator` using the tooling image
- **`validate`** — `DockerOperator` using the tooling image

Trigger manually from the Airflow UI or:

```bash
docker exec template-airflow-scheduler airflow dags trigger de_pipeline
```

Scheduler + worker mount `/var/run/docker.sock` so they can exec into pipeline containers.

---

## 10. Env Profiles

Profiles live in `env/` and are activated with `make env-select ENV=env/<profile>.env`.
This copies the profile to `.env` — only `.env` is read at runtime.

| Profile | Broker | Storage | Notes |
|---|---|---|---|
| `env/local.env` | redpanda | minio | Default — fully local, no cloud creds needed |
| `env/local_kafka.env` | kafka | minio | Kafka parity testing, still local |
| `env/aws.env` | kafka | aws_s3 | Set real S3 bucket + IAM role or access keys |
| `env/gcs.env` | kafka | gcs | Set GCS endpoint + service account key |
| `env/azure.env` | kafka | azure | Set Azure Blob endpoint + storage account key |

**Key variables shared across all profiles:**

| Variable | What it controls |
|---|---|
| `BROKER` / `STORAGE` / `CATALOG` / `MODE` | Infrastructure axes |
| `TOPIC` / `DLQ_TOPIC` | Kafka topic names |
| `BRONZE_TABLE` / `SILVER_TABLE` | Iceberg table identifiers (`schema.table`) |
| `DATA_PATH` | Container-internal path to the Parquet file |
| `KEY_FIELD` | Column used as Kafka message key (null = round-robin) |
| `DATASET_NAME` | Used for run-metrics tagging and validation |
| `WAREHOUSE` | Iceberg warehouse path (s3a:// for Flink, s3:// for DuckDB) |
| `TOPIC_RETENTION_MS` / `DLQ_RETENTION_MS` | Topic retention (default 3d / 7d) |
| `GENERATOR_MODE` | `burst` · `realtime` · `batch` |
| `VALIDATE_SCHEMA` + `SCHEMA_PATH` | Opt-in JSON Schema validation in generator |

For REST catalog (`CATALOG=rest`), see `infra/catalog.rest-lakekeeper.yml` (advanced).

---

## 11. Container Reference

| Container | Always-on | Profile | Image | Purpose |
|---|---|---|---|---|
| `template-flink-jm` | yes | — | `docker/flink.Dockerfile` | Flink JobManager + REST API |
| `template-flink-tm` | yes | — | `docker/flink.Dockerfile` | Flink TaskManager |
| `template-redpanda` or `template-kafka` | yes | — | upstream | Message broker |
| `template-minio` | local only | — | upstream | Object storage (local) |
| `template-prometheus` | yes | — | upstream | Metrics scraping |
| `template-grafana` | yes | — | upstream | Dashboards |
| `template-dbt` | on-demand | `dbt` | `docker/tooling.Dockerfile` | dbt build / test |
| `template-generator` | on-demand | `generator` | `docker/tooling.Dockerfile` | Parquet → Kafka producer |
| `template-redpanda-console` | no | `console` | upstream | Redpanda topic UI (:8085) |
| `template-kafka-ui` | no | `console` | upstream | Kafka topic UI (:8085) |
| `template-airflow-*` | no | `orchestration` | `docker/airflow.Dockerfile` | Airflow CeleryExecutor stack (:8080) |

`template-dbt` and `template-generator` share the same `docker/tooling.Dockerfile` image. All containers share the `pipeline-net` bridge network.

---

## 12. Command Reference

**First-time setup:**

```bash
make setup-dev              # install dev tools into .venv
make venv-reset             # fix WSL/Windows .venv contamination (Access Denied on lib64)
make env-select ENV=...     # activate an env profile
make doctor                 # pre-flight checks
```

**Adding Python dependencies:**

```bash
make add-dep PKG="httpx>=0.27"                        # → container group
make add-dep GROUP=data-connectors PKG="sqlalchemy"   # → data-connectors group
make add-dep GROUP=dev PKG="pytest-mock"              # → dev group (host only)
# then rebuild:
docker compose build
```

**Lifecycle:**

```bash
make up                     # start all infra (broker + Flink + MinIO + Prometheus + Grafana)
make down                   # stop all, remove volumes
make clean                  # stop + prune + remove build/
make restart                # down + up
```

**Data pipeline:**

```bash
make create-topics
make generate               # all events
make generate-limited       # 10k events
make process                # Bronze + Silver batch
make process-bronze         # Bronze only
make process-silver         # Silver only
make process-streaming      # streaming Bronze (long-running)
make wait-for-silver        # readiness gate
make dbt-build
make dbt-test
make validate
```

**Scaffold:**

```bash
make scaffold-validate DATASET=<name>
make scaffold DATASET=<name>
make scaffold DATASET=<name> MART_STUB=true
make build-sql
make show-sql
```

**Observability and add-ons:**

```bash
make health                 # curl-checks all services
make obs-up / obs-down      # restart or stop Prometheus + Grafana
make console-up / console-down   # broker topic UI on :8085
make orch-up / orch-down    # Airflow on :8080
```

**Code quality:**

```bash
make fmt                    # ruff format
make lint                   # ruff check --fix
make type                   # pyright
make test-unit              # pytest tests/unit/ (no Docker)
make test-integration       # pytest tests/integration/ (Docker required)
make ci                     # lint + type + test-unit
```

**Iceberg maintenance:**

```bash
make compact-silver         # rewrite small files to 128 MB targets
make expire-snapshots       # drop snapshots older than 7 days
make vacuum                 # remove orphan data files
make maintain               # compact-silver + expire-snapshots
make benchmark              # full timed run
```

**Debug:**

```bash
make print-config           # resolved axis values
make debug-env              # raw values with [brackets] to expose whitespace
make validate-config        # schema check + data file presence
make logs                   # tail all services
make logs-flink             # tail Flink JobManager
make logs-broker            # tail broker
make status                 # docker compose ps + Flink job list
make flink-jobs             # Flink jobs with state
make check-lag              # consumer group lag + DLQ status
```

---

## 13. Troubleshooting

**Diagnostic order:**

```
make doctor  →  make print-config  →  make validate-config  →  make build-sql + show-sql  →  make health  →  make validate
```

**Common issues:**

| Symptom | Likely cause | Fix |
|---|---|---|
| `Access is denied` on `.venv\lib64` | `.venv` created in WSL, used on Windows | `make venv-reset` |
| `make setup-dev` fails immediately | Same WSL/Windows venv contamination | `make venv-reset` |
| Data file not found | `HOST_DATA_DIR` / `DATA_PATH` mismatch | `make debug-env` — verify `$(HOST_DATA_DIR)/$(notdir $(DATA_PATH))` exists |
| Topics not created | Broker not healthy yet | `make health`, wait, retry `make create-topics` |
| Silver never ready | Flink exception or bad storage auth | `make logs-flink`; check `WAREHOUSE`, `AWS_*` vars |
| dbt reads nothing | Silver not written or wrong endpoint | Check Silver write succeeded; check `DUCKDB_S3_ENDPOINT` |
| Generator exits immediately | Schema validation failure or empty `SCHEMA_PATH` | Set `VALIDATE_SCHEMA=false` or set `SCHEMA_PATH` and mount the schema file |
| Airflow can't find tooling image | Tooling image not built | Run `make up` before `make orch-up` |
| `docker compose build` fails on `confluent-kafka` | Missing `librdkafka-dev` | The `tooling.Dockerfile` installs it via apt — ensure the Docker build has internet access |

---

## 14. Repository Map

```
pyproject.toml              # ALL Python deps: host dev tools + container runtime
uv.lock                     # universal lockfile (all platforms, all groups)
CHANGELOG.md                # session-by-session change log
Makefile                    # operational entrypoint (all make targets)
.env                        # active config (gitignored — copied from env/)
.env.example                # annotated example

env/                        # named env profiles (committed, gitignored at root as .env)
  local.env                 # Redpanda + MinIO (default)
  local_kafka.env           # Kafka + MinIO
  aws.env                   # Kafka + AWS S3
  gcs.env                   # Kafka + GCS
  azure.env                 # Kafka + Azure Blob

config/
  settings.py               # Pydantic Settings — typed contract for all env vars

datasets/
  _template/                # Jinja2 templates for scaffold output
    bronze.sql.j2           # → flink/sql/05_bronze_batch.sql.tmpl
    silver.sql.j2           # → flink/sql/06_silver.sql.tmpl
    streaming.sql.j2        # → flink/sql/07_bronze_streaming.sql.tmpl
    stg.sql.j2              # → dbt/models/staging/stg_<name>.sql
    staging_tests.yml.j2    # → dbt/models/staging/stg_<name>.yml
    source.yml.j2           # → dbt/models/sources/<name>_sources.yml
    mart.sql.j2             # → dbt/models/marts/mart_<name>.sql (if MART_STUB=true)
  taxi/dataset.yml          # reference manifest (taxi trip data)
  registry.yml              # dataset registry

docker/
  flink.Dockerfile          # Flink image (curl + S3A plugin + Iceberg JARs)
  tooling.Dockerfile        # Unified Python image (dbt + generator + scripts)
  airflow.Dockerfile        # Airflow 2.10 + Docker CLI + providers-docker

dags/
  de_pipeline.py            # Airflow DAG: create_topics→generate→flink→dbt→validate

flink/
  sql/*.sql.tmpl            # Flink SQL templates (scaffold output — string.Template vars)
  conf/                     # Flink + Hadoop config templates

infra/
  base.yml                  # Core services: Flink JM/TM, dbt, generator
  broker.redpanda.yml       # Redpanda + Console (--profile console)
  broker.kafka.yml          # Kafka + Kafka UI (--profile console)
  storage.minio.yml         # MinIO (loaded when STORAGE=minio)
  observability.yml         # Prometheus + Grafana (always loaded)
  orchestration.airflow.yml # Airflow CeleryExecutor (--profile orchestration)
  catalog.rest-lakekeeper.yml  # REST catalog (--profile catalog-rest, advanced)
  prometheus.yml            # Prometheus scrape config
  grafana/                  # provisioning YAML + flink_pipeline.json dashboard

scripts/
  dataset_manifest.py       # Pydantic DatasetManifest model + loader
  scaffold_dataset.py       # Jinja2 template rendering → SQL + dbt files
  validate_manifest.py      # CLI wrapper for manifest validation
  render_sql.py             # string.Template render → build/sql/ + build/conf/
  validate.py               # 4-stage smoke test (runs on host via uv run)
  wait_for_iceberg.py       # Silver readiness gate (runs in dbt container)
  count_iceberg.py          # Bronze/Silver row count (runs in dbt container)
  health/                   # Composable health check modules
    broker.py · flink.py · storage.py · iceberg.py · dbt_check.py

tests/
  unit/                     # Fast, no Docker: settings, manifest, render, health
  integration/              # Docker E2E tests

build/                      # Rendered SQL + conf (gitignored, created by make build-sql)
```

---

## 15. Additional Docs

- [docs/00_learning_path.md](docs/00_learning_path.md) — recommended reading order
- [docs/01_stack_overview.md](docs/01_stack_overview.md) — deep-dive on each component
- [docs/02_local_quickstart.md](docs/02_local_quickstart.md) — expanded first-run guide
- [docs/03_add_new_dataset.md](docs/03_add_new_dataset.md) — dataset manifest reference
- [docs/04_batch_to_streaming.md](docs/04_batch_to_streaming.md) — switching pipeline modes
- [docs/05_prod_deploy_notes.md](docs/05_prod_deploy_notes.md) — production checklist
- [docs/06_secrets_and_auth.md](docs/06_secrets_and_auth.md) — IAM, Workload Identity, Key Vault
- [docs/07_observability.md](docs/07_observability.md) — Grafana + Prometheus deep-dive
