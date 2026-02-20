# de_template

A local-first data engineering template for **Parquet → Broker → Flink → Iceberg → dbt**.

This repo is designed to be copied and adapted for new datasets with minimal boilerplate.
It supports:

- **Broker:** `redpanda` or `kafka`
- **Storage:** `minio`, `aws_s3`, `gcs`, `azure`
- **Pipeline mode:** `batch` or `streaming_bronze`
- **Generator mode:** `burst`, `realtime`, `batch`
- **Observability:** Prometheus + Grafana (on by default)
- **Orchestration:** Airflow CeleryExecutor (opt-in via `make orch-up`)

---

## 1. Template Fit

Use this template when:
- Your source data is tabular and mostly append-only.
- You want a Bronze/Silver pattern on Iceberg plus dbt downstream models.
- You want to start locally and keep a migration path to cloud object storage.

Not a good fit as-is when:
- You need full CDC upserts/deletes and strict end-to-end exactly-once serving semantics.
- You require deep nested/semi-structured transformation patterns by default.
- You need heavy governance controls beyond this starter framework.

---

## 2. Architecture

```
Parquet file
    │
    ▼
data-generator ──► broker (Redpanda or Kafka) ──► Flink JM/TM ──► Iceberg (Bronze → Silver)
                                                                         │
                                                           dbt ◄─────────┘
                                                             │
                                                    mart models / dbt tests
```

**Monitoring** (always-on):
- Prometheus `http://localhost:9090` — scrapes Flink JM + Redpanda metrics
- Grafana `http://localhost:3000` — pre-built Flink pipeline dashboard

**Orchestration** (opt-in):
- Airflow CeleryExecutor `http://localhost:8080` — DAG for the full pipeline

**Broker UI** (opt-in):
- `http://localhost:8085` — Redpanda Console (Redpanda) or Kafka UI (Kafka)

---

## 3. Choose Your Path

| Decision | Option | Strengths | Trade-offs | Typical situation |
|---|---|---|---|---|
| Broker | `redpanda` | Fast local startup, simple single-binary | Not a 1:1 Kafka distro runtime | Local dev and smoke tests |
| Broker | `kafka` | Closer to production Kafka deployments | Slightly more operational overhead locally | Kafka-in-prod parity |
| Pipeline `MODE` | `batch` | Deterministic, easy reruns/backfills | Higher latency | Daily/hourly loads, CI/E2E, dev |
| Pipeline `MODE` | `streaming_bronze` | Low-latency continuous Bronze ingestion | More operational complexity | Near-real-time ingestion |
| Storage | `minio` | Fully local, no cloud creds required | Not real cloud IAM/network behavior | First run and local iteration |
| Storage | `aws_s3`/`gcs`/`azure` | Cloud-realistic path | Requires real cloud auth | Environment parity before deploy |
| Generator `GENERATOR_MODE` | `burst` | Fastest throughput | Less realistic cadence | Smoke tests and load tests |
| Generator `GENERATOR_MODE` | `realtime` | Temporal realism | Slower test cycles | Prod-like replay |
| Generator `GENERATOR_MODE` | `batch` | Controlled chunks | More tuning knobs | Ingestion experiments |

---

## 4. Quick Start (Local)

### Prerequisites

- Docker Desktop with Compose v2
- `make`
- `uv` (`pip install uv`)
- A Parquet file for your dataset

### Step 1 — Install Python tooling

```bash
make setup-dev
```

### Step 2 — Activate a profile

```bash
make env-select ENV=env/local.env
```

This copies `env/local.env` to `.env`. Available profiles: `local`, `local_kafka`, `aws`, `gcs`, `azure`.

### Step 3 — Place your Parquet file

By default the host data dir is `../data` (one level above repo root):

```bash
mkdir -p ../data
cp /path/to/your_file.parquet ../data/
```

### Step 4 — Run diagnostics

```bash
make doctor
```

Checks settings, manifest validation, SQL rendering, data-path presence, Docker daemon, and compose config. Run `DOCTOR_DBT_PARSE=true make doctor` for a deeper dbt parse check.

### Step 5 — Start infrastructure

```bash
make up
```

Starts broker + Flink + MinIO + **Prometheus + Grafana** (all default-on).

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
make generate-limited    # 10k events (smoke test)
# or:
make generate            # all events
```

### Step 8 — Process with Flink

```bash
make process             # Bronze batch + Silver batch
```

### Step 9 — Wait for Silver readiness

```bash
make wait-for-silver
```

Polls Iceberg Silver until `WAIT_FOR_SILVER_MIN_ROWS` is met (timeout controlled by `WAIT_FOR_SILVER_TIMEOUT_SECONDS`).

### Step 10 — Build dbt models

```bash
make dbt-build
```

### Step 11 — Validate end-to-end

```bash
make validate
```

Runs 4 stages: infra health → Flink job state → Iceberg row counts + DLQ → dbt test.

### Step 12 — Tear down

```bash
make down
```

---

## 5. Adding a New Dataset

This is the primary way to adapt the template for your use case.

### Step 1 — Create the manifest

```bash
mkdir -p datasets/mydata
cp datasets/taxi/dataset.yml datasets/mydata/dataset.yml
```

Edit `datasets/mydata/dataset.yml`. Key fields:

```yaml
manifest_version: 1
name: mydata
topic: mydata.raw
dlq_topic: mydata.raw.dlq
bronze_table: bronze.raw_mydata
silver_table: silver.cleaned_mydata
event_ts_col: event_timestamp      # column name — must match a column below
ts_format: "yyyy-MM-dd'T'HH:mm:ss"
partition_date_col: event_date
partition_date_expr: "DATE_FORMAT(event_timestamp, 'yyyy-MM-dd')"
key_field: entity_id               # Kafka partition affinity; null = round-robin
watermark_interval_seconds: 10     # late-event tolerance in streaming mode
dedup_key: [entity_id, event_timestamp]
surrogate_key_fields: [entity_id, event_timestamp]
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
    is_event_ts: true
  - name: value
    kafka_type: DOUBLE
    bronze_type: DOUBLE
    silver_type: DOUBLE
```

All column names, `event_ts_col`, `dedup_key`, `surrogate_key_fields`, and `key_field` are cross-validated by Pydantic at scaffold time.

### Step 2 — Validate the manifest

```bash
make scaffold-validate DATASET=mydata
```

Fails fast with a clear error if any field is wrong before generating files.

### Step 3 — Generate SQL + dbt assets

```bash
make scaffold DATASET=mydata
# optionally add a mart stub:
make scaffold DATASET=mydata MART_STUB=true
```

This generates:
- `flink/sql/05_bronze_batch.sql.tmpl` — Bronze batch DDL + INSERT
- `flink/sql/06_silver.sql.tmpl` — Silver DDL + dedup/surrogate key INSERT
- `flink/sql/07_bronze_streaming.sql.tmpl` — Streaming Bronze DDL
- `dbt/models/staging/stg_mydata.sql` — dbt staging model
- `dbt/models/staging/stg_mydata.yml` — dbt staging tests
- `dbt/models/sources/mydata_sources.yml` — dbt source definition
- (optional) `dbt/models/marts/mart_mydata.sql` — mart stub

### Step 4 — Update your env profile

Edit your `.env` or an `env/` profile to point at the new dataset:

```bash
TOPIC=mydata.raw
DLQ_TOPIC=mydata.raw.dlq
BRONZE_TABLE=bronze.raw_mydata
SILVER_TABLE=silver.cleaned_mydata
DATA_PATH=/data/mydata.parquet
KEY_FIELD=entity_id             # match key_field in dataset.yml
DATASET_NAME=mydata
```

Then:

```bash
make env-select ENV=env/local.env   # re-activate to reload .env
```

### Step 5 — Render SQL and run

```bash
make build-sql
make show-sql    # inspect rendered SQL before running
make up
make create-topics
make generate-limited
make process
make wait-for-silver
make dbt-build
make validate
```

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

Set `MODE=streaming_bronze` in your env profile first.

```bash
make env-select ENV=env/local.env  # edit to set MODE=streaming_bronze first
make up
make create-topics
make process-streaming             # starts long-running Bronze job
make generate-limited              # events flow continuously into Bronze
make process-silver                # batch Silver job on top of Bronze
make wait-for-silver
make dbt-build
make validate
```

Streaming controls:

```bash
make flink-jobs
make flink-cancel JOB=<job-id>
make flink-restart-streaming
make check-lag
```

| Aspect | Batch | Streaming Bronze |
|---|---|---|
| Bronze job lifecycle | Finite run | Long-running job |
| Simplicity | Higher | Lower |
| Latency | Higher | Lower |
| Best for | Backfills, CI | Near-real-time ingestion |

### C. Airflow-Orchestrated (opt-in)

Start the Airflow stack after `make up`:

```bash
make orch-up
```

Opens `http://localhost:8080` (admin/admin). The `de_pipeline` DAG runs:
`create_topics → generate_data → flink_bronze → flink_silver → dbt_build → validate`

Trigger manually from the UI or:

```bash
docker exec template-airflow-scheduler airflow dags trigger de_pipeline
```

Stop when done:

```bash
make orch-down
```

---

## 7. Observability

Prometheus and Grafana start automatically with `make up`. No extra flags needed.

| URL | Service | Credentials |
|---|---|---|
| `http://localhost:3000` | Grafana (Flink pipeline dashboard) | admin/admin |
| `http://localhost:9090` | Prometheus | — |

Grafana auto-provisions `infra/grafana/dashboards/flink_pipeline.json` as the home dashboard.

Prometheus scrapes:
- **Flink JobManager** — `:9249/` (JVM, checkpoint, throughput metrics)
- **Redpanda broker** — `:9644/public_metrics` (topic lag, throughput; Redpanda only)

To restart observability without restarting the full stack:

```bash
make obs-up     # restart prometheus + grafana
make obs-down   # stop prometheus + grafana
```

---

## 8. Broker Topic UI (opt-in)

Both broker types expose a web UI on port 8085 under `--profile console`.

```bash
make console-up     # start topic UI: http://localhost:8085
make console-down   # stop topic UI
```

- **Redpanda** → Redpanda Console (topic browse, consumer group lag, schema registry)
- **Kafka** → Kafka UI by Provectus (topic browse, consumer group lag)

---

## 9. Profiles and Environments

Profiles live in `env/` and are activated via `make env-select ENV=...`.

| Profile | Broker | Storage | Notes |
|---|---|---|---|
| `env/local.env` | redpanda | minio | Default — fully local, no cloud creds |
| `env/local_kafka.env` | kafka | minio | Kafka parity testing |
| `env/aws.env` | kafka | aws_s3 | Set real bucket + IAM |
| `env/gcs.env` | kafka | gcs | Set endpoint + SA key |
| `env/azure.env` | kafka | azure | Set endpoint + storage key |

For REST catalog (`CATALOG=rest`), see `infra/catalog.rest-lakekeeper.yml` (advanced).

---

## 10. Container Reference

| Container | Always-on | Profile | Purpose |
|---|---|---|---|
| `template-flink-jm` | yes | — | Flink JobManager |
| `template-flink-tm` | yes | — | Flink TaskManager |
| `template-redpanda` or `template-kafka` | yes | — | Message broker |
| `template-minio` | yes (local) | — | Object storage (local only) |
| `template-prometheus` | yes | — | Metrics scraping |
| `template-grafana` | yes | — | Dashboards |
| `template-dbt` | on-demand | `dbt` | dbt models (run via `make dbt-*`) |
| `template-generator` | on-demand | `generator` | Data producer (run via `make generate`) |
| `template-redpanda-console` | no | `console` | Redpanda topic UI (:8085) |
| `template-kafka-ui` | no | `console` | Kafka topic UI (:8085) |
| `template-airflow-*` | no | `orchestration` | Airflow CeleryExecutor stack (:8080) |

All containers share the `pipeline-net` bridge network. The single `docker/tooling.Dockerfile` image is used for both `dbt` and `data-generator` services.

---

## 11. High-Value Commands

**Config and diagnostics:**

```bash
make print-config
make debug-env
make validate-config
make doctor
```

**Data and processing:**

```bash
make create-topics
make generate
make generate-limited
make process
make process-bronze
make process-silver
make process-streaming
```

**Validation and dbt:**

```bash
make wait-for-silver
make dbt-build
make dbt-test
make validate
```

**Observability and broker UI:**

```bash
make health           # checks broker, Flink, Prometheus, Grafana, MinIO
make obs-up           # restart Grafana + Prometheus
make obs-down         # stop Grafana + Prometheus
make console-up       # start broker topic UI on :8085
make console-down     # stop broker topic UI
```

**Orchestration:**

```bash
make orch-up          # start Airflow on :8080
make orch-down        # stop Airflow
```

**Code quality:**

```bash
make fmt
make lint
make type
make test-unit
make test-integration
make ci
```

**Iceberg maintenance:**

```bash
make compact-silver
make expire-snapshots
make vacuum
make benchmark
```

---

## 12. Troubleshooting

Use this order:

1. `make doctor`
2. `make print-config`
3. `make validate-config`
4. `make build-sql` then `make show-sql`
5. `make health`
6. `make validate`

Common issues:

| Symptom | Likely cause | Fix |
|---|---|---|
| Data file not found | `HOST_DATA_DIR` / `DATA_PATH` mismatch | Check `make debug-env`; verify file exists at `$(HOST_DATA_DIR)/$(notdir $(DATA_PATH))` |
| Silver never ready | Flink job exception or bad storage auth | Check `make logs-flink`; check `WAREHOUSE`, `AWS_*` vars |
| dbt reads nothing | Silver table path/config wrong or Silver write failed | Verify Silver write succeeded; check `DUCKDB_S3_ENDPOINT` |
| Airflow can't find tooling image | Image not built | Run `make up` before `make orch-up` to build all images |
| `VALIDATE_SCHEMA=true` fails | `SCHEMA_PATH` empty | Set `SCHEMA_PATH=/schemas/yourfile.json` and mount schema file |

---

## 13. Repository Map

```
env/                    # named env profiles (local, aws, gcs, azure)
config/
  settings.py           # Pydantic config contract (all env vars typed here)
datasets/
  _template/            # Jinja2 templates for scaffold (bronze/silver/staging SQL)
  taxi/dataset.yml      # example dataset manifest
  registry.yml          # dataset registry
docker/
  flink.Dockerfile      # Flink image (curl + S3A + Iceberg JARs)
  tooling.Dockerfile    # unified Python image (dbt + generator + scripts)
  airflow.Dockerfile    # Airflow + Docker CLI + providers-docker
dags/
  de_pipeline.py        # Airflow DAG (full pipeline orchestration)
flink/
  sql/*.sql.tmpl        # Flink SQL templates (rendered by scaffold)
  conf/                 # Flink/Hadoop config templates
infra/
  base.yml              # core services (Flink JM/TM, dbt, generator)
  broker.redpanda.yml   # Redpanda broker + console (--profile console)
  broker.kafka.yml      # Kafka broker + Kafka UI (--profile console)
  storage.minio.yml     # MinIO (local only)
  observability.yml     # Prometheus + Grafana (always-on)
  orchestration.airflow.yml  # Airflow CeleryExecutor (--profile orchestration)
  catalog.rest-lakekeeper.yml  # REST catalog (advanced, --profile catalog-rest)
  prometheus.yml        # Prometheus scrape config
  grafana/              # Grafana provisioning + dashboard JSON
scripts/
  dataset_manifest.py   # Pydantic manifest schema + loader
  scaffold_dataset.py   # Jinja2 template rendering → SQL + dbt files
  render_sql.py         # string.Template rendering → build/sql/
  validate.py           # 4-stage smoke test (importable)
  health/               # composable health check modules
  wait_for_iceberg.py   # Silver readiness gate
  count_iceberg.py      # Bronze/Silver row count (runs inside dbt container)
tests/
  unit/                 # fast tests, no Docker
  integration/          # Docker E2E tests
build/                  # rendered SQL + conf output (gitignored)
```

---

## 14. Additional Docs

- [docs/00_learning_path.md](docs/00_learning_path.md)
- [docs/01_stack_overview.md](docs/01_stack_overview.md)
- [docs/02_local_quickstart.md](docs/02_local_quickstart.md)
- [docs/03_add_new_dataset.md](docs/03_add_new_dataset.md)
- [docs/04_batch_to_streaming.md](docs/04_batch_to_streaming.md)
- [docs/05_prod_deploy_notes.md](docs/05_prod_deploy_notes.md)
- [docs/06_secrets_and_auth.md](docs/06_secrets_and_auth.md)
- [docs/07_observability.md](docs/07_observability.md)
