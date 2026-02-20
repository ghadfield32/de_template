# de_template

A local-first data engineering template for **Parquet -> Broker -> Flink -> Iceberg -> dbt**.

This repo is designed to be copied and adapted for new datasets with minimal boilerplate.
It supports:
- Broker: `redpanda` or `kafka`
- Storage: `minio`, `aws_s3`, `gcs`, `azure`
- Pipeline mode: `batch` or `streaming_bronze`
- Generator mode: `burst`, `realtime`, `batch`

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

## 2. Choose Your Path

Use this table first, then follow the linear setup section.

| Decision | Option | Strengths | Trade-offs | Typical situation |
|---|---|---|---|---|
| Broker | `redpanda` | Fast local startup, simple single-binary local dev, web console | Not a 1:1 Kafka distro runtime | Local development and smoke tests |
| Broker | `kafka` | Closer to many production Kafka deployments | Slightly more operational overhead locally | You run Kafka in prod and want parity |
| Pipeline `MODE` | `batch` | Deterministic, easy to reason about, simple reruns/backfills | Higher latency | Daily/hourly loads, CI/E2E, development |
| Pipeline `MODE` | `streaming_bronze` | Low-latency continuous Bronze ingestion | More operational complexity (long-running job, checkpoints) | Near-real-time ingestion use cases |
| Storage | `minio` | Fully local, no cloud creds required | Not real cloud IAM/network behavior | First run and local iteration |
| Storage | `aws_s3`/`gcs`/`azure` | Cloud-realistic storage path | Requires real cloud auth/network setup | Environment parity before deployment |
| Generator `GENERATOR_MODE` | `burst` | Fastest throughput for tests/benchmarks | Less realistic event cadence | Smoke tests and load tests |
| Generator `GENERATOR_MODE` | `realtime` | Better temporal realism | Slower test cycles | Replaying production-like arrival patterns |
| Generator `GENERATOR_MODE` | `batch` | Controlled chunks and pacing | More tuning knobs | Controlled ingestion experiments |

---

## 3. Linear First Run (Recommended)

This is the fastest path to a full PASS on local defaults.

### Step 0: Prerequisites

- Docker Desktop with Compose v2
- `make`
- `uv` (`pip install uv`)
- A Parquet file

### Step 1: Install Python tooling

```bash
make setup-dev
```

### Step 2: Activate a profile

```bash
make env-select ENV=env/local.env
```

This copies `env/local.env` to `.env`.

### Step 3: Put your input Parquet where the generator expects it

By default, the host data dir is `../data` (one level above repo root) and `DATA_PATH` points to `/data/<filename>` inside containers.

```bash
mkdir -p ../data
cp /path/to/your_file.parquet ../data/
```

### Step 4: Run one-command diagnostics

```bash
make doctor
```

This checks settings, manifest validation, template rendering, data-path presence, Docker daemon, and compose config rendering.

Optional deeper check:

```bash
DOCTOR_DBT_PARSE=true make doctor
```

### Step 5: Start infrastructure

```bash
make up
```

### Step 6: Create topics

```bash
make create-topics
```

### Step 7: Produce test data

```bash
make generate-limited
```

### Step 8: Run Flink batch processing

```bash
make process
```

### Step 9: Wait for Silver readiness gate

```bash
make wait-for-silver
```

Readiness uses:
- `WAIT_FOR_SILVER_MIN_ROWS`
- `WAIT_FOR_SILVER_TIMEOUT_SECONDS`
- `WAIT_FOR_SILVER_POLL_SECONDS`

`ALLOW_EMPTY=true` only bypasses timeout when Silver was queryable and consistently returned `0` rows. It does **not** bypass auth/path/query failures.

### Step 10: Build dbt models

```bash
make dbt-build
```

### Step 11: Validate the full pipeline

```bash
make validate
```

Validation stages:
1. Infra health
2. Flink job state
3. Iceberg row counts + DLQ threshold
4. dbt test

### Step 12: Tear down

```bash
make down
```

---

## 4. Operational Playbooks

### A. Batch Playbook (default)

Use when you want deterministic runs and easy backfills.

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

### B. Streaming Bronze Playbook

Use when you want continuous Bronze ingestion with lower latency.

```bash
# Set MODE=streaming_bronze in your active env profile first
make env-select ENV=env/local.env
make up
make create-topics
make process-streaming
make generate-limited
make process-silver
make wait-for-silver
make dbt-build
make validate
```

Useful controls:

```bash
make flink-jobs
make flink-cancel JOB=<job-id>
make flink-restart-streaming
make check-lag
```

Batch vs streaming summary:

| Aspect | Batch (`MODE=batch`) | Streaming Bronze (`MODE=streaming_bronze`) |
|---|---|---|
| Bronze job lifecycle | Finite run | Long-running job |
| Simplicity | Higher | Lower |
| Latency | Higher | Lower |
| Best for | Backfills, scheduled loads, CI | Near-real-time ingestion |

---

## 5. Profiles and Environments

Profiles live in `env/` and are activated via `make env-select ENV=...`.

| Profile | Good for | Notes |
|---|---|---|
| `env/local.env` | Default local start | Redpanda + MinIO |
| `env/local_kafka.env` | Kafka parity testing | Kafka + MinIO |
| `env/aws.env` | AWS path testing | Set real bucket/auth |
| `env/gcs.env` | GCS path testing | Set endpoint/auth correctly |
| `env/azure.env` | Azure path testing | Set endpoint/auth correctly |

If you need REST catalog (`CATALOG=rest`), treat it as advanced/manual compose wiring with `infra/catalog.rest-lakekeeper.yml`.

---

## 6. Scaffold a New Dataset

Recommended way to adapt this template.

### Step 1: Create manifest

```bash
mkdir -p datasets/orders
cp datasets/taxi/dataset.yml datasets/orders/dataset.yml
# edit datasets/orders/dataset.yml
```

### Step 2: Validate manifest

```bash
make scaffold-validate DATASET=orders
```

### Step 3: Generate SQL + dbt assets

```bash
make scaffold DATASET=orders
# optional mart stub:
# make scaffold DATASET=orders MART_STUB=true
```

### Step 4: Render and inspect generated SQL

```bash
make build-sql
make show-sql
```

### Step 5: Run your pipeline flow (batch or streaming)

---

## 7. High-Value Commands

Configuration and diagnostics:

```bash
make print-config
make debug-env
make validate-config
make doctor
```

Data and processing:

```bash
make create-topics
make generate
make generate-limited
make process
make process-bronze
make process-silver
make process-streaming
```

Validation and dbt:

```bash
make wait-for-silver
make dbt-build
make dbt-test
make validate
```

Quality and tests:

```bash
make fmt
make lint
make type
make test-unit
make test-integration
make ci
```

Ops and maintenance:

```bash
make health
make check-lag
make obs-up
make obs-down
make compact-silver
make expire-snapshots
make vacuum
make benchmark
```

---

## 8. Troubleshooting (Use This Order)

1. `make doctor`
2. `make print-config`
3. `make validate-config`
4. `make build-sql` then `make show-sql`
5. `make health`
6. `make validate`

Common issues:
- Data file not found: check `HOST_DATA_DIR` and `DATA_PATH` alignment.
- Silver never ready: inspect Flink job exceptions and storage auth; `ALLOW_EMPTY=true` will not hide query failures.
- dbt reading nothing: verify Silver table path/config and successful Silver writes.

---

## 9. Repository Map

High-impact paths:

- `Makefile`: operational entrypoint
- `env/*.env`: profile templates
- `config/settings.py`: typed config contract
- `scripts/render_sql.py`: template rendering + unresolved placeholder checks
- `scripts/wait_for_iceberg.py`: readiness gate logic
- `scripts/validate.py`: 4-stage validation flow
- `scripts/scaffold_dataset.py`: dataset asset generation
- `scripts/dataset_manifest.py`: typed manifest schema
- `flink/sql/*.tmpl`: SQL templates
- `flink/conf/*.tmpl`: Flink/Hadoop config templates
- `infra/*.yml`: compose overlays
- `dbt/`: dbt models/tests/seeds
- `tests/`: unit and integration coverage

---

## 10. Additional Docs

- `docs/00_learning_path.md`
- `docs/01_stack_overview.md`
- `docs/02_local_quickstart.md`
- `docs/03_add_new_dataset.md`
- `docs/04_batch_to_streaming.md`
- `docs/05_prod_deploy_notes.md`
- `docs/06_cloud_storage.md`
- `docs/06_secrets_and_auth.md`
- `docs/07_observability.md`
