SHELL := bash
# =============================================================================
# de_template: Dual-Broker Data Pipeline Template
# =============================================================================
# Four configuration axes (set in .env):
#   BROKER  = redpanda | kafka
#   CATALOG = hadoop   | rest
#   STORAGE = minio    | aws_s3 | gcs | azure
#   MODE    = batch    | streaming_bronze
#
# Quick start:
#   cp .env.example .env
#   make print-config
#   make up
#   make create-topics
#   make generate-limited
#   make process
#   make validate
# =============================================================================

# Load .env if it exists (exports all vars)
ifneq (,$(wildcard .env))
    include .env
    export
endif

# --- Project name — prevents container/network collisions between parallel stacks ---
# Two checkouts can coexist: COMPOSE_PROJECT_NAME=de_v2 make up
COMPOSE_PROJECT_NAME ?= de_template
export COMPOSE_PROJECT_NAME

# --- Axis defaults (overridable from shell: make up BROKER=kafka) ---
BROKER   ?= redpanda
CATALOG  ?= hadoop
STORAGE  ?= minio
MODE     ?= batch
ALLOW_EMPTY ?= false
GENERATOR_MODE ?= burst
DATASET_NAME ?=
RUN_METRICS_MAX_AGE_MINUTES ?= 120
ALLOW_STALE_RUN_METRICS ?= false
REQUIRE_RUN_METRICS ?= true
BRONZE_TABLE ?= bronze.raw_trips
SILVER_TABLE ?= silver.cleaned_trips
BRONZE_COMPLETENESS_RATIO ?= 0.95
WAIT_FOR_SILVER_MIN_ROWS ?= 1
WAIT_FOR_SILVER_TIMEOUT_SECONDS ?= 90
WAIT_FOR_SILVER_POLL_SECONDS ?= 5
HEALTH_HTTP_TIMEOUT_SECONDS ?= 5
HEALTH_DOCKER_TIMEOUT_SECONDS ?= 15
ICEBERG_QUERY_TIMEOUT_SECONDS ?= 90
ICEBERG_METADATA_TIMEOUT_SECONDS ?= 30
DLQ_READ_TIMEOUT_SECONDS ?= 10
DBT_TEST_TIMEOUT_SECONDS ?= 300
DOCTOR_DBT_PARSE ?= false
VALIDATE_SCHEMA ?= false
SCHEMA_PATH ?= /schemas/taxi_trip.json
BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS ?= 180
BENCHMARK_POLL_SECONDS ?= 5

# GNU make reads .env via `include` as a makefile fragment.
# Lines like "BROKER=redpanda  # comment" leave trailing whitespace in the value.
# Trailing whitespace breaks ifeq comparisons ("redpanda  " != "redpanda").
# $(strip ...) removes leading/trailing whitespace from all four axes.
BROKER   := $(strip $(BROKER))
CATALOG  := $(strip $(CATALOG))
STORAGE  := $(strip $(STORAGE))
MODE     := $(strip $(MODE))
ALLOW_EMPTY := $(strip $(ALLOW_EMPTY))
GENERATOR_MODE := $(strip $(GENERATOR_MODE))
DATASET_NAME := $(strip $(DATASET_NAME))
RUN_METRICS_MAX_AGE_MINUTES := $(strip $(RUN_METRICS_MAX_AGE_MINUTES))
ALLOW_STALE_RUN_METRICS := $(strip $(ALLOW_STALE_RUN_METRICS))
REQUIRE_RUN_METRICS := $(strip $(REQUIRE_RUN_METRICS))
BRONZE_TABLE := $(strip $(BRONZE_TABLE))
SILVER_TABLE := $(strip $(SILVER_TABLE))
BRONZE_COMPLETENESS_RATIO := $(strip $(BRONZE_COMPLETENESS_RATIO))
WAIT_FOR_SILVER_MIN_ROWS := $(strip $(WAIT_FOR_SILVER_MIN_ROWS))
WAIT_FOR_SILVER_TIMEOUT_SECONDS := $(strip $(WAIT_FOR_SILVER_TIMEOUT_SECONDS))
WAIT_FOR_SILVER_POLL_SECONDS := $(strip $(WAIT_FOR_SILVER_POLL_SECONDS))
HEALTH_HTTP_TIMEOUT_SECONDS := $(strip $(HEALTH_HTTP_TIMEOUT_SECONDS))
HEALTH_DOCKER_TIMEOUT_SECONDS := $(strip $(HEALTH_DOCKER_TIMEOUT_SECONDS))
ICEBERG_QUERY_TIMEOUT_SECONDS := $(strip $(ICEBERG_QUERY_TIMEOUT_SECONDS))
ICEBERG_METADATA_TIMEOUT_SECONDS := $(strip $(ICEBERG_METADATA_TIMEOUT_SECONDS))
DLQ_READ_TIMEOUT_SECONDS := $(strip $(DLQ_READ_TIMEOUT_SECONDS))
DBT_TEST_TIMEOUT_SECONDS := $(strip $(DBT_TEST_TIMEOUT_SECONDS))
DOCTOR_DBT_PARSE := $(strip $(DOCTOR_DBT_PARSE))
VALIDATE_SCHEMA := $(strip $(VALIDATE_SCHEMA))
SCHEMA_PATH := $(strip $(SCHEMA_PATH))
BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS := $(strip $(BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS))
BENCHMARK_POLL_SECONDS := $(strip $(BENCHMARK_POLL_SECONDS))

# --- Data directory (host-side, passed to docker compose via environment) ---
# Absolute path is used so Docker Compose gets an unambiguous bind mount source
# regardless of which directory docker compose resolves relative paths from.
# Default: repo-root data/ (one level up from de_template/)
# Override in .env or shell: HOST_DATA_DIR=/your/absolute/path
HOST_DATA_DIR ?= $(abspath $(CURDIR)/../data)
export HOST_DATA_DIR

# --- Compose file assembly based on axes ---
# Observability (Prometheus + Grafana) is always included — starts with `make up`
COMPOSE_FILES := -f infra/base.yml -f infra/observability.yml

ifeq ($(BROKER),redpanda)
    COMPOSE_FILES += -f infra/broker.redpanda.yml
else
    COMPOSE_FILES += -f infra/broker.kafka.yml
endif

ifeq ($(STORAGE),minio)
    COMPOSE_FILES += -f infra/storage.minio.yml
endif

# Orchestration overlay (Airflow) — included unconditionally so `down` can stop it
COMPOSE_FILES += -f infra/orchestration.airflow.yml

COMPOSE          = docker compose $(COMPOSE_FILES)
FLINK_SQL_CLIENT = MSYS_NO_PATHCONV=1 $(COMPOSE) exec -T flink-jobmanager /opt/flink/bin/sql-client.sh embedded

.PHONY: help print-config validate-config \
        doctor \
        env-select \
        up down clean restart \
        build-sql show-sql \
        create-topics \
        generate generate-limited persist-run-metrics \
        process process-bronze process-silver process-streaming \
        wait-for-silver \
        dbt-build dbt-test dbt-docs \
        validate validate-py health check-lag \
        setup-dev add-dep venv-reset test test-unit test-integration \
        fmt lint type ci \
        scaffold scaffold-validate \
        obs-up obs-down \
        console-up console-down \
        orch-up orch-down \
        benchmark \
        flink-jobs flink-cancel flink-restart-streaming \
        compact-silver expire-snapshots vacuum maintain \
        logs logs-broker logs-flink logs-flink-tm status ps

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# =============================================================================
# Environment Profile Selection
# =============================================================================

ENV ?= env/local.env

env-select: ## Copy a named env profile to .env: make env-select ENV=env/local.env
ifndef ENV
	@uv run python -c "import pathlib; profiles = sorted(pathlib.Path('env').glob('*.env')); print('Usage: make env-select ENV=env/<profile>.env\nAvailable profiles:'); [print(f'  {p.name}') for p in profiles]"
	@exit 1
endif
	@uv run python -c "import shutil; shutil.copy('$(ENV)', '.env'); print('Active config: $(ENV) -> .env')"
	@$(MAKE) print-config

debug-env: ## Show raw variable values with [brackets] to expose whitespace/path issues
	@echo "=== de_template: Raw Variable Debug ==="
	@echo "BROKER        = [$(BROKER)]"
	@echo "CATALOG       = [$(CATALOG)]"
	@echo "STORAGE       = [$(STORAGE)]"
	@echo "MODE          = [$(MODE)]"
	@echo "GENERATOR_MODE = [$(GENERATOR_MODE)]"
	@echo "ALLOW_EMPTY   = [$(ALLOW_EMPTY)]"
	@echo "DATASET_NAME  = [$(DATASET_NAME)]"
	@echo "RUN_METRICS_MAX_AGE_MINUTES = [$(RUN_METRICS_MAX_AGE_MINUTES)]"
	@echo "ALLOW_STALE_RUN_METRICS     = [$(ALLOW_STALE_RUN_METRICS)]"
	@echo "REQUIRE_RUN_METRICS         = [$(REQUIRE_RUN_METRICS)]"
	@echo "BRONZE_TABLE = [$(BRONZE_TABLE)]"
	@echo "SILVER_TABLE = [$(SILVER_TABLE)]"
	@echo "BRONZE_COMPLETENESS_RATIO = [$(BRONZE_COMPLETENESS_RATIO)]"
	@echo "WAIT_FOR_SILVER_MIN_ROWS = [$(WAIT_FOR_SILVER_MIN_ROWS)]"
	@echo "WAIT_FOR_SILVER_TIMEOUT_SECONDS = [$(WAIT_FOR_SILVER_TIMEOUT_SECONDS)]"
	@echo "WAIT_FOR_SILVER_POLL_SECONDS = [$(WAIT_FOR_SILVER_POLL_SECONDS)]"
	@echo "HEALTH_HTTP_TIMEOUT_SECONDS = [$(HEALTH_HTTP_TIMEOUT_SECONDS)]"
	@echo "HEALTH_DOCKER_TIMEOUT_SECONDS = [$(HEALTH_DOCKER_TIMEOUT_SECONDS)]"
	@echo "ICEBERG_QUERY_TIMEOUT_SECONDS = [$(ICEBERG_QUERY_TIMEOUT_SECONDS)]"
	@echo "ICEBERG_METADATA_TIMEOUT_SECONDS = [$(ICEBERG_METADATA_TIMEOUT_SECONDS)]"
	@echo "DLQ_READ_TIMEOUT_SECONDS = [$(DLQ_READ_TIMEOUT_SECONDS)]"
	@echo "DBT_TEST_TIMEOUT_SECONDS = [$(DBT_TEST_TIMEOUT_SECONDS)]"
	@echo "VALIDATE_SCHEMA = [$(VALIDATE_SCHEMA)]"
	@echo "SCHEMA_PATH = [$(SCHEMA_PATH)]"
	@echo "BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS = [$(BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS)]"
	@echo "BENCHMARK_POLL_SECONDS = [$(BENCHMARK_POLL_SECONDS)]"
	@echo "COMPOSE_FILES = [$(COMPOSE_FILES)]"
	@echo "HOST_DATA_DIR = [$(HOST_DATA_DIR)]"
	@echo "DATA_PATH     = [$(DATA_PATH)]"
	@echo "TOPIC         = [$(TOPIC)]"
	@echo "WAREHOUSE     = [$(WAREHOUSE)]"

# =============================================================================
# Configuration
# =============================================================================

print-config: ## Echo all resolved configuration axes
	@echo "=== de_template: Resolved Configuration ==="
	@echo "BROKER   = $(BROKER)"
	@echo "CATALOG  = $(CATALOG)"
	@echo "STORAGE  = $(STORAGE)"
	@echo "MODE     = $(MODE)"
	@echo "GENERATOR_MODE= $(GENERATOR_MODE)"
	@echo "ALLOW_EMPTY= $(ALLOW_EMPTY)"
	@echo "DATASET_NAME= $(DATASET_NAME)"
	@echo "RUN_METRICS_MAX_AGE_MINUTES= $(RUN_METRICS_MAX_AGE_MINUTES)"
	@echo "ALLOW_STALE_RUN_METRICS= $(ALLOW_STALE_RUN_METRICS)"
	@echo "REQUIRE_RUN_METRICS= $(REQUIRE_RUN_METRICS)"
	@echo "BRONZE_TABLE= $(BRONZE_TABLE)"
	@echo "SILVER_TABLE= $(SILVER_TABLE)"
	@echo "BRONZE_COMPLETENESS_RATIO= $(BRONZE_COMPLETENESS_RATIO)"
	@echo "WAIT_FOR_SILVER_MIN_ROWS= $(WAIT_FOR_SILVER_MIN_ROWS)"
	@echo "WAIT_FOR_SILVER_TIMEOUT_SECONDS= $(WAIT_FOR_SILVER_TIMEOUT_SECONDS)"
	@echo "WAIT_FOR_SILVER_POLL_SECONDS= $(WAIT_FOR_SILVER_POLL_SECONDS)"
	@echo "HEALTH_HTTP_TIMEOUT_SECONDS= $(HEALTH_HTTP_TIMEOUT_SECONDS)"
	@echo "HEALTH_DOCKER_TIMEOUT_SECONDS= $(HEALTH_DOCKER_TIMEOUT_SECONDS)"
	@echo "ICEBERG_QUERY_TIMEOUT_SECONDS= $(ICEBERG_QUERY_TIMEOUT_SECONDS)"
	@echo "ICEBERG_METADATA_TIMEOUT_SECONDS= $(ICEBERG_METADATA_TIMEOUT_SECONDS)"
	@echo "DLQ_READ_TIMEOUT_SECONDS= $(DLQ_READ_TIMEOUT_SECONDS)"
	@echo "DBT_TEST_TIMEOUT_SECONDS= $(DBT_TEST_TIMEOUT_SECONDS)"
	@echo "VALIDATE_SCHEMA= $(VALIDATE_SCHEMA)"
	@echo "SCHEMA_PATH= $(SCHEMA_PATH)"
	@echo "BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS= $(BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS)"
	@echo "BENCHMARK_POLL_SECONDS= $(BENCHMARK_POLL_SECONDS)"
	@echo "TOPIC    = $(TOPIC)"
	@echo "DLQ_TOPIC= $(DLQ_TOPIC)"
	@echo "DLQ_MAX  = $(DLQ_MAX)"
	@echo "WAREHOUSE= $(WAREHOUSE)"
	@echo "S3_ENDPOINT (Flink)  = $(S3_ENDPOINT)"
	@echo "DUCKDB_S3_ENDPOINT   = $(DUCKDB_S3_ENDPOINT)"
	@echo "DATA_PATH= $(DATA_PATH)"
	@echo "MAX_EVENTS= $(MAX_EVENTS)"

validate-config: ## Fail on incompatible configuration combinations
ifeq ($(CATALOG),rest)
	@echo "CATALOG=rest: verifying catalog.rest-lakekeeper.yml is valid..."
	@$(COMPOSE) --profile catalog-rest config --quiet 2>/dev/null \
		|| (echo "ERROR: CATALOG=rest config invalid — check infra/catalog.rest-lakekeeper.yml" && exit 1)
endif
	@uv run python -c "from config.settings import load_settings; s=load_settings(); print(f'Config schema OK: BROKER=[{s.BROKER}] CATALOG=[{s.CATALOG}] STORAGE=[{s.STORAGE}] MODE=[{s.MODE}] GENERATOR_MODE=[{s.GENERATOR_MODE}]')"
	@# Verify data file exists on the host before starting containers
	@DATA_FILE="$(HOST_DATA_DIR)/$(notdir $(DATA_PATH))"; \
	    [ -f "$$DATA_FILE" ] \
	    || (echo "ERROR: Parquet file not found: $$DATA_FILE" \
	        && echo "  HOST_DATA_DIR = $(HOST_DATA_DIR)" \
	        && echo "  DATA_PATH     = $(DATA_PATH)" \
	        && echo "  Place your parquet file at $$DATA_FILE" \
	        && echo "  or set HOST_DATA_DIR in .env to its parent directory." \
	        && exit 1)
	@echo "Config + data path check OK."
	@echo "Data file: $(HOST_DATA_DIR)/$(notdir $(DATA_PATH))"

doctor: ## Run local diagnostics (settings, manifests, template render, docker, compose)
	@echo "=== de_template doctor ==="
	@echo "[1/7] Settings schema"
	@uv run python -c "from config.settings import load_settings; s=load_settings(); print(f'OK: BROKER={s.BROKER} MODE={s.MODE} GENERATOR_MODE={s.GENERATOR_MODE} STORAGE={s.STORAGE}')"
	@echo "[2/7] Dataset manifests"
	@uv run python -c "from scripts.dataset_manifest import available_datasets, parse_manifest; ds = available_datasets(); [parse_manifest(d) for d in ds]; print('OK: ' + (', '.join(ds) if ds else '(no datasets found)'))"
	@echo "[3/7] SQL/conf render"
	@$(MAKE) build-sql
	@echo "[4/7] Host data path"
	@DATA_FILE="$(HOST_DATA_DIR)/$(notdir $(DATA_PATH))"; \
	    [ -f "$$DATA_FILE" ] \
	    && echo "OK: $$DATA_FILE" \
	    || (echo "ERROR: missing parquet file: $$DATA_FILE" && exit 1)
	@echo "[5/7] Docker daemon"
	@docker info > /dev/null 2>&1 \
	    && echo "OK: docker daemon reachable" \
	    || (echo "ERROR: docker daemon is not reachable" && exit 1)
	@echo "[6/7] Docker compose config"
	@mkdir -p build
	@$(COMPOSE) config > build/doctor.compose.yml
	@echo "OK: build/doctor.compose.yml"
	@echo "[7/7] dbt parse"
	@if [ "$(DOCTOR_DBT_PARSE)" = "true" ] || [ "$(DOCTOR_DBT_PARSE)" = "TRUE" ] || [ "$(DOCTOR_DBT_PARSE)" = "1" ] || [ "$(DOCTOR_DBT_PARSE)" = "yes" ] || [ "$(DOCTOR_DBT_PARSE)" = "YES" ]; then \
	    MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm --no-deps --entrypoint /bin/sh dbt -c "dbt parse --profiles-dir ."; \
	    echo "OK: dbt parse"; \
	else \
	    echo "SKIP: set DOCTOR_DBT_PARSE=true to run dbt parse"; \
	fi

# =============================================================================
# Lifecycle
# =============================================================================

up: validate-config build-sql ## Start all infrastructure services (incl. Prometheus + Grafana)
	$(COMPOSE) up -d
	@echo ""
	@echo "=== de_template: $(BROKER) + Flink + Iceberg + Observability ==="
	@echo "Flink Dashboard:    http://localhost:8081"
	@echo "Grafana:            http://localhost:3000  (admin/admin)"
	@echo "Prometheus:         http://localhost:9090"
ifeq ($(STORAGE),minio)
	@echo "MinIO Console:      http://localhost:9001  ($(MINIO_ROOT_USER)/$(MINIO_ROOT_PASSWORD))"
endif
	@echo ""
	@echo "Optional add-ons:"
	@echo "  make console-up   → Broker topic UI on http://localhost:8085"
	@echo "  make orch-up      → Airflow UI on http://localhost:8080  (admin/admin)"
	@echo ""
	@echo "Next steps:"
	@echo "  make create-topics    → Create Kafka topics"
	@echo "  make generate-limited → Produce 10k test events"
	@echo "  make process          → Submit Flink SQL batch jobs"
	@echo "  make validate         → Run 4-stage smoke test"

down: ## Stop all services and remove volumes
	$(COMPOSE) --profile generator --profile dbt --profile catalog-rest \
	    --profile console --profile orchestration down -v
	@echo "Pipeline stopped and volumes removed."

clean: ## Stop everything and prune all related resources
	$(COMPOSE) --profile generator --profile dbt --profile catalog-rest \
	    --profile console --profile orchestration down -v --remove-orphans
	rm -rf build/
	@echo "Pipeline fully cleaned."

restart: ## Restart all services
	$(MAKE) down
	$(MAKE) up

# =============================================================================
# SQL Template Rendering
# =============================================================================

build-sql: ## Render SQL + conf templates → build/sql/ + build/conf/ (strict)
	@mkdir -p build/sql build/conf
	@BROKER="$(BROKER)" CATALOG="$(CATALOG)" STORAGE="$(STORAGE)" MODE="$(MODE)" \
	    TOPIC="$(TOPIC)" DLQ_TOPIC="$(DLQ_TOPIC)" \
	    WAREHOUSE="$(WAREHOUSE)" S3_ENDPOINT="$(S3_ENDPOINT)" \
	    S3_USE_SSL="$(S3_USE_SSL)" S3_PATH_STYLE="$(S3_PATH_STYLE)" \
	    AWS_ACCESS_KEY_ID="$(AWS_ACCESS_KEY_ID)" \
	    AWS_SECRET_ACCESS_KEY="$(AWS_SECRET_ACCESS_KEY)" \
	    AWS_REGION="$(AWS_REGION)" \
	    python3 scripts/render_sql.py

show-sql: ## Print rendered SQL files and the active config axes
	@echo "=== Active config: BROKER=$(BROKER) CATALOG=$(CATALOG) STORAGE=$(STORAGE) MODE=$(MODE) ==="
	@echo ""
	@for f in build/sql/*.sql build/conf/*.xml; do \
	    [ -f "$$f" ] || continue; \
	    echo "━━━ $$f ━━━"; \
	    cat "$$f"; \
	    echo ""; \
	done

# =============================================================================
# Topic Management
# =============================================================================

# Retention defaults — override in .env (TOPIC_RETENTION_MS / DLQ_RETENTION_MS)
TOPIC_RETENTION_MS ?= 259200000
DLQ_RETENTION_MS   ?= 604800000

create-topics: ## Create broker topics (primary + Dead Letter Queue)
ifeq ($(BROKER),redpanda)
	$(COMPOSE) exec broker rpk topic create $(TOPIC) \
		--brokers localhost:9092 \
		--partitions 3 \
		--replicas 1 \
		--topic-config retention.ms=$(TOPIC_RETENTION_MS) \
		--topic-config cleanup.policy=delete || true
	$(COMPOSE) exec broker rpk topic create $(DLQ_TOPIC) \
		--brokers localhost:9092 \
		--partitions 1 \
		--replicas 1 \
		--topic-config retention.ms=$(DLQ_RETENTION_MS) \
		--topic-config cleanup.policy=delete || true
	@$(COMPOSE) exec broker rpk topic list --brokers localhost:9092
else
	$(COMPOSE) exec broker /opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(TOPIC) \
		--partitions 3 \
		--replication-factor 1 \
		--config retention.ms=$(TOPIC_RETENTION_MS) \
		--config cleanup.policy=delete
	$(COMPOSE) exec broker /opt/kafka/bin/kafka-topics.sh \
		--create --if-not-exists \
		--bootstrap-server localhost:9092 \
		--topic $(DLQ_TOPIC) \
		--partitions 1 \
		--replication-factor 1 \
		--config retention.ms=$(DLQ_RETENTION_MS) \
		--config cleanup.policy=delete
	@$(COMPOSE) exec broker /opt/kafka/bin/kafka-topics.sh \
		--list --bootstrap-server localhost:9092
endif
	@echo "Topics created: $(TOPIC) (3 partitions, $(TOPIC_RETENTION_MS)ms) + $(DLQ_TOPIC) (DLQ, $(DLQ_RETENTION_MS)ms)"

# =============================================================================
# Data Generation
# =============================================================================

generate: ## Produce all events to broker (burst mode)
	MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm data-generator
	@$(MAKE) persist-run-metrics
	@echo "Data generation complete."

generate-limited: ## Produce 10k events for testing (smoke test)
	MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm -e MAX_EVENTS=10000 data-generator
	@$(MAKE) persist-run-metrics
	@echo "Limited data generation complete (10k events)."

persist-run-metrics: ## Copy generator metrics volume -> build/run_metrics.json
	@mkdir -p build
	@TMP_FILE="$$(mktemp)"; \
	if MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm --no-deps \
	    --entrypoint python3 dbt \
	    -c "import json; print(json.dumps(json.load(open('/metrics/latest.json'))))" > "$$TMP_FILE"; then \
	    mv "$$TMP_FILE" build/run_metrics.json; \
	    echo "Run metrics written: build/run_metrics.json"; \
	else \
	    rm -f "$$TMP_FILE"; \
	    echo "WARN: unable to persist run metrics from /metrics/latest.json"; \
	fi

# =============================================================================
# Flink SQL Processing
# =============================================================================

process: process-bronze process-silver ## Submit all Flink SQL batch jobs (Bronze + Silver)
	@echo "All Flink SQL batch jobs complete."

process-bronze: ## Submit Bronze layer Flink SQL jobs (batch: Kafka → Iceberg)
	@echo "=== Bronze: $(BROKER) → Iceberg ==="
	$(FLINK_SQL_CLIENT) \
	    -i /opt/flink/sql/00_catalog.sql \
	    -f /opt/flink/sql/05_bronze_batch.sql
	@echo "Bronze layer complete."

process-silver: ## Submit Silver layer Flink SQL jobs (batch: Bronze → Cleaned Iceberg)
	@echo "=== Silver: Bronze → Cleaned Iceberg ==="
	$(FLINK_SQL_CLIENT) \
	    -i /opt/flink/sql/00_catalog.sql \
	    -f /opt/flink/sql/06_silver.sql
	@echo "Silver layer complete."

process-streaming: ## Start continuous streaming Bronze job (runs indefinitely)
	@echo "=== Streaming Bronze: $(BROKER) → Iceberg (continuous) ==="
	@echo "NOTE: This job runs indefinitely. Cancel from Flink Dashboard or Ctrl+C."
	$(FLINK_SQL_CLIENT) \
	    -i /opt/flink/sql/00_catalog.sql \
	    -f /opt/flink/sql/07_bronze_streaming.sql

# =============================================================================
# Wait Gate (replaces sleep 15)
# =============================================================================

wait-for-silver: ## Poll Silver via iceberg_scan until WAIT_FOR_SILVER_MIN_ROWS is met
	MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm \
	    --entrypoint python3 dbt \
	    /scripts/wait_for_iceberg.py
	@echo "Silver table ready."

# =============================================================================
# dbt Transformations
# =============================================================================

dbt-build: ## Run dbt build (full-refresh) on Iceberg Silver data
	MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm \
	    --entrypoint /bin/sh dbt \
	    -c "dbt deps --profiles-dir . && dbt build --full-refresh --profiles-dir ."
	@echo "dbt build complete."

dbt-test: ## Run dbt tests only
	MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm \
	    --entrypoint /bin/sh dbt \
	    -c "dbt test --profiles-dir ."

dbt-docs: ## Generate dbt documentation
	MSYS_NO_PATHCONV=1 $(COMPOSE) run --rm \
	    --entrypoint /bin/sh dbt \
	    -c "dbt docs generate --profiles-dir ."

# =============================================================================
# Validation (4-stage smoke test)
# =============================================================================

validate: validate-py ## Run 4-stage smoke test (typed Python validator)

validate-py: ## Run 4-stage smoke test (Python via uv)
	uv run python scripts/validate.py

# =============================================================================
# Developer Setup & Tests  (managed by uv — see pyproject.toml)
# =============================================================================
# Dependency groups in pyproject.toml:
#   [project.dependencies]          — shared base (pydantic, pyyaml, jinja2)
#   [dependency-groups.dev]         — host-only tools (pytest, ruff, pyright) ← synced here
#   [dependency-groups.container]   — Docker tooling image runtime deps
#   [dependency-groups.data-connectors] — API clients / DB drivers (extend as needed)
#
# To add a new dep to the tooling container:
#   make add-dep PKG="httpx>=0.27"                        → adds to container group
#   make add-dep GROUP=data-connectors PKG="sqlalchemy"   → adds to data-connectors group
# Then rebuild: docker compose build (or make up)
# =============================================================================

setup-dev: ## Create/sync .venv with dev tools (pytest, ruff, pyright)
	uv sync

add-dep: ## Add a dep to pyproject.toml: make add-dep PKG="httpx>=0.27" [GROUP=container]
ifndef PKG
	@echo "Usage: make add-dep PKG='<package>[>=version]' [GROUP=<container|data-connectors|dev>]"
	@echo "  Default GROUP=container (container tooling image)"
	@echo "  After adding: docker compose build (or make up) to rebuild the tooling image"
	@exit 1
endif
	uv add --group $(or $(GROUP),container) $(PKG)
	@echo ""
	@echo "Added '$(PKG)' to group '$(or $(GROUP),container)' in pyproject.toml + uv.lock"
	@echo "Rebuild the tooling image to make it available in containers:"
	@echo "  docker compose build"

venv-reset: ## Delete and recreate .venv (fixes WSL/Windows cross-platform venv contamination)
	@echo "Removing .venv..."
	rm -rf .venv
	@echo "Recreating .venv for this platform..."
	uv sync
	@echo "Done. Run: cat .venv/pyvenv.cfg  to verify 'home' points to the correct platform."

test: test-unit ## Run fast unit tests (alias for test-unit)

test-unit: ## Run unit tests — no Docker required, runs in seconds
	uv run pytest tests/unit/ -v

test-integration: ## Run full E2E integration tests — requires Docker + active .env
	uv run pytest tests/integration/ -v -m integration --timeout=600

# =============================================================================
# Code Quality  (ruff + pyright — see pyproject.toml for config)
# =============================================================================

fmt: ## Auto-format all Python files with ruff
	uv run ruff format .

lint: ## Lint all Python files with ruff (auto-fix safe violations)
	uv run ruff check . --fix

type: ## Type-check all Python files with pyright
	uv run pyright

ci: lint type test-unit ## Fast local pre-push check: lint + type + unit tests

# =============================================================================
# Dataset Scaffold
# =============================================================================

scaffold-validate: ## Validate datasets/<name>/dataset.yml schema: make scaffold-validate DATASET=taxi
ifndef DATASET
	@uv run python -c "import pathlib; datasets=[p.name for p in sorted(pathlib.Path('datasets').iterdir()) if p.is_dir() and not p.name.startswith('_')]; print('Usage: make scaffold DATASET=<name>\nAvailable datasets:\n' + '\n'.join('  ' + d for d in datasets))"
	@exit 1
endif
	uv run python scripts/validate_manifest.py $(DATASET)

scaffold: ## Generate SQL + dbt assets from manifest: make scaffold DATASET=taxi [MART_STUB=true]
ifndef DATASET
	@uv run python -c "import pathlib; datasets=[p.name for p in sorted(pathlib.Path('datasets').iterdir()) if p.is_dir() and not p.name.startswith('_')]; print('Usage: make scaffold DATASET=<name>\nAvailable datasets:\n' + '\n'.join('  ' + d for d in datasets))"
	@exit 1
endif
	@$(MAKE) scaffold-validate DATASET=$(DATASET)
	uv run python scripts/scaffold_dataset.py $(DATASET) $(if $(filter true TRUE 1 yes YES,$(MART_STUB)),--with-mart-stub)
	@echo "Next: make build-sql"

health: ## Quick health check of all services
	@echo "=== de_template: Health Check (BROKER=$(BROKER)) ==="
ifeq ($(BROKER),redpanda)
	@echo -n "Redpanda:         " && $(COMPOSE) exec -T broker \
	    rpk cluster health 2>/dev/null | grep -qE 'Healthy:.+true' \
	    && echo "OK" || echo "FAIL"
else
	@echo -n "Kafka:            " && $(COMPOSE) exec -T broker \
	    /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 \
	    > /dev/null 2>&1 && echo "OK" || echo "FAIL"
endif
	@echo -n "Flink Dashboard:  " && curl -sf http://localhost:8081/overview > /dev/null 2>&1 \
	    && echo "OK" || echo "FAIL"
	@echo -n "Prometheus:       " && curl -sf http://localhost:9090/-/healthy > /dev/null 2>&1 \
	    && echo "OK" || echo "FAIL"
	@echo -n "Grafana:          " && curl -sf http://localhost:3000/api/health > /dev/null 2>&1 \
	    && echo "OK" || echo "FAIL"
ifeq ($(STORAGE),minio)
	@echo -n "MinIO:            " && curl -sf http://localhost:9000/minio/health/live \
	    > /dev/null 2>&1 && echo "OK" || echo "FAIL"
endif

check-lag: ## Show consumer group lag and DLQ status
ifeq ($(BROKER),redpanda)
	$(COMPOSE) exec broker rpk group describe flink-consumer --brokers localhost:9092
	@echo ""
	@$(COMPOSE) exec -T broker rpk topic consume $(DLQ_TOPIC) \
	    --brokers localhost:9092 --num 1 2>/dev/null \
	    && echo "DLQ has messages - investigate!" || echo "DLQ: empty (OK)"
else
	$(COMPOSE) exec broker /opt/kafka/bin/kafka-consumer-groups.sh \
	    --bootstrap-server localhost:9092 --describe --group flink-consumer
	@echo ""
	@$(COMPOSE) exec -T broker /opt/kafka/bin/kafka-console-consumer.sh \
	    --bootstrap-server localhost:9092 --topic $(DLQ_TOPIC) \
	    --from-beginning --max-messages 1 --timeout-ms 3000 2>/dev/null \
	    && echo "DLQ has messages - investigate!" || echo "DLQ: empty (OK)"
endif

# =============================================================================
# Benchmark (Full End-to-End)
# =============================================================================

benchmark: ## Full benchmark: down → up → topics → generate → process → validate → down
	@echo "============================================================"
	@echo "  de_template Benchmark"
	@echo "  BROKER=$(BROKER)  MODE=$(MODE)"
	@echo "============================================================"
	@START_TIME=$$(date +%s) && \
	$(MAKE) down 2>/dev/null || true && \
	$(MAKE) up && \
	echo "Waiting for Flink Dashboard to become reachable..." && \
	timeout $(BENCHMARK_SERVICE_READY_TIMEOUT_SECONDS) bash -c "until curl -sf http://localhost:8081/overview > /dev/null; do sleep $(BENCHMARK_POLL_SECONDS); done" && \
	$(MAKE) create-topics && \
	$(MAKE) generate-limited && \
	$(MAKE) process && \
	$(MAKE) wait-for-silver && \
	$(MAKE) dbt-build && \
	END_TIME=$$(date +%s) && \
	ELAPSED=$$((END_TIME - START_TIME)) && \
	echo "" && \
	echo "============================================================" && \
	echo "  BENCHMARK COMPLETE" && \
	echo "  Total elapsed: $${ELAPSED}s" && \
	echo "============================================================" && \
	mkdir -p benchmark_results && \
	echo "{\"broker\": \"$(BROKER)\", \"mode\": \"$(MODE)\", \"elapsed_seconds\": $$ELAPSED, \"timestamp\": \"$$(date -Iseconds)\"}" \
	    > benchmark_results/latest.json && \
	echo "Results saved to benchmark_results/latest.json" && \
	$(MAKE) down

# =============================================================================
# Flink Job Lifecycle (streaming operational control)
# =============================================================================

flink-jobs: ## List all Flink jobs with state + restart count
	@echo "=== Flink Jobs ==="
	@curl -s http://localhost:8081/jobs/overview 2>/dev/null | \
	    python3 -c "import json,sys; d=json.load(sys.stdin); jobs=d.get('jobs',[]); [print('  (no jobs)') for _ in [1] if not jobs] or [print(f'  {j[\"jid\"][:8]}...  state={j[\"state\"]:<10}  name={j[\"name\"]}') for j in jobs]" \
	    2>/dev/null || echo "  (Flink not running)"

flink-cancel: ## Cancel a Flink job by ID: make flink-cancel JOB=<job-id>
ifndef JOB
	@echo "Usage: make flink-cancel JOB=<job-id>"
	@echo "Get job IDs with: make flink-jobs"
	@exit 1
endif
	@echo "Cancelling Flink job: $(JOB)"
	curl -sf -XPATCH "http://localhost:8081/jobs/$(JOB)?mode=cancel" \
	    && echo "Job $(JOB) cancelled." \
	    || echo "Cancel failed — check job ID with: make flink-jobs"

flink-restart-streaming: ## Cancel all RUNNING streaming jobs then start fresh Bronze
	@echo "=== Cancelling all RUNNING Flink jobs ==="
	@RUNNING=$$(curl -s http://localhost:8081/jobs/overview 2>/dev/null | \
	    python3 -c "import json,sys; \
	        d=json.load(sys.stdin); \
	        print(' '.join(j['jid'] for j in d.get('jobs',[]) if j['state']=='RUNNING'))" \
	    2>/dev/null || echo ""); \
	if [ -n "$$RUNNING" ]; then \
	    for jid in $$RUNNING; do \
	        echo "  Cancelling $$jid ..."; \
	        curl -sf -XPATCH "http://localhost:8081/jobs/$$jid?mode=cancel" > /dev/null; \
	    done; \
	    sleep 3; \
	else \
	    echo "  No RUNNING jobs found."; \
	fi
	$(MAKE) process-streaming

# =============================================================================
# Iceberg Maintenance (run periodically in production)
# =============================================================================

compact-silver: ## Compact Silver Iceberg files to target 128MB
	@bash scripts/iceberg_maintenance.sh compact-silver \
	    SQL_CLIENT="$(FLINK_SQL_CLIENT)"

expire-snapshots: ## Expire Iceberg snapshots older than 7 days
	@bash scripts/iceberg_maintenance.sh expire-snapshots \
	    SQL_CLIENT="$(FLINK_SQL_CLIENT)"

vacuum: ## Remove orphan Iceberg files (run weekly)
	@bash scripts/iceberg_maintenance.sh vacuum \
	    SQL_CLIENT="$(FLINK_SQL_CLIENT)"

maintain: compact-silver expire-snapshots ## Run all routine Iceberg maintenance

# =============================================================================
# Observability (Prometheus + Grafana start automatically with `make up`)
# =============================================================================

obs-up: ## Restart Prometheus + Grafana (already included in `make up`)
	$(COMPOSE) up -d prometheus grafana
	@echo "Grafana:    http://localhost:3000  (admin/admin)"
	@echo "Prometheus: http://localhost:9090"

obs-down: ## Stop Prometheus + Grafana
	$(COMPOSE) stop prometheus grafana

# =============================================================================
# Broker Console (topic UI — opt-in, port 8085)
# =============================================================================

console-up: ## Start broker topic UI (--profile console): make console-up
	$(COMPOSE) --profile console up -d
	@echo "Broker console: http://localhost:8085"

console-down: ## Stop broker topic UI
	$(COMPOSE) --profile console down

# =============================================================================
# Orchestration (Airflow CeleryExecutor — opt-in, port 8080)
# =============================================================================

orch-up: ## Start Airflow orchestration stack (--profile orchestration)
	$(COMPOSE) --profile orchestration up -d airflow-postgres airflow-redis
	@echo "Waiting for Airflow DB to be ready..."
	@sleep 10
	$(COMPOSE) --profile orchestration run --rm airflow-init
	$(COMPOSE) --profile orchestration up -d airflow-webserver airflow-scheduler airflow-worker
	@echo ""
	@echo "Airflow UI: http://localhost:8080  (admin/admin)"
	@echo "Trigger the de_pipeline DAG from the UI or via:"
	@echo "  docker exec template-airflow-scheduler airflow dags trigger de_pipeline"

orch-down: ## Stop Airflow orchestration stack
	$(COMPOSE) --profile orchestration down

logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

logs-broker: ## Tail broker (Redpanda/Kafka) logs
	$(COMPOSE) logs -f broker

logs-flink: ## Tail Flink JobManager logs
	$(COMPOSE) logs -f flink-jobmanager

logs-flink-tm: ## Tail Flink TaskManager logs
	$(COMPOSE) logs -f flink-taskmanager

status: ## Show service status and Flink job list
	@echo "=== de_template: Service Status ==="
	$(COMPOSE) ps
	@echo ""
	@echo "=== Flink Jobs ==="
	@curl -s http://localhost:8081/jobs/overview 2>/dev/null | \
	    python3 -m json.tool 2>/dev/null || echo "(Flink not running)"

ps: ## Show running containers
	$(COMPOSE) ps
