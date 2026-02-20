"""
de_template pipeline DAG
========================
Orchestrates the full DE pipeline:
  create_topics → generate_data → flink_bronze → flink_silver → dbt_build → validate

Scheduling:
  schedule=None  — manual trigger by default.
  Set to "@daily" or a cron expression for scheduled runs.

Configuration:
  All pipeline settings are read from Airflow Variables (set once, reuse across
  runs). To bootstrap, go to Admin → Variables and import from a JSON export,
  or set them individually:
    TOPIC, DLQ_TOPIC, TOPIC_RETENTION_MS, DLQ_RETENTION_MS,
    DATA_PATH, KEY_FIELD, GENERATOR_MODE, HOST_DATA_DIR

  Storage credentials (AWS_ACCESS_KEY_ID, etc.) are injected as environment
  variables on the Airflow services via infra/orchestration.airflow.yml, so
  DockerOperator containers inherit them automatically.

Operator strategy:
  BashOperator  — for commands that exec into already-running pipeline
                  containers (Flink SQL client, broker topic creation)
  DockerOperator — for ephemeral tooling tasks (generator, dbt, validate)
                   that use the single tooling image
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# ---------------------------------------------------------------------------
# Container / image names — derived from COMPOSE_PROJECT_NAME
# ---------------------------------------------------------------------------
PROJECT = os.getenv("COMPOSE_PROJECT_NAME", "de_template")
BROKER = os.getenv("BROKER", "redpanda")

FLINK_JM_CONTAINER = f"{PROJECT}-flink-jm"
BROKER_CONTAINER = f"{PROJECT}-{BROKER}"   # template-redpanda or template-kafka
TOOLING_IMAGE = f"{PROJECT}-tooling:latest"
PIPELINE_NETWORK = f"{PROJECT}_pipeline-net"

# ---------------------------------------------------------------------------
# DAG default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "de_template",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# ---------------------------------------------------------------------------
# Shared storage environment — passed into every DockerOperator container
# ---------------------------------------------------------------------------
_storage_env = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", ""),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
    "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    "WAREHOUSE": os.getenv("WAREHOUSE", ""),
    "DUCKDB_S3_ENDPOINT": os.getenv("DUCKDB_S3_ENDPOINT", ""),
    "DUCKDB_S3_USE_SSL": os.getenv("DUCKDB_S3_USE_SSL", "false"),
    "DUCKDB_UNSAFE_ENABLE_VERSION_GUESSING": os.getenv(
        "DUCKDB_UNSAFE_ENABLE_VERSION_GUESSING", "true"
    ),
    "BRONZE_TABLE": os.getenv("BRONZE_TABLE", "bronze.raw_trips"),
    "SILVER_TABLE": os.getenv("SILVER_TABLE", "silver.cleaned_trips"),
    "ALLOW_EMPTY": os.getenv("ALLOW_EMPTY", "false"),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="de_pipeline",
    default_args=default_args,
    description="de_template: full batch pipeline (topics → generate → Flink → dbt → validate)",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger. Change to "@daily" for scheduled runs.
    catchup=False,
    tags=["de_template", "pipeline"],
) as dag:

    # -----------------------------------------------------------------------
    # Step 1: Create Kafka/Redpanda topics
    # -----------------------------------------------------------------------
    # Uses `docker exec` into the running broker container.
    # Idempotent: `|| true` / `--if-not-exists` makes re-runs safe.
    create_topics = BashOperator(
        task_id="create_topics",
        bash_command="""
set -euo pipefail
TOPIC="{{ var.value.TOPIC }}"
DLQ_TOPIC="{{ var.value.DLQ_TOPIC }}"
TOPIC_RMS="{{ var.value.get('TOPIC_RETENTION_MS', '259200000') }}"
DLQ_RMS="{{ var.value.get('DLQ_RETENTION_MS', '604800000') }}"

if [ "$(BROKER)" = "redpanda" ]; then
  docker exec %(broker)s rpk topic create "$TOPIC" \
      --brokers localhost:9092 \
      --partitions 3 --replicas 1 \
      --topic-config "retention.ms=$TOPIC_RMS" \
      --topic-config cleanup.policy=delete || true
  docker exec %(broker)s rpk topic create "$DLQ_TOPIC" \
      --brokers localhost:9092 \
      --partitions 1 --replicas 1 \
      --topic-config "retention.ms=$DLQ_RMS" \
      --topic-config cleanup.policy=delete || true
else
  docker exec %(broker)s /opt/kafka/bin/kafka-topics.sh \
      --create --if-not-exists \
      --bootstrap-server localhost:9092 \
      --topic "$TOPIC" --partitions 3 --replication-factor 1 \
      --config "retention.ms=$TOPIC_RMS" --config cleanup.policy=delete
  docker exec %(broker)s /opt/kafka/bin/kafka-topics.sh \
      --create --if-not-exists \
      --bootstrap-server localhost:9092 \
      --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 1 \
      --config "retention.ms=$DLQ_RMS" --config cleanup.policy=delete
fi
echo "Topics created: $TOPIC + $DLQ_TOPIC"
""" % {"broker": BROKER_CONTAINER},
    )

    # -----------------------------------------------------------------------
    # Step 2: Generate data (ephemeral tooling container)
    # -----------------------------------------------------------------------
    generate_data = DockerOperator(
        task_id="generate_data",
        image=TOOLING_IMAGE,
        command="python /app/generator.py",
        network_mode=PIPELINE_NETWORK,
        auto_remove="success",
        environment={
            **_storage_env,
            "BROKER_URL": "broker:9092",
            "TOPIC": "{{ var.value.TOPIC }}",
            "DLQ_TOPIC": "{{ var.value.DLQ_TOPIC }}",
            "DATA_PATH": "{{ var.value.DATA_PATH }}",
            "METRICS_PATH": "/metrics/latest.json",
            "GENERATOR_MODE": "{{ var.value.get('GENERATOR_MODE', 'burst') }}",
            "KEY_FIELD": "{{ var.value.get('KEY_FIELD', '') }}",
            "MAX_EVENTS": "0",
        },
        mounts=[
            Mount(
                target="/data",
                source="{{ var.value.HOST_DATA_DIR }}",
                type="bind",
                read_only=True,
            ),
        ],
    )

    # -----------------------------------------------------------------------
    # Step 3: Flink Bronze batch job
    # -----------------------------------------------------------------------
    # Execs into the already-running Flink JobManager container.
    flink_bronze = BashOperator(
        task_id="flink_bronze_batch",
        bash_command=(
            f"docker exec -i {FLINK_JM_CONTAINER} "
            "/opt/flink/bin/sql-client.sh embedded "
            "-i /opt/flink/sql/00_catalog.sql "
            "-f /opt/flink/sql/05_bronze_batch.sql"
        ),
    )

    # -----------------------------------------------------------------------
    # Step 4: Flink Silver batch job
    # -----------------------------------------------------------------------
    flink_silver = BashOperator(
        task_id="flink_silver",
        bash_command=(
            f"docker exec -i {FLINK_JM_CONTAINER} "
            "/opt/flink/bin/sql-client.sh embedded "
            "-i /opt/flink/sql/00_catalog.sql "
            "-f /opt/flink/sql/06_silver.sql"
        ),
    )

    # -----------------------------------------------------------------------
    # Step 5: dbt build (ephemeral tooling container)
    # -----------------------------------------------------------------------
    dbt_build = DockerOperator(
        task_id="dbt_build",
        image=TOOLING_IMAGE,
        entrypoint="/bin/sh",
        command="-c 'dbt deps --profiles-dir . && dbt build --full-refresh --profiles-dir .'",
        working_dir="/dbt",
        network_mode=PIPELINE_NETWORK,
        auto_remove="success",
        mounts=[
            # dbt project — absolute host path required for DockerOperator bind mount.
            # DE_TEMPLATE_ROOT must be set on airflow-scheduler/worker in
            # infra/orchestration.airflow.yml for production deployments.
            Mount(
                target="/dbt",
                source=os.path.join(
                    os.getenv("DE_TEMPLATE_ROOT", "/opt/airflow/de_template"), "dbt"
                ),
                type="bind",
                read_only=False,
            ),
        ],
        environment=_storage_env,
    )

    # -----------------------------------------------------------------------
    # Step 6: Validate (ephemeral tooling container)
    # -----------------------------------------------------------------------
    validate = DockerOperator(
        task_id="validate",
        image=TOOLING_IMAGE,
        command="python /scripts/validate.py",
        network_mode=PIPELINE_NETWORK,
        auto_remove="success",
        mounts=[
            Mount(
                target="/scripts",
                source=os.path.join(
                    os.getenv("DE_TEMPLATE_ROOT", "/opt/airflow/de_template"), "scripts"
                ),
                type="bind",
                read_only=True,
            ),
        ],
        environment=_storage_env,
    )

    # -----------------------------------------------------------------------
    # Pipeline dependency chain
    # -----------------------------------------------------------------------
    create_topics >> generate_data >> flink_bronze >> flink_silver >> dbt_build >> validate
