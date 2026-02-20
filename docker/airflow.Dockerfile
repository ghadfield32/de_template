FROM apache/airflow:2.10.0

USER root

# Docker CLI — required by airflow-scheduler and airflow-worker to exec into
# running pipeline containers (Flink JM, broker) via BashOperator.
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# DockerOperator support for launching ephemeral tooling containers
RUN pip install --no-cache-dir apache-airflow-providers-docker
