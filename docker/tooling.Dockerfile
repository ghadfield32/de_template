FROM python:3.12-slim

# gcc + librdkafka-dev required to compile the confluent-kafka C extension.
# If confluent-kafka ever ships a pure-Python wheel these can be removed.
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# uv — fast Python package installer. Single static binary, no extra deps.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency spec BEFORE application code.
# Docker layer caching: changing generator.py won't invalidate the dep layer.
COPY pyproject.toml uv.lock ./

# Install:
#   [project.dependencies]          — pydantic, pyyaml, jinja2 (shared base)
#   [dependency-groups.container]   — pyarrow, confluent-kafka, dbt-core, duckdb, etc.
#   [dependency-groups.data-connectors] — API clients / DB drivers (empty by default)
# Skip dev group (pytest, ruff, pyright — not needed in the container).
# --frozen: use uv.lock exactly as committed, no re-resolution.
RUN uv sync \
    --group container \
    --group data-connectors \
    --no-group dev \
    --frozen \
    --no-install-project

# Generator script is the only file baked in.
# scripts/, config/, datasets/, dbt/ are all mounted at runtime via volumes.
COPY generator/generator.py ./

# Activate the uv-managed venv so `python` resolves to the installed packages.
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT []
CMD ["python"]
