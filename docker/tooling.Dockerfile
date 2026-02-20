FROM python:3.12-slim

# gcc + librdkafka-dev required by confluent-kafka C extension
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir \
    "pyarrow>=14.0.0" \
    "confluent-kafka>=2.3.0" \
    "orjson>=3.9.0" \
    "jsonschema>=4.0.0" \
    "dbt-core>=1.8" \
    "dbt-duckdb>=1.8.0,<2.0.0" \
    duckdb \
    pandas \
    pyyaml \
    jinja2 \
    "pydantic>=2" \
    "pydantic-settings>=2"

# Generator script is the only file baked in.
# scripts/, config/, datasets/, dbt/ are all mounted at runtime via volumes.
COPY generator/generator.py /app/generator.py

ENTRYPOINT []
CMD ["python"]
