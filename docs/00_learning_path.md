# Learning Path

This is the fastest practical sequence for first-time users.

## 1) Decide Your Path

Start with this default unless you have a strong reason not to:
- `env/local.env`
- `MODE=batch`
- `GENERATOR_MODE=burst`

When to choose something else:
- Need Kafka parity: `env/local_kafka.env`
- Need continuous Bronze ingestion: `MODE=streaming_bronze`
- Need cloud storage parity: `env/aws.env`, `env/gcs.env`, or `env/azure.env`

## 2) Run the Linear Setup

From repo root:

```bash
make setup-dev
make env-select ENV=env/local.env
mkdir -p ../data
cp /path/to/your_file.parquet ../data/
make doctor
make up
make create-topics
make generate-limited
make process
make wait-for-silver
make dbt-build
make validate
```

## 3) Understand What Just Happened

- Bronze loaded raw records to Iceberg.
- Silver applied dedup/quality/transforms.
- dbt built downstream models.
- `make validate` checked 4 stages: infra, Flink jobs, Iceberg counts, dbt tests.

## 4) Move to Your Dataset

```bash
mkdir -p datasets/orders
cp datasets/taxi/dataset.yml datasets/orders/dataset.yml
# edit datasets/orders/dataset.yml
make scaffold-validate DATASET=orders
make scaffold DATASET=orders
make build-sql
make show-sql
```

## 5) Move to Streaming (Optional)

```bash
# set MODE=streaming_bronze in active env profile
make env-select ENV=env/local.env
make up
make create-topics
make process-streaming
make generate-limited
make process-silver
make validate
```

## 6) Keep It Healthy

Use these first when debugging:

```bash
make doctor
make print-config
make health
make validate
```

For deeper details, use:
- `docs/02_local_quickstart.md`
- `docs/03_add_new_dataset.md`
- `docs/04_batch_to_streaming.md`
