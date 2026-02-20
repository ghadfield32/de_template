# Environment Profiles

This folder contains named environment profiles for the 4 configuration axes.
The active profile is always loaded from `.env` at the repo root (gitignored).

## Available Profiles

| File              | BROKER   | STORAGE  | Description                       |
|-------------------|----------|----------|-----------------------------------|
| `local.env`       | redpanda | minio    | Default local dev (no cloud creds)|
| `local_kafka.env` | kafka    | minio    | Local dev with Apache Kafka 4.0   |
| `aws.env`         | kafka    | aws_s3   | AWS S3 (IAM-first, no hardcoded keys) |
| `gcs.env`         | kafka    | gcs      | Google Cloud Storage via HMAC     |
| `azure.env`       | kafka    | azure    | Azure Blob via S3-compat layer    |

## How to Select a Profile

```bash
# Copy a profile to the active .env
make env-select ENV=env/local.env

# Or manually:
cp env/local.env .env
```

## Customising a Profile

1. Copy the closest profile: `cp env/local.env env/my_project.env`
2. Edit `env/my_project.env` with your topic name, data path, and credentials
3. Activate: `make env-select ENV=env/my_project.env`
4. Verify: `make print-config`

## Environment Variable Reference

| Variable            | Required | Default            | Notes                                         |
|---------------------|----------|--------------------|-----------------------------------------------|
| `BROKER`            | yes      | `redpanda`         | `redpanda` or `kafka`                         |
| `CATALOG`           | yes      | `hadoop`           | `hadoop` or `rest`                            |
| `STORAGE`           | yes      | `minio`            | `minio`, `aws_s3`, `gcs`, or `azure`          |
| `MODE`              | yes      | `batch`            | Flink pipeline mode: `batch` or `streaming_bronze` |
| `GENERATOR_MODE`    | no       | `burst`            | Generator send mode: `burst`, `realtime`, or `batch` |
| `TOPIC`             | yes      | —                  | Primary Kafka topic                           |
| `DLQ_TOPIC`         | yes      | —                  | Dead-letter topic                             |
| `DATA_PATH`         | yes      | —                  | Container-internal Parquet path               |
| `WAIT_FOR_SILVER_MIN_ROWS` | no | `1`              | Readiness threshold: wait until Silver rows >= this value |
| `WAREHOUSE`         | yes      | `s3a://warehouse/` | Iceberg warehouse root (`s3a://` prefix)      |
| `S3_ENDPOINT`       | minio    | —                  | Required for `STORAGE=minio`                  |
| `DUCKDB_S3_ENDPOINT`| yes      | —                  | No `http://` prefix (DuckDB httpfs format)    |
| `AWS_ACCESS_KEY_ID` | minio    | —                  | MinIO root user or AWS key                    |
| `AWS_SECRET_ACCESS_KEY` | minio| —                  | MinIO root password or AWS secret             |
