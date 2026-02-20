# Production Deployment Notes

This document captures the main changes when moving from local Docker usage to production infrastructure.

## 1) Deployment Philosophy

Local template defaults optimize for speed of iteration.
Production should optimize for:
- reliability
- observability
- security
- controlled change rollout

## 2) Broker Strategy

Local:
- in-cluster broker via `infra/broker.*.yml`

Production:
- managed Kafka/compatible broker (MSK, Confluent, Aiven, etc.)
- topic management via IaC or platform tooling, not `make create-topics`

What to adjust:
- broker bootstrap addresses
- TLS/SASL auth properties in source SQL template
- secrets handling via secret manager / runtime identity

## 3) Storage Strategy

Local:
- MinIO (`STORAGE=minio`)

Production:
- cloud object storage (`aws_s3`, `gcs`, `azure`)

What to adjust:
- `WAREHOUSE`
- endpoint/SSL/path-style configuration
- runtime credentials (IAM roles/workload identity/managed identity)

## 4) Catalog Strategy

Local default:
- file-based (Hadoop) catalog

Production recommendation:
- REST catalog for multi-engine and multi-writer safety

If using Lakekeeper in this repo, run it explicitly with its compose overlay:

```bash
docker compose \
  -f infra/base.yml \
  -f infra/broker.redpanda.yml \
  -f infra/storage.minio.yml \
  -f infra/catalog.rest-lakekeeper.yml \
  --profile catalog-rest up -d
```

## 5) Flink Runtime

Local:
- Docker compose services

Production:
- Kubernetes + Flink Operator (or managed Flink)

Recommended production controls:
- checkpoint and savepoint lifecycle
- autoscaling / resource classes
- alerting on checkpoint failure and restart loops

## 6) dbt Runtime

Local:
- `make dbt-build`

Production:
- orchestrator-managed dbt execution (Airflow, Dagster, Prefect, dbt Cloud)
- environment-specific credentials injected at runtime

## 7) Secrets and Auth

Do not ship real secrets in `.env`.

Use:
- AWS Secrets Manager / SSM
- GCP Secret Manager
- Azure Key Vault
- Vault
- Kubernetes secrets + workload identity

## 8) Operational Guardrails to Keep

Retain these template protections in production workflows:
- `make doctor`-equivalent preflight checks
- strict SQL rendering (no unresolved placeholders)
- explicit readiness gating (`WAIT_FOR_SILVER_*`)
- staged validation (`make validate` logic)
- DLQ thresholds (`DLQ_MAX`)

## 9) Suggested Rollout Sequence

1. Run local path to green.
2. Move storage to cloud profile.
3. Move broker to external managed endpoint.
4. Introduce REST catalog.
5. Move Flink/dbt to orchestrated runtime.
6. Add alerting and SLOs.
