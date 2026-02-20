# Observability

What to watch, where to watch it, and how to react.

## Start Observability Stack

```bash
make up
make obs-up
```

Endpoints:
- Flink dashboard: `http://localhost:8081`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (`admin/admin`)

## Key Signals

## 1) Flink Job Health

Watch:
- job state (RUNNING/FINISHED as expected for mode)
- restart count
- checkpoint success/failure trend

Useful command:

```bash
make flink-jobs
```

## 2) Broker Lag and DLQ

Watch:
- consumer lag trend
- DLQ message count

Useful command:

```bash
make check-lag
```

## 3) Iceberg Readiness and Data Movement

Watch:
- Bronze/Silver row progression
- Silver readiness gate behavior
- metadata errors from `count_iceberg.py`

Useful command:

```bash
make wait-for-silver
make validate
```

## 4) dbt Quality Gate

Watch:
- dbt test pass/fail

Useful command:

```bash
make dbt-test
```

## Useful Debug Order

```bash
make doctor
make health
make check-lag
make validate
make logs-flink
make logs-flink-tm
```

## Optional Maintenance Observability

Streaming and frequent runs can produce many small files/snapshots. Monitor maintenance cadence:

```bash
make compact-silver
make expire-snapshots
make vacuum
```

## Alerting Ideas (Platform-Agnostic)

- Flink checkpoint failures > 0 for N minutes
- Flink job restarts > 0
- DLQ messages > `DLQ_MAX`
- Silver rows below expected threshold after processing window
- dbt test failures
