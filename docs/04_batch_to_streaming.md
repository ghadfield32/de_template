# Batch to Streaming Bronze

How to switch from bounded batch processing to continuous Bronze ingestion.

## Behavior Differences

| Aspect | `MODE=batch` | `MODE=streaming_bronze` |
|---|---|---|
| Bronze job | Finite run | Long-running job |
| Stage 2 validation | expects `FINISHED` jobs | expects `RUNNING` jobs |
| Operational load | lower | higher |
| Latency | higher | lower |

## Migration Steps

1. Set `MODE=streaming_bronze` in your active env profile.
2. Re-activate profile:

```bash
make env-select ENV=env/local.env
```

3. Start infra and topics:

```bash
make up
make create-topics
```

4. Start continuous Bronze:

```bash
make process-streaming
```

5. Produce events:

```bash
make generate-limited
# or make generate
```

6. Run Silver on demand:

```bash
make process-silver
make wait-for-silver
make dbt-build
make validate
```

## Operating Streaming

```bash
make flink-jobs
make check-lag
make flink-cancel JOB=<job-id>
make flink-restart-streaming
```

Validation in streaming mode requires:
- at least one RUNNING Flink job
- zero restarts for the running job check to pass

## Switch Back to Batch

1. Set `MODE=batch` in env profile.
2. Re-activate and run:

```bash
make env-select ENV=env/local.env
make process
```
