"""Taxi trip event generator.

Reads NYC Yellow Taxi parquet data and produces events to a Kafka-compatible
broker (Kafka or Redpanda). Supports three modes:
  - burst:    As fast as possible (benchmarking)
  - realtime: Simulates actual event-time spacing
  - batch:    Sends events in configurable batch sizes with delays

Configuration via environment variables:
  BROKER_URL       Kafka/Redpanda bootstrap servers  (default: localhost:9092)
  TOPIC            Target topic name                  (default: taxi.raw_trips)
  DLQ_TOPIC        Dead Letter Queue topic name       (default: taxi.raw_trips.dlq)
  MODE             burst | realtime | batch           (default: burst)
  RATE_LIMIT       Max events/sec in burst mode, 0=unlimited (default: 0)
  BATCH_SIZE       Events per batch in batch mode     (default: 1000)
  BATCH_DELAY      Seconds between batches            (default: 1.0)
  DATA_PATH        Path to parquet file               (default: /data/yellow_tripdata_2024-01.parquet)
  MAX_EVENTS       Stop after N events, 0=all         (default: 0)
  METRICS_PATH     Write JSON metrics to this path    (default: /tmp/generator_metrics.json)
  VALIDATE_SCHEMA  Validate rows against JSON Schema before producing (default: false)
  SCHEMA_PATH      Path to JSON Schema file            (default: /schemas/taxi_trip.json)

Usage:
    python generator.py
    python generator.py --mode burst --broker localhost:9092
"""

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime

import orjson
import pyarrow.parquet as pq
from confluent_kafka import Producer


def delivery_callback(err, msg):
    if err is not None:
        print(f"  [ERROR] Delivery failed: {err}", file=sys.stderr)


def load_schema(schema_path: str) -> dict | None:
    """Load a JSON Schema from disk. Returns None if file not found."""
    try:
        with open(schema_path) as f:
            return json.load(f)
    except FileNotFoundError:
        print(
            f"  [WARN] Schema file not found: {schema_path} — validation skipped", file=sys.stderr
        )
        return None


def make_validator(schema: dict):
    """Build a jsonschema validator. Returns None if jsonschema not importable."""
    try:
        import jsonschema

        return jsonschema.Draft7Validator(schema)
    except ImportError:
        print("  [WARN] jsonschema not installed — validation skipped", file=sys.stderr)
        return None


def read_parquet(path: str, max_events: int = 0):
    """Yield rows from parquet file as dicts."""
    table = pq.read_table(path)
    total = table.num_rows if max_events == 0 else min(max_events, table.num_rows)
    print(f"  Source: {path} ({table.num_rows:,} rows, sending {total:,})")

    batches = table.to_batches(max_chunksize=10_000)
    sent = 0
    for batch in batches:
        for row in batch.to_pylist():
            if sent >= total:
                return
            # Convert timestamps to ISO strings for JSON serialization
            for key, val in row.items():
                if isinstance(val, datetime):
                    row[key] = val.isoformat()
            yield row
            sent += 1


def produce_to_dlq(producer: Producer, dlq_topic: str, row: dict, error: str) -> None:
    """Send a rejected row to the Dead Letter Queue with error context."""
    envelope = {
        "error": error,
        "row": row,
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }
    producer.produce(dlq_topic, value=orjson.dumps(envelope), callback=delivery_callback)


def create_producer(broker_url: str) -> Producer:
    conf = {
        "bootstrap.servers": broker_url,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 500000,
        "queue.buffering.max.kbytes": 1048576,
        "compression.type": "lz4",
    }
    return Producer(conf)


def produce_burst(
    producer: Producer,
    topic: str,
    dlq_topic: str,
    rows,
    rate_limit: int,
    validator,
) -> tuple[int, int, float, float]:
    """Produce as fast as possible, optionally rate-limited.

    Returns (valid_count, invalid_count, elapsed_seconds, rate_per_sec).
    """
    count = 0
    invalid = 0
    start = time.perf_counter()
    last_report = start

    for row in rows:
        if validator is not None:
            errors = list(validator.iter_errors(row))
            if errors:
                invalid += 1
                produce_to_dlq(producer, dlq_topic, row, errors[0].message)
                producer.poll(0)
                continue

        key = str(row.get("PULocationID", "")).encode("utf-8")
        value = orjson.dumps(row)
        producer.produce(topic, value=value, key=key, callback=delivery_callback)
        count += 1

        if count % 10000 == 0:
            producer.poll(0)
            now = time.perf_counter()
            if now - last_report >= 5.0:
                elapsed = now - start
                rate = count / elapsed
                print(f"  Produced {count:,} events ({rate:,.0f} evt/s)")
                last_report = now

        # Rate limiting
        if rate_limit > 0 and count % rate_limit == 0:
            elapsed = time.perf_counter() - start
            expected = count / rate_limit
            if elapsed < expected:
                time.sleep(expected - elapsed)

    producer.flush(timeout=30)
    elapsed = time.perf_counter() - start
    rate = count / elapsed if elapsed > 0 else 0
    return count, invalid, elapsed, rate


def produce_batch(
    producer: Producer,
    topic: str,
    dlq_topic: str,
    rows,
    batch_size: int,
    batch_delay: float,
    validator,
) -> tuple[int, int, float, float]:
    """Produce in fixed-size batches with delays between them.

    Returns (valid_count, invalid_count, elapsed_seconds, rate_per_sec).
    """
    count = 0
    invalid = 0
    batch_count = 0
    start = time.perf_counter()

    batch_buffer = []
    for row in rows:
        if validator is not None:
            errors = list(validator.iter_errors(row))
            if errors:
                invalid += 1
                produce_to_dlq(producer, dlq_topic, row, errors[0].message)
                continue

        batch_buffer.append(row)
        if len(batch_buffer) >= batch_size:
            for r in batch_buffer:
                key = str(r.get("PULocationID", "")).encode("utf-8")
                value = orjson.dumps(r)
                producer.produce(topic, value=value, key=key, callback=delivery_callback)
                count += 1
            producer.flush(timeout=30)
            batch_count += 1
            elapsed = time.perf_counter() - start
            rate = count / elapsed if elapsed > 0 else 0
            print(f"  Batch {batch_count}: {count:,} total ({rate:,.0f} evt/s)")
            batch_buffer = []
            time.sleep(batch_delay)

    # Final partial batch
    if batch_buffer:
        for r in batch_buffer:
            key = str(r.get("PULocationID", "")).encode("utf-8")
            value = orjson.dumps(r)
            producer.produce(topic, value=value, key=key, callback=delivery_callback)
            count += 1
        producer.flush(timeout=30)

    elapsed = time.perf_counter() - start
    rate = count / elapsed if elapsed > 0 else 0
    return count, invalid, elapsed, rate


def main():
    parser = argparse.ArgumentParser(description="Taxi trip event generator")
    parser.add_argument("--broker", default=os.environ.get("BROKER_URL", "localhost:9092"))
    parser.add_argument("--topic", default=os.environ.get("TOPIC", "taxi.raw_trips"))
    parser.add_argument("--dlq-topic", default=os.environ.get("DLQ_TOPIC", "taxi.raw_trips.dlq"))
    parser.add_argument(
        "--mode", default=os.environ.get("MODE", "burst"), choices=["burst", "realtime", "batch"]
    )
    parser.add_argument("--rate-limit", type=int, default=int(os.environ.get("RATE_LIMIT", "0")))
    parser.add_argument("--batch-size", type=int, default=int(os.environ.get("BATCH_SIZE", "1000")))
    parser.add_argument(
        "--batch-delay", type=float, default=float(os.environ.get("BATCH_DELAY", "1.0"))
    )
    parser.add_argument(
        "--data-path", default=os.environ.get("DATA_PATH", "/data/yellow_tripdata_2024-01.parquet")
    )
    parser.add_argument("--max-events", type=int, default=int(os.environ.get("MAX_EVENTS", "0")))
    parser.add_argument(
        "--validate-schema",
        action="store_true",
        default=os.environ.get("VALIDATE_SCHEMA", "false").lower() == "true",
    )
    parser.add_argument(
        "--schema-path", default=os.environ.get("SCHEMA_PATH", "/schemas/taxi_trip.json")
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Taxi Trip Event Generator")
    print("=" * 60)
    print(f"  Broker:     {args.broker}")
    print(f"  Topic:      {args.topic}")
    print(f"  DLQ Topic:  {args.dlq_topic}")
    print(f"  Mode:       {args.mode}")
    print(f"  Data:       {args.data_path}")
    max_events_str = "all" if args.max_events == 0 else f"{args.max_events:,}"
    print(f"  Max events: {max_events_str}")
    if args.validate_schema:
        print(f"  Schema:     {args.schema_path}  [validation ON]")
    else:
        print("  Schema:     [validation OFF — set VALIDATE_SCHEMA=true to enable]")
    print()

    # Schema validation setup (opt-in)
    validator = None
    if args.validate_schema:
        schema = load_schema(args.schema_path)
        if schema is not None:
            validator = make_validator(schema)
            if validator is not None:
                print("  [OK] Schema validator loaded")

    producer = create_producer(args.broker)
    rows = read_parquet(args.data_path, args.max_events)

    if args.mode == "burst":
        count, invalid, elapsed, rate = produce_burst(
            producer, args.topic, args.dlq_topic, rows, args.rate_limit, validator
        )
    elif args.mode == "batch":
        count, invalid, elapsed, rate = produce_batch(
            producer, args.topic, args.dlq_topic, rows, args.batch_size, args.batch_delay, validator
        )
    else:
        # realtime mode: burst with rate limiting to approximate real-time
        count, invalid, elapsed, rate = produce_burst(
            producer, args.topic, args.dlq_topic, rows, rate_limit=5000, validator=validator
        )

    print()
    print("=" * 60)
    print("  GENERATOR COMPLETE")
    print(f"  Events produced: {count:,}")
    if validator is not None:
        print(f"  Invalid (→ DLQ): {invalid:,}")
    print(f"  Elapsed: {elapsed:.2f}s")
    print(f"  Rate:    {rate:,.0f} events/sec")
    print("=" * 60)

    # Write metrics for validate.sh data-derived count checks
    metrics_path = os.environ.get("METRICS_PATH", "/tmp/generator_metrics.json")
    metrics = {
        "events": count,
        "invalid": invalid,
        "elapsed_seconds": round(elapsed, 3),
        "events_per_second": round(rate, 1),
        "mode": args.mode,
        "broker": args.broker,
        "topic": args.topic,
        "schema_validation": args.validate_schema,
    }
    with open(metrics_path, "wb") as f:
        f.write(orjson.dumps(metrics))
    print(f"  Metrics written to {metrics_path}")


if __name__ == "__main__":
    main()
