# Adding a New Dataset

Replace the NYC Taxi example with your own data. This requires changes to exactly 5 files.

---

## The 5-File Changeset

| # | File | What to change |
|---|------|---------------|
| 1 | `.env` | `TOPIC`, `DATA_PATH` |
| 2 | `flink/sql/01_source.sql.tmpl` | Field list matching your Parquet schema |
| 3 | `flink/sql/05_bronze.sql.tmpl` | Bronze table DDL + INSERT field list |
| 4 | `flink/sql/06_silver.sql.tmpl` | Silver table DDL, INSERT, dedup key, date filter |
| 5 | `dbt/models/staging/stg_<your_table>.sql` | Column renames/casts from Silver |

Optionally update `schemas/taxi_trip.json` for schema documentation, and add/replace seed CSVs.

---

## Step-by-Step Example: Ride-Share Data

Suppose your Parquet schema is:

```
ride_id         STRING
driver_id       INT
pickup_ts       TIMESTAMP
dropoff_ts      TIMESTAMP
distance_km     DOUBLE
fare_usd        DOUBLE
tip_usd         DOUBLE
status          STRING  # completed | cancelled | dispute
```

### 1. `.env`

```ini
TOPIC=rideshare.raw_rides
DATA_PATH=/data/rideshare_2024-01.parquet
```

### 2. `flink/sql/01_source.sql.tmpl`

Replace the field list in the Kafka source table DDL:

```sql
-- TODO: Replace with your schema fields
CREATE TABLE IF NOT EXISTS kafka_raw_rides (
    ride_id         STRING,
    driver_id       INT,
    pickup_ts       STRING,   -- keep as STRING from Kafka JSON
    dropoff_ts      STRING,
    distance_km     DOUBLE,
    fare_usd        DOUBLE,
    tip_usd         DOUBLE,
    status          STRING,
    kafka_timestamp TIMESTAMP(3) METADATA FROM 'timestamp',
    kafka_partition INT METADATA FROM 'partition',
    kafka_offset    BIGINT METADATA FROM 'offset'
) WITH (
    'connector' = 'kafka',
    'topic' = '${TOPIC}',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'flink-rideshare-consumer',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset'   -- remove for streaming
);

SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';
```

### 3. `flink/sql/05_bronze.sql.tmpl`

```sql
CREATE TABLE IF NOT EXISTS iceberg_catalog.bronze.raw_rides (
    ride_id         STRING,
    driver_id       INT,
    pickup_ts       STRING,
    dropoff_ts      STRING,
    distance_km     DOUBLE,
    fare_usd        DOUBLE,
    tip_usd         DOUBLE,
    status          STRING,
    kafka_timestamp TIMESTAMP(3),
    kafka_partition INT,
    kafka_offset    BIGINT
) WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'iceberg_catalog',
    'database-name' = 'bronze',
    'table-name' = 'raw_rides'
);

INSERT INTO iceberg_catalog.bronze.raw_rides
SELECT
    ride_id,
    driver_id,
    pickup_ts,
    dropoff_ts,
    distance_km,
    fare_usd,
    tip_usd,
    status,
    kafka_timestamp,
    kafka_partition,
    kafka_offset
FROM kafka_raw_rides;
```

### 4. `flink/sql/06_silver.sql.tmpl`

```sql
-- TODO: Choose your dedup key (ride_id in this example)
-- TODO: Adjust date range filter for your data
-- TODO: Add validation predicates (e.g. fare_usd >= 0)

CREATE TABLE IF NOT EXISTS iceberg_catalog.silver.cleaned_rides (
    ride_id         STRING,
    driver_id       INT,
    pickup_datetime TIMESTAMP(3),  -- parsed from STRING
    dropoff_datetime TIMESTAMP(3),
    pickup_date     DATE,           -- for PARTITIONED BY
    distance_km     DOUBLE,
    fare_usd        DOUBLE,
    tip_usd         DOUBLE,
    status          STRING
) PARTITIONED BY (pickup_date)     -- partition by DATE, not timestamp transform
WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'iceberg_catalog',
    'database-name' = 'silver',
    'table-name' = 'cleaned_rides'
);

INSERT INTO iceberg_catalog.silver.cleaned_rides
SELECT
    ride_id,
    driver_id,
    TO_TIMESTAMP(pickup_ts, 'yyyy-MM-dd HH:mm:ss') AS pickup_datetime,
    TO_TIMESTAMP(dropoff_ts, 'yyyy-MM-dd HH:mm:ss') AS dropoff_datetime,
    CAST(TO_TIMESTAMP(pickup_ts, 'yyyy-MM-dd HH:mm:ss') AS DATE) AS pickup_date,
    distance_km,
    fare_usd,
    tip_usd,
    status
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ride_id
               ORDER BY kafka_offset DESC
           ) AS row_num
    FROM iceberg_catalog.bronze.raw_rides
    WHERE
        pickup_ts IS NOT NULL
        AND dropoff_ts IS NOT NULL
        AND fare_usd >= 0
        AND distance_km >= 0
        AND TO_TIMESTAMP(pickup_ts, 'yyyy-MM-dd HH:mm:ss') >= TIMESTAMP '2024-01-01 00:00:00'
        AND TO_TIMESTAMP(pickup_ts, 'yyyy-MM-dd HH:mm:ss') <  TIMESTAMP '2024-02-01 00:00:00'
)
WHERE row_num = 1;
```

### 5. `dbt/models/staging/stg_rides.sql`

```sql
with source as (
    select * from {{ source('raw_rideshare', 'raw_rides') }}
),

renamed as (
    select
        ride_id,
        driver_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_date,
        distance_km,
        fare_usd,
        tip_usd,
        status
    from source
    where fare_usd >= 0
      and distance_km >= 0
)

select * from renamed
```

Update `dbt/models/sources/sources.yml`:

```yaml
sources:
  - name: raw_rideshare
    schema: silver
    tables:
      - name: raw_rides
        description: "Silver layer deduplicated rides from Iceberg"
        external_location: "iceberg_scan('s3://warehouse/silver/cleaned_rides', allow_moved_paths = true)"
```

---

## Checklist

- [ ] `.env` — `TOPIC` and `DATA_PATH` updated
- [ ] `01_source.sql.tmpl` — field list matches Parquet columns
- [ ] `05_bronze.sql.tmpl` — table name and INSERT match source
- [ ] `06_silver.sql.tmpl` — dedup key is your unique ID column; date range matches your data month
- [ ] Silver table includes an explicit DATE column for `PARTITIONED BY`
- [ ] `stg_<table>.sql` — column renames/casts from Silver
- [ ] `sources.yml` — external_location points to correct Silver path
- [ ] `make build-sql` — no unsubstituted `${VAR}` errors
- [ ] `make validate` — 4/4 PASS

---

## Common Gotchas

**Flink DDL: no transform partitioning**
```sql
-- WRONG: parser error "Encountered '('"
PARTITIONED BY (days(pickup_datetime))

-- RIGHT: explicit DATE column
pickup_date DATE,
...
PARTITIONED BY (pickup_date)
```

**Bronze table silent failure = empty Silver**
If Bronze CREATE fails (e.g. wrong field type), Silver INSERT finds 0 source rows.
dbt tests then PASS vacuously (0 rows = 0 nulls). Always run `make validate` to check actual counts.

**String timestamps from Kafka JSON**
Kafka JSON tends to deliver timestamps as strings. Parse them with `TO_TIMESTAMP(col, 'format')` in Silver, not in Bronze. Bronze is raw / pass-through.
