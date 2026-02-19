FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY generator.py .

# Default environment (overridden by docker-compose environment: block)
ENV BROKER_URL=broker:9092
ENV TOPIC=taxi.raw_trips
ENV MODE=burst
ENV DATA_PATH=/data/yellow_tripdata_2024-01.parquet
ENV MAX_EVENTS=0
ENV METRICS_PATH=/metrics/latest.json
# Schema validation (opt-in): set VALIDATE_SCHEMA=true + SCHEMA_PATH to enable
ENV VALIDATE_SCHEMA=false
ENV SCHEMA_PATH=/schemas/taxi_trip.json

ENTRYPOINT ["python", "generator.py"]
