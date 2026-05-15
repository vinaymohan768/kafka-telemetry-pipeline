# kafka-telemetry-pipeline

Real-time device telemetry pipeline built with Apache Kafka, Python, PostgreSQL, and FastAPI. Simulates a fleet of 50 devices sending telemetry events, detects anomalies using a sliding-window Z-score algorithm, and exposes queryable metrics via a REST API.

**Sustained throughput:** 500 events/sec · **End-to-end latency:** sub-100ms · **Anomaly detection:** per-device, stateless, O(1) per event

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Device Fleet (50 devices)                    │
│           Snapdragon models · 5G/LTE · CPU, signal, throughput       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ keyed by device_id (partition affinity)
                               ▼
                    ┌─────────────────────┐
                    │   Kafka Broker      │
                    │   device-telemetry  │  6 partitions · lz4 compressed
                    │   device-anomalies  │  3 partitions · 7-day retention
                    └──────────┬──────────┘
                               │
                               ▼
              ┌────────────────────────────────┐
              │       Consumer Pipeline         │
              │                                 │
              │  1. Deserialize JSON event      │
              │  2. Sliding-window detector     │  per-device Welford state
              │     (cpu, signal, throughput,   │  Z-score threshold: 2.0–3.0
              │      memory)                    │
              │  3. Batch insert → PostgreSQL   │  200 events / 500ms flush
              │  4. Publish alerts → Kafka      │  if anomaly detected
              └──────────┬────────────┬─────────┘
                         │            │
              ┌──────────▼──┐    ┌────▼─────────────────┐
              │  PostgreSQL  │    │  device-anomalies     │
              │              │    │  Kafka topic          │
              │  telemetry_  │    │  (downstream alerts,  │
              │  events      │    │   monitoring systems) │
              │  (partitioned│    └──────────────────────┘
              │  by month)   │
              │              │
              │  anomaly_    │
              │  alerts      │
              └──────┬───────┘
                     │
              ┌──────▼───────┐
              │  FastAPI      │
              │  Query API    │  :8000
              │               │
              │  /devices     │
              │  /metrics     │
              │  /anomalies   │
              │  /stats       │
              └──────────────┘
```

---

## Key Design Decisions

**Kafka partitioning by device_id**  Events for the same device always land on the same partition. The consumer maintains sliding-window state in memory per device with no distributed coordination, no Redis, no shared state. This is the standard pattern for stateful stream processing without a full Flink/Spark deployment.

**Welford's online algorithm**  Rolling mean and variance computed in O(1) per event using Welford's method. No full window scan on each update. Supports online removal of evicted values for true sliding-window behavior. See [`anomaly_detector/sliding_window.py`](anomaly_detector/sliding_window.py).

**Manual Kafka offset commit**  Consumer commits offsets only after a successful PostgreSQL batch write. No event is lost between a Kafka commit and a failed DB write. This trades some throughput for at-least-once delivery semantics.

**PostgreSQL range partitioning**  `telemetry_events` is partitioned by month. Queries filtered by time range scan only the relevant partition(s). Composite B-tree index on `(device_id, event_timestamp DESC)` covers the primary access pattern: "give me the last N readings for device X."

**Partial index on critical events**  A separate partial index on `event_type = 'critical'` keeps critical-event queries fast without bloating the main index.

---

## Anomaly Detection

The detector runs a **per-device, per-metric sliding window** of the last 60 readings (~6 seconds at 10 events/device/sec). It uses Z-score thresholds tuned per metric:

| Metric | Warning (σ) | Critical (σ) | Rationale |
|---|---|---|---|
| `cpu_usage_pct` | 2.0 | 3.0 | Sharp spikes indicate runaway processes |
| `signal_strength_dbm` | 1.8 | 2.5 | Network degradation appears as sustained drift |
| `throughput_mbps` | 1.8 | 2.5 | Low threshold catches gradual throughput erosion |
| `memory_usage_pct` | 2.2 | 3.2 | Memory grows slower — higher threshold reduces noise |

The window requires at least 30 readings before flagging anomalies (warmup guard). Approximately 2% of simulated events are injected anomalies  CPU spikes, signal drops, or throughput crashes.

---

## Getting Started

**Prerequisites:** Docker, Docker Compose

```bash
git clone https://github.com/vinaymohan768/kafka-telemetry-pipeline
cd kafka-telemetry-pipeline
docker compose up --build
```

That's it. All services start in dependency order: Kafka → topic creation → PostgreSQL → consumer/producer → API.

**Check the pipeline is running:**

```bash
# API health
curl http://localhost:8000/health

# Pipeline stats after ~30 seconds
curl http://localhost:8000/stats/summary

# Recent anomalies
curl http://localhost:8000/anomalies/recent?severity=critical&limit=10

# Metrics for a specific device
curl "http://localhost:8000/devices/DEV-00001/metrics?since_minutes=5"
```

**Swagger UI:** http://localhost:8000/docs

---

## Project Structure

```
kafka-telemetry-pipeline/
├   producer/
│   ├ telemetry_producer.py     # Multi-device event simulator
│   ├ requirements.txt
│   └ Dockerfile
├   consumer/
│   ├ telemetry_consumer.py     # Kafka consumer + DB writer + alert publisher
│   ├ requirements.txt
│   └ Dockerfile
├   anomaly_detector/
│   ├ __init__.py
│   └ sliding_window.py         # Welford's online algorithm, Z-score detection
├   api/
│   ├ main.py                   # FastAPI query endpoints
│   ├ requirements.txt
│   └ Dockerfile
├   db/
│   └── init.sql                  # Schema: partitioned tables, indexes, rollup view
├   load_test/
│   └ load_test.py              # Throughput and latency benchmarks
└   docker-compose.yml
```

---

## Performance

Benchmarked on a single machine (MacBook M2, 16GB RAM):

| Metric | Result |
|---|---|
| Producer throughput | 500 events/sec sustained |
| Consumer processing | < 2ms per event (anomaly detection + deserialization) |
| DB batch write (200 events) | 8–15ms |
| End-to-end latency (produce → DB) | 60–95ms (p99) |
| API response  device metrics | 12–30ms |
| API response  recent anomalies | 8–20ms |

---

## Configuration

All services are configured via environment variables. Override in `docker-compose.yml` or pass directly:

| Variable | Default | Description |
|---|---|---|
| `DEVICE_COUNT` | 50 | Number of simulated devices |
| `EVENTS_PER_SECOND` | 500 | Target producer throughput |
| `ANOMALY_WINDOW_SIZE` | 60 | Sliding window size per device per metric |
| `DB_BATCH_SIZE` | 200 | Events per PostgreSQL batch insert |
| `FLUSH_INTERVAL_MS` | 500 | Max time between DB flushes |

---

## Tech Stack

`Python 3.11` `Apache Kafka` `PostgreSQL 16` `FastAPI` `confluent-kafka` `psycopg2` `Docker Compose`
