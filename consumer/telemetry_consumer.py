"""
telemetry_consumer.py

Kafka consumer pipeline:
  1. Read telemetry events from `device-telemetry` topic
  2. Run per-device sliding-window anomaly detection
  3. Batch-insert events into PostgreSQL (time-partitioned table)
  4. Publish anomaly alerts to `device-anomalies` topic

Kafka partition assignment: one consumer group member per partition.
Device events are keyed by device_id, so all readings for a given device
land on the same partition → sliding-window state stays local, no distributed
coordination needed.
"""

import json
import logging
import os
import signal
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException

# Add parent dir so anomaly_detector is importable inside Docker
sys.path.insert(0, "/app")
from anomaly_detector import SlidingWindowDetector, AnomalyResult

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("consumer")

#  Config ────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_GROUP    = os.getenv("KAFKA_CONSUMER_GROUP", "telemetry-pipeline")
TOPIC_IN          = os.getenv("KAFKA_TOPIC_IN", "device-telemetry")
TOPIC_ALERTS      = os.getenv("KAFKA_TOPIC_ALERTS", "device-anomalies")
WINDOW_SIZE       = int(os.getenv("ANOMALY_WINDOW_SIZE", "60"))
BATCH_SIZE        = int(os.getenv("DB_BATCH_SIZE", "200"))
FLUSH_INTERVAL_MS = int(os.getenv("FLUSH_INTERVAL_MS", "500"))

DB_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'telemetry')} "
    f"user={os.getenv('POSTGRES_USER', 'telemetry')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'telemetry')} "
    f"connect_timeout=10"
)

#  DB helpers ────────────────────────────────────────────────────────────────

def get_db_conn():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    return conn


def flush_events(conn, batch: list[dict]):
    if not batch:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO telemetry_events (
                device_id, model, firmware, network_type, event_timestamp,
                cpu_usage_pct, memory_usage_pct, signal_strength_dbm,
                throughput_mbps, battery_pct, event_type
            ) VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [
                (
                    e["device_id"], e["model"], e["firmware"], e["network_type"],
                    e["timestamp"],
                    e["cpu_usage_pct"], e["memory_usage_pct"], e["signal_strength_dbm"],
                    e["throughput_mbps"], e["battery_pct"], e["event_type"],
                )
                for e in batch
            ],
            page_size=BATCH_SIZE,
        )
    conn.commit()
    log.debug("Flushed %d events to DB", len(batch))


def flush_anomalies(conn, anomalies: list[AnomalyResult]):
    if not anomalies:
        return
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO anomaly_alerts (
                device_id, detected_at, metric, observed_value,
                rolling_mean, rolling_std_dev, z_score, severity
            ) VALUES %s
            """,
            [
                (
                    a.device_id, a.timestamp, a.metric, a.observed_value,
                    a.mean, a.std_dev, a.z_score, a.severity,
                )
                for a in anomalies
            ],
            page_size=500,
        )
    conn.commit()
    log.debug("Stored %d anomalies", len(anomalies))


#  Kafka helpers ─────────────────────────────────────────────────────────────

def build_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,          # manual commit after DB write
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 30_000,
        "fetch.min.bytes": 1024,
        "fetch.max.wait.ms": 100,
    })


def build_alert_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "1",
        "linger.ms": 10,
        "batch.size": 16384,
        "compression.type": "lz4",
    })


def publish_alert(producer: Producer, anomaly: AnomalyResult):
    payload = json.dumps({
        "device_id": anomaly.device_id,
        "timestamp": anomaly.timestamp,
        "metric": anomaly.metric,
        "observed_value": anomaly.observed_value,
        "mean": anomaly.mean,
        "std_dev": anomaly.std_dev,
        "z_score": anomaly.z_score,
        "severity": anomaly.severity,
    }).encode("utf-8")
    producer.produce(
        topic=TOPIC_ALERTS,
        key=anomaly.device_id.encode("utf-8"),
        value=payload,
    )
    producer.poll(0)


#  Main loop ─────────────────────────────────────────────────────────────────

def run():
    log.info("Consumer starting | group=%s, topic=%s", CONSUMER_GROUP, TOPIC_IN)

    consumer = build_consumer()
    alert_producer = build_alert_producer()
    detector = SlidingWindowDetector(window_size=WINDOW_SIZE)

    conn = get_db_conn()
    consumer.subscribe([TOPIC_IN])

    event_batch: list[dict] = []
    anomaly_batch: list[AnomalyResult] = []
    last_flush = time.monotonic()
    total_processed = 0
    total_anomalies = 0

    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutdown signal — draining pipeline...")
        running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        while running:
            msg = consumer.poll(timeout=0.1)

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    pass
                else:
                    log.error("Kafka error: %s", msg.error())
            else:
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as e:
                    log.warning("Malformed message skipped: %s", e)
                    continue

                # Anomaly detection (in-memory, no I/O)
                detected = detector.process(event)
                event_batch.append(event)
                anomaly_batch.extend(detected)
                total_processed += 1

                if detected:
                    for a in detected:
                        publish_alert(alert_producer, a)
                        log.warning(
                            "ANOMALY | device=%s metric=%s z=%.2f severity=%s",
                            a.device_id, a.metric, a.z_score, a.severity,
                        )

            # Flush to DB on batch size or time interval
            elapsed_ms = (time.monotonic() - last_flush) * 1000
            if len(event_batch) >= BATCH_SIZE or elapsed_ms >= FLUSH_INTERVAL_MS:
                flush_events(conn, event_batch)
                flush_anomalies(conn, anomaly_batch)
                consumer.commit(asynchronous=False)   # commit after successful DB write
                total_anomalies += len(anomaly_batch)
                log.info(
                    "Flushed | events=%d anomalies=%d | total_processed=%d total_anomalies=%d devices_tracked=%d",
                    len(event_batch), len(anomaly_batch),
                    total_processed, total_anomalies,
                    detector.device_count(),
                )
                event_batch.clear()
                anomaly_batch.clear()
                last_flush = time.monotonic()

    except KafkaException as e:
        log.error("Fatal Kafka error: %s", e)
    finally:
        # Final flush on shutdown
        flush_events(conn, event_batch)
        flush_anomalies(conn, anomaly_batch)
        consumer.commit(asynchronous=False)
        alert_producer.flush(timeout=10)
        consumer.close()
        conn.close()
        log.info("Consumer shut down. Total processed: %d, Total anomalies: %d", total_processed, total_anomalies)


if __name__ == "__main__":
    run()
