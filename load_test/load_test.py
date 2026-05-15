"""
load_test.py

Measures producer throughput and end-to-end latency for the telemetry pipeline.
Run this while docker compose is up.

Usage:
    pip install confluent-kafka requests
    python load_test/load_test.py

Output: throughput (events/sec), p50/p95/p99 delivery latency
"""

import json
import time
import statistics
import os
import requests
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "device-telemetry"
API_BASE = "http://localhost:8000"
TEST_EVENTS = 10_000
WARMUP_EVENTS = 500


def make_event(i: int) -> dict:
    return {
        "device_id": f"DEV-{(i % 50):05d}",
        "model": "SD8Gen3",
        "firmware": "v14.2.1",
        "network_type": "5G",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "cpu_usage_pct": 45.0 + (i % 30),
        "memory_usage_pct": 55.0,
        "signal_strength_dbm": -72.0,
        "throughput_mbps": 180.0,
        "battery_pct": 80.0,
        "event_type": "normal",
    }


def run_throughput_test():
    print(f"\n── Producer Throughput Test ({TEST_EVENTS:,} events) ──")
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "linger.ms": 5,
        "batch.size": 65536,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })

    # Warmup
    for i in range(WARMUP_EVENTS):
        producer.produce(TOPIC, key=f"DEV-{i % 50:05d}".encode(), value=b"{}")
    producer.flush()

    delivery_latencies = []

    def on_delivery(err, msg):
        if err is None:
            delivery_latencies.append(time.monotonic())

    start = time.monotonic()
    send_times = []

    for i in range(TEST_EVENTS):
        payload = json.dumps(make_event(i)).encode("utf-8")
        send_times.append(time.monotonic())
        producer.produce(
            TOPIC,
            key=f"DEV-{i % 50:05d}".encode(),
            value=payload,
            callback=on_delivery,
        )
        producer.poll(0)

    producer.flush(timeout=30)
    elapsed = time.monotonic() - start
    throughput = TEST_EVENTS / elapsed

    print(f"  Events sent:      {TEST_EVENTS:,}")
    print(f"  Elapsed:          {elapsed:.2f}s")
    print(f"  Throughput:       {throughput:,.0f} events/sec")

    if delivery_latencies and send_times:
        latencies_ms = [
            (delivery_latencies[i] - send_times[i]) * 1000
            for i in range(min(len(delivery_latencies), len(send_times)))
        ]
        latencies_ms.sort()
        print(f"  Delivery p50:     {statistics.median(latencies_ms):.1f}ms")
        print(f"  Delivery p95:     {latencies_ms[int(len(latencies_ms) * 0.95)]:.1f}ms")
        print(f"  Delivery p99:     {latencies_ms[int(len(latencies_ms) * 0.99)]:.1f}ms")


def run_api_test():
    print("\n── API Latency Test ──")
    endpoints = [
        ("/health", {}),
        ("/stats/summary", {}),
        ("/anomalies/recent", {"limit": 50}),
        ("/devices", {"limit": 50}),
        ("/devices/DEV-00001/metrics", {"since_minutes": 5}),
    ]

    for path, params in endpoints:
        times = []
        for _ in range(20):
            t0 = time.monotonic()
            try:
                r = requests.get(f"{API_BASE}{path}", params=params, timeout=5)
                elapsed = (time.monotonic() - t0) * 1000
                if r.status_code == 200:
                    times.append(elapsed)
            except requests.RequestException:
                print(f"  {path}: unreachable")
                break
        if times:
            print(f"  {path:<40} p50={statistics.median(times):.1f}ms  p95={sorted(times)[int(len(times)*0.95)]:.1f}ms")


if __name__ == "__main__":
    print("Kafka Telemetry Pipeline — Load Test")
    print("Make sure docker compose is running before starting.")
    run_throughput_test()
    run_api_test()
    print("\nDone.")
