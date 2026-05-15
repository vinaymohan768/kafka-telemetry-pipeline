"""
telemetry_producer.py
Simulates a fleet of devices sending telemetry events to Kafka.
Each event models a Snapdragon-style device: CPU, memory, signal strength,
throughput, and battery. Configurable device count and event rate.
"""

import json
import time
import random
import logging
import os
import signal
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from confluent_kafka import Producer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "device-telemetry")
DEVICE_COUNT = int(os.getenv("DEVICE_COUNT", "50"))
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "500"))


@dataclass
class DeviceProfile:
    device_id: str
    model: str
    firmware: str
    network_type: str
    # Baseline ranges — used to generate realistic drift
    cpu_base: float
    mem_base: float
    signal_base: float


@dataclass
class TelemetryEvent:
    device_id: str
    model: str
    firmware: str
    network_type: str
    timestamp: str
    cpu_usage_pct: float       # 0–100
    memory_usage_pct: float    # 0–100
    signal_strength_dbm: float # -120 to -40 dBm
    throughput_mbps: float     # 0–1000
    battery_pct: float         # 0–100
    event_type: str            # "normal" | "degraded" | "critical"


def make_device_fleet(count: int) -> list[DeviceProfile]:
    models = ["SD8Gen3", "SD7sGen3", "SD6Gen1", "SD4Gen2", "SD888"]
    networks = ["5G", "LTE", "5G", "5G", "LTE"]  # weighted toward 5G
    devices = []
    for i in range(count):
        model_idx = i % len(models)
        devices.append(DeviceProfile(
            device_id=f"DEV-{i:05d}",
            model=models[model_idx],
            firmware=f"v{random.randint(10, 15)}.{random.randint(0, 9)}.{random.randint(0, 9)}",
            network_type=networks[model_idx],
            cpu_base=random.uniform(20.0, 50.0),
            mem_base=random.uniform(40.0, 70.0),
            signal_base=random.uniform(-90.0, -55.0),
        ))
    return devices


def generate_event(device: DeviceProfile, inject_anomaly: bool = False) -> TelemetryEvent:
    """
    Generate a telemetry reading for a device.
    Normal readings follow Gaussian noise around the device's baseline.
    Anomalies spike specific metrics beyond 3-sigma thresholds.
    """
    cpu = device.cpu_base + random.gauss(0, 5)
    mem = device.mem_base + random.gauss(0, 4)
    signal = device.signal_base + random.gauss(0, 3)
    throughput = max(0, random.gauss(200, 40))
    battery = random.uniform(20, 100)

    if inject_anomaly:
        # Randomly spike one or more metrics to trigger anomaly detection
        anomaly_type = random.choice(["cpu_spike", "signal_drop", "throughput_crash"])
        if anomaly_type == "cpu_spike":
            cpu = random.uniform(88, 100)
        elif anomaly_type == "signal_drop":
            signal = random.uniform(-120, -105)
            throughput = random.uniform(0, 10)
        elif anomaly_type == "throughput_crash":
            throughput = random.uniform(0, 5)

    cpu = round(max(0.0, min(100.0, cpu)), 2)
    mem = round(max(0.0, min(100.0, mem)), 2)
    signal = round(max(-120.0, min(-40.0, signal)), 2)
    throughput = round(max(0.0, throughput), 2)
    battery = round(battery, 2)

    if cpu > 85 or signal < -105 or throughput < 10:
        event_type = "critical"
    elif cpu > 70 or signal < -90 or throughput < 50:
        event_type = "degraded"
    else:
        event_type = "normal"

    return TelemetryEvent(
        device_id=device.device_id,
        model=device.model,
        firmware=device.firmware,
        network_type=device.network_type,
        timestamp=datetime.now(timezone.utc).isoformat(),
        cpu_usage_pct=cpu,
        memory_usage_pct=mem,
        signal_strength_dbm=signal,
        throughput_mbps=throughput,
        battery_pct=battery,
        event_type=event_type,
    )


def delivery_report(err, msg):
    if err:
        log.error("Delivery failed for %s: %s", msg.key(), err)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",                    # wait for all ISR replicas
        "retries": 5,
        "retry.backoff.ms": 300,
        "linger.ms": 5,                   # micro-batching to improve throughput
        "batch.size": 65536,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })


def run():
    log.info("Starting producer | devices=%d, rate=%.0f/s, topic=%s", DEVICE_COUNT, EVENTS_PER_SECOND, TOPIC)
    producer = build_producer()
    devices = make_device_fleet(DEVICE_COUNT)
    interval = 1.0 / EVENTS_PER_SECOND
    sent = 0
    anomaly_rate = 0.02  # 2% of events are injected anomalies

    # Graceful shutdown
    running = True
    def handle_signal(sig, frame):
        nonlocal running
        log.info("Shutdown signal received, flushing...")
        running = False
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    while running:
        device = random.choice(devices)
        inject = random.random() < anomaly_rate
        event = generate_event(device, inject_anomaly=inject)
        payload = json.dumps(asdict(event)).encode("utf-8")

        producer.produce(
            topic=TOPIC,
            key=event.device_id.encode("utf-8"),  # partition by device for ordering
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)  # non-blocking — trigger callbacks
        sent += 1

        if sent % 10_000 == 0:
            log.info("Sent %d events", sent)

        time.sleep(interval)

    producer.flush(timeout=10)
    log.info("Producer shut down cleanly. Total sent: %d", sent)


if __name__ == "__main__":
    run()
