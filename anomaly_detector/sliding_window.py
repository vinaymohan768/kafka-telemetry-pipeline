"""
sliding_window.py

Per-device sliding window anomaly detector using Welford's online algorithm
for computing rolling mean and variance in O(1) per update — no full window
scan required.

Design choices:
- Each device gets its own independent window (no cross-device state)
- Welford's algorithm avoids recomputing sum/sum-of-squares on every eviction
- Z-score threshold is configurable per metric
- Minimum window size prevents false positives during warmup

Metrics evaluated independently:
  - cpu_usage_pct
  - signal_strength_dbm
  - throughput_mbps
  - memory_usage_pct
"""

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AnomalyResult:
    device_id: str
    timestamp: str
    metric: str
    observed_value: float
    mean: float
    std_dev: float
    z_score: float
    severity: str  # "warning" | "critical"


@dataclass
class WelfordState:
    """
    Online mean/variance tracker using Welford's method.
    Supports sliding window by tracking a fixed-size deque and
    subtracting evicted values using a compensated update.
    """
    window_size: int
    _count: int = field(default=0, init=False)
    _mean: float = field(default=0.0, init=False)
    _M2: float = field(default=0.0, init=False)    # sum of squared deviations
    _buffer: list = field(default_factory=list, init=False)

    def update(self, value: float):
        """Add new value; evict oldest if window is full."""
        if len(self._buffer) == self.window_size:
            self._remove(self._buffer.pop(0))
        self._buffer.append(value)
        self._add(value)

    def _add(self, value: float):
        self._count += 1
        delta = value - self._mean
        self._mean += delta / self._count
        delta2 = value - self._mean
        self._M2 += delta * delta2

    def _remove(self, value: float):
        """
        Online removal using the inverse Welford update.
        Valid when count > 1; degrades gracefully to reset on underflow.
        """
        if self._count <= 1:
            self._count = 0
            self._mean = 0.0
            self._M2 = 0.0
            return
        self._count -= 1
        delta = value - self._mean
        self._mean -= delta / self._count
        delta2 = value - self._mean
        self._M2 -= delta * delta2
        self._M2 = max(0.0, self._M2)  # guard against floating-point underflow

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def variance(self) -> float:
        return self._M2 / self._count if self._count > 1 else 0.0

    @property
    def std_dev(self) -> float:
        return math.sqrt(self.variance)

    @property
    def ready(self) -> bool:
        """True once enough data has accumulated for reliable detection."""
        return self._count >= self.window_size // 2


MONITORED_METRICS = ["cpu_usage_pct", "signal_strength_dbm", "throughput_mbps", "memory_usage_pct"]

# Z-score thresholds per metric
# Signal and throughput use lower thresholds — network degradation appears as
# sustained subtle drift rather than sharp spikes.
THRESHOLDS: dict[str, dict[str, float]] = {
    "cpu_usage_pct":       {"warning": 2.0, "critical": 3.0},
    "signal_strength_dbm": {"warning": 1.8, "critical": 2.5},
    "throughput_mbps":     {"warning": 1.8, "critical": 2.5},
    "memory_usage_pct":    {"warning": 2.2, "critical": 3.2},
}


class SlidingWindowDetector:
    """
    Maintains per-device, per-metric Welford windows.
    Thread-safe if access is serialized per device (single consumer thread
    per partition is the standard Kafka pattern — no locking needed).
    """

    def __init__(self, window_size: int = 60):
        """
        Args:
            window_size: Number of readings to keep per device per metric.
                         At 500 events/sec across 50 devices, each device
                         receives ~10 events/sec, so window_size=60 ≈ 6 seconds.
        """
        self.window_size = window_size
        # device_id -> metric -> WelfordState
        self._states: dict[str, dict[str, WelfordState]] = defaultdict(
            lambda: {m: WelfordState(window_size=window_size) for m in MONITORED_METRICS}
        )

    def process(self, event: dict) -> list[AnomalyResult]:
        """
        Update windows for all metrics in the event.
        Returns a list of AnomalyResult for any metric that crossed a threshold.
        """
        device_id = event["device_id"]
        timestamp = event["timestamp"]
        anomalies = []

        for metric in MONITORED_METRICS:
            value = event.get(metric)
            if value is None:
                continue

            state = self._states[device_id][metric]
            state.update(value)

            if not state.ready:
                continue

            std = state.std_dev
            if std < 1e-6:
                continue  # flat signal — nothing to detect against

            z = abs(value - state.mean) / std
            thresholds = THRESHOLDS[metric]

            severity: Optional[str] = None
            if z >= thresholds["critical"]:
                severity = "critical"
            elif z >= thresholds["warning"]:
                severity = "warning"

            if severity:
                anomalies.append(AnomalyResult(
                    device_id=device_id,
                    timestamp=timestamp,
                    metric=metric,
                    observed_value=round(value, 4),
                    mean=round(state.mean, 4),
                    std_dev=round(std, 4),
                    z_score=round(z, 4),
                    severity=severity,
                ))

        return anomalies

    def device_count(self) -> int:
        return len(self._states)
