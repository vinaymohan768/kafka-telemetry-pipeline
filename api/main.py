"""
main.py — Telemetry Query API

FastAPI service exposing queryable views into the telemetry_events and
anomaly_alerts PostgreSQL tables. Designed for dashboards, monitoring
systems, and ad-hoc diagnostics.

Endpoints:
  GET /health                              — liveness + DB connectivity check
  GET /devices                             — list all known devices
  GET /devices/{device_id}/metrics         — recent telemetry for a device
  GET /devices/{device_id}/anomalies       — anomaly history for a device
  GET /anomalies/recent                    — latest anomalies across all devices
  GET /stats/summary                       — aggregate pipeline stats
"""

import os
import time
import logging
from contextlib import asynccontextmanager
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("api")

DB_DSN = (
    f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
    f"port={os.getenv('POSTGRES_PORT', '5432')} "
    f"dbname={os.getenv('POSTGRES_DB', 'telemetry')} "
    f"user={os.getenv('POSTGRES_USER', 'telemetry')} "
    f"password={os.getenv('POSTGRES_PASSWORD', 'telemetry')} "
    f"connect_timeout=5"
)

# ── Connection pool (simple — use pgbouncer or asyncpg for prod scale) ────────

_conn: Optional[psycopg2.extensions.connection] = None

def get_conn():
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(DB_DSN)
        _conn.autocommit = True
    return _conn


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("API starting up, verifying DB connection...")
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        log.info("DB connection OK")
    except Exception as e:
        log.error("DB connection failed on startup: %s", e)
    yield
    if _conn and not _conn.closed:
        _conn.close()
    log.info("API shut down")


app = FastAPI(
    title="Device Telemetry API",
    description="Real-time device telemetry and anomaly query API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ── Response models ───────────────────────────────────────────────────────────

class HealthResponse(BaseModel):
    status: str
    db: str
    uptime_seconds: float


class DeviceSummary(BaseModel):
    device_id: str
    model: str
    firmware: str
    network_type: str
    event_count: int
    last_seen: str


class TelemetryReading(BaseModel):
    event_timestamp: str
    cpu_usage_pct: float
    memory_usage_pct: float
    signal_strength_dbm: float
    throughput_mbps: float
    battery_pct: float
    event_type: str


class AnomalyRecord(BaseModel):
    device_id: str
    detected_at: str
    metric: str
    observed_value: float
    rolling_mean: float
    rolling_std_dev: float
    z_score: float
    severity: str


class PipelineStats(BaseModel):
    total_events: int
    total_anomalies: int
    critical_anomalies: int
    unique_devices: int
    events_last_hour: int
    anomalies_last_hour: int


# ── Helpers ───────────────────────────────────────────────────────────────────

_start_time = time.monotonic()


def query(sql: str, params=None) -> list[dict]:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health():
    db_status = "ok"
    try:
        query("SELECT 1")
    except Exception:
        db_status = "unreachable"

    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        db=db_status,
        uptime_seconds=round(time.monotonic() - _start_time, 2),
    )


@app.get("/devices", response_model=list[DeviceSummary])
def list_devices(limit: int = Query(100, ge=1, le=1000)):
    rows = query(
        """
        SELECT
            device_id,
            model,
            firmware,
            network_type,
            COUNT(*) AS event_count,
            MAX(event_timestamp)::text AS last_seen
        FROM telemetry_events
        GROUP BY device_id, model, firmware, network_type
        ORDER BY last_seen DESC
        LIMIT %s
        """,
        (limit,),
    )
    return [DeviceSummary(**r) for r in rows]


@app.get("/devices/{device_id}/metrics", response_model=list[TelemetryReading])
def get_device_metrics(
    device_id: str,
    limit: int = Query(100, ge=1, le=5000),
    since_minutes: int = Query(60, ge=1, le=1440),
):
    rows = query(
        """
        SELECT
            event_timestamp::text,
            cpu_usage_pct,
            memory_usage_pct,
            signal_strength_dbm,
            throughput_mbps,
            battery_pct,
            event_type
        FROM telemetry_events
        WHERE device_id = %s
          AND event_timestamp >= NOW() - INTERVAL '%s minutes'
        ORDER BY event_timestamp DESC
        LIMIT %s
        """,
        (device_id, since_minutes, limit),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for device {device_id}")
    return [TelemetryReading(**r) for r in rows]


@app.get("/devices/{device_id}/anomalies", response_model=list[AnomalyRecord])
def get_device_anomalies(
    device_id: str,
    limit: int = Query(50, ge=1, le=500),
    severity: Optional[str] = Query(None, regex="^(warning|critical)$"),
):
    sql = """
        SELECT
            device_id,
            detected_at::text,
            metric,
            observed_value,
            rolling_mean,
            rolling_std_dev,
            z_score,
            severity
        FROM anomaly_alerts
        WHERE device_id = %s
    """
    params: list = [device_id]

    if severity:
        sql += " AND severity = %s"
        params.append(severity)

    sql += " ORDER BY detected_at DESC LIMIT %s"
    params.append(limit)

    rows = query(sql, params)
    return [AnomalyRecord(**r) for r in rows]


@app.get("/anomalies/recent", response_model=list[AnomalyRecord])
def recent_anomalies(
    limit: int = Query(100, ge=1, le=1000),
    severity: Optional[str] = Query(None, regex="^(warning|critical)$"),
    since_minutes: int = Query(60, ge=1, le=1440),
):
    sql = """
        SELECT
            device_id,
            detected_at::text,
            metric,
            observed_value,
            rolling_mean,
            rolling_std_dev,
            z_score,
            severity
        FROM anomaly_alerts
        WHERE detected_at >= NOW() - INTERVAL '%s minutes'
    """
    params: list = [since_minutes]

    if severity:
        sql += " AND severity = %s"
        params.append(severity)

    sql += " ORDER BY detected_at DESC LIMIT %s"
    params.append(limit)

    rows = query(sql, params)
    return [AnomalyRecord(**r) for r in rows]


@app.get("/stats/summary", response_model=PipelineStats)
def pipeline_summary():
    events = query("""
        SELECT
            COUNT(*) AS total_events,
            COUNT(DISTINCT device_id) AS unique_devices,
            SUM(CASE WHEN event_timestamp >= NOW() - INTERVAL '1 hour' THEN 1 ELSE 0 END) AS events_last_hour
        FROM telemetry_events
    """)[0]

    anomalies = query("""
        SELECT
            COUNT(*) AS total_anomalies,
            SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) AS critical_anomalies,
            SUM(CASE WHEN detected_at >= NOW() - INTERVAL '1 hour' THEN 1 ELSE 0 END) AS anomalies_last_hour
        FROM anomaly_alerts
    """)[0]

    return PipelineStats(
        total_events=events["total_events"] or 0,
        total_anomalies=anomalies["total_anomalies"] or 0,
        critical_anomalies=anomalies["critical_anomalies"] or 0,
        unique_devices=events["unique_devices"] or 0,
        events_last_hour=events["events_last_hour"] or 0,
        anomalies_last_hour=anomalies["anomalies_last_hour"] or 0,
    )
