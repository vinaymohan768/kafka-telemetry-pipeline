-- init.sql
-- Schema for the device telemetry pipeline.
-- Uses PostgreSQL declarative partitioning (range by month) to keep
-- query plans fast on large datasets without manual partition management.

--  Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;  -- query performance tracking

--  Telemetry events (partitioned by month) ───────────────────────────────────
CREATE TABLE IF NOT EXISTS telemetry_events (
    id                  BIGSERIAL,
    device_id           VARCHAR(20)     NOT NULL,
    model               VARCHAR(30)     NOT NULL,
    firmware            VARCHAR(20)     NOT NULL,
    network_type        VARCHAR(5)      NOT NULL,
    event_timestamp     TIMESTAMPTZ     NOT NULL,
    cpu_usage_pct       NUMERIC(5, 2)   NOT NULL CHECK (cpu_usage_pct BETWEEN 0 AND 100),
    memory_usage_pct    NUMERIC(5, 2)   NOT NULL CHECK (memory_usage_pct BETWEEN 0 AND 100),
    signal_strength_dbm NUMERIC(6, 2)   NOT NULL CHECK (signal_strength_dbm BETWEEN -120 AND -40),
    throughput_mbps     NUMERIC(8, 2)   NOT NULL CHECK (throughput_mbps >= 0),
    battery_pct         NUMERIC(5, 2)   NOT NULL CHECK (battery_pct BETWEEN 0 AND 100),
    event_type          VARCHAR(10)     NOT NULL CHECK (event_type IN ('normal', 'degraded', 'critical')),
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
) PARTITION BY RANGE (event_timestamp);

-- Monthly partitions — add new ones as needed or automate with pg_partman
CREATE TABLE IF NOT EXISTS telemetry_events_2025_01
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_02
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_03
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_04
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_05
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_06
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_07
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_08
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_09
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_10
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_11
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2025_12
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_01
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_02
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_03
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_04
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_05
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE TABLE IF NOT EXISTS telemetry_events_2026_06
    PARTITION OF telemetry_events
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');

--  Indexes on partitioned table ──────────────────────────────────────────────
-- Composite B-tree: device lookups filtered by time (most common query pattern)
CREATE INDEX IF NOT EXISTS idx_telemetry_device_time
    ON telemetry_events (device_id, event_timestamp DESC);

-- Partial index for fast critical event queries (small % of total rows)
CREATE INDEX IF NOT EXISTS idx_telemetry_critical
    ON telemetry_events (event_timestamp DESC)
    WHERE event_type = 'critical';

-- Network type index for fleet-wide segmentation queries
CREATE INDEX IF NOT EXISTS idx_telemetry_network_time
    ON telemetry_events (network_type, event_timestamp DESC);


--  Anomaly alerts ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id              BIGSERIAL PRIMARY KEY,
    device_id       VARCHAR(20)     NOT NULL,
    detected_at     TIMESTAMPTZ     NOT NULL,
    metric          VARCHAR(40)     NOT NULL,
    observed_value  NUMERIC(10, 4)  NOT NULL,
    rolling_mean    NUMERIC(10, 4)  NOT NULL,
    rolling_std_dev NUMERIC(10, 4)  NOT NULL,
    z_score         NUMERIC(8, 4)   NOT NULL,
    severity        VARCHAR(10)     NOT NULL CHECK (severity IN ('warning', 'critical')),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_device_time
    ON anomaly_alerts (device_id, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_severity_time
    ON anomaly_alerts (severity, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_metric
    ON anomaly_alerts (metric, detected_at DESC);


--  Materialized view: per-device 5-minute rollup ─────────────────────────────
-- Refresh with: SELECT refresh_telemetry_rollup();
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_5min_rollup AS
SELECT
    device_id,
    network_type,
    date_trunc('minute', event_timestamp) -
        INTERVAL '1 minute' * (EXTRACT(MINUTE FROM event_timestamp)::int % 5) AS window_start,
    COUNT(*)                                AS event_count,
    ROUND(AVG(cpu_usage_pct)::numeric, 2)      AS avg_cpu,
    ROUND(MAX(cpu_usage_pct)::numeric, 2)      AS max_cpu,
    ROUND(AVG(memory_usage_pct)::numeric, 2)   AS avg_memory,
    ROUND(AVG(signal_strength_dbm)::numeric, 2) AS avg_signal,
    ROUND(AVG(throughput_mbps)::numeric, 2)    AS avg_throughput,
    ROUND(MIN(throughput_mbps)::numeric, 2)    AS min_throughput,
    SUM(CASE WHEN event_type = 'critical' THEN 1 ELSE 0 END) AS critical_count
FROM telemetry_events
GROUP BY device_id, network_type, window_start
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_rollup_device_window
    ON telemetry_5min_rollup (device_id, window_start);

-- Function to refresh the rollup (call from a cron or pg_cron)
CREATE OR REPLACE FUNCTION refresh_telemetry_rollup()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY telemetry_5min_rollup;
END;
$$;
