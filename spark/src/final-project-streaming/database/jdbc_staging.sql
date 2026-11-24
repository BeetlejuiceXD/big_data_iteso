-- Apply to a fresh final-demo database (or an isolated test database) before consumer.py.
-- This file never modifies the historical streaming_project database.
-- The existing events/window_stats schema remains unchanged.
CREATE TABLE IF NOT EXISTS jdbc_stage_events (
    stream_id TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    event_id TEXT NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    category VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS jdbc_stage_events_batch_idx
    ON jdbc_stage_events (stream_id, batch_id);

CREATE TABLE IF NOT EXISTS jdbc_stage_windows (
    stream_id TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_events BIGINT NOT NULL,
    purchases BIGINT NOT NULL,
    revenue NUMERIC(14, 2) NOT NULL
);
CREATE INDEX IF NOT EXISTS jdbc_stage_windows_batch_idx
    ON jdbc_stage_windows (stream_id, batch_id);

-- Row locks serialize consolidation for each stream. Spark JDBC may commit
-- individual partitions before the full DataFrame write finishes.
CREATE TABLE IF NOT EXISTS jdbc_stream_locks (
    stream_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS jdbc_batch_commits (
    stream_id TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    payload_digest TEXT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream_id, batch_id)
);
