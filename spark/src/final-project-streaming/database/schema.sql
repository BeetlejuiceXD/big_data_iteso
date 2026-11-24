-- 1. Eventos procesados
CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    category VARCHAR(50) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    price NUMERIC(12, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL
);

-- 2. Estadísticas por ventanas de tiempo
CREATE TABLE IF NOT EXISTS window_stats (
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_events BIGINT NOT NULL,
    purchases BIGINT NOT NULL,
    revenue NUMERIC(14, 2) NOT NULL,

    PRIMARY KEY (window_start, window_end, category)
);