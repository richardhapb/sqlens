CREATE TABLE IF NOT EXISTS query_metrics (
    id BIGSERIAL PRIMARY KEY,
    fingerprint VARCHAR(64) UNIQUE NOT NULL,
    example_sql TEXT NOT NULL,
    execution_count BIGINT NOT NULL DEFAULT 0,
    total_duration_secs DOUBLE PRECISION NOT NULL DEFAULT 0,
    min_duration_secs REAL NOT NULL DEFAULT 0,
    max_duration_secs REAL NOT NULL DEFAULT 0,
    avg_duration_secs REAL NOT NULL DEFAULT 0,
    first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_query_metrics_avg_duration ON query_metrics(avg_duration_secs DESC);
CREATE INDEX IF NOT EXISTS idx_query_metrics_total_duration ON query_metrics(total_duration_secs DESC);
CREATE INDEX IF NOT EXISTS idx_query_metrics_count ON query_metrics(execution_count DESC);
CREATE INDEX IF NOT EXISTS idx_query_metrics_last_seen ON query_metrics(last_seen DESC);
