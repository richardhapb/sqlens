# SQLens

A high-performance PostgreSQL wire protocol proxy that captures query metrics without touching your application code.

> [!WARNING]
> **Not production-ready.**
> - No TLS/SSL passthrough yet
> - No pgbouncer transaction-mode compatibility
> - Missing benchmarks for overhead guarantees
> - Breaking changes expected

## What It Does

SQLens intercepts PostgreSQL wire protocol traffic, extracts query patterns using fingerprinting, and aggregates performance metrics. Point your app at SQLens instead of Postgres — everything else stays the same.
```
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   Your App  │─────▶│   SQLens    │─────▶│  PostgreSQL │
│  port 5433  │      │   (proxy)   │      │  port 5432  │
└─────────────┘      └──────┬──────┘      └─────────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │ Metrics DB  │
                    │  (postgres) │
                    └─────────────┘
```

## Features

- **Transparent proxying** — No application changes required
- **Query fingerprinting** — Groups `SELECT * FROM users WHERE id=1` and `SELECT * FROM users WHERE id=9999` as the same pattern
- **Extended query protocol support** — Handles prepared statements, portals, and pipelined queries correctly
- **Per-pattern metrics:**
  - Execution count
  - Total / min / max / avg duration
  - First seen / last seen timestamps
- **Automatic persistence** — Metrics stored in PostgreSQL on configurable interval
- **Memory-bounded** — LRU-style eviction prevents unbounded growth

## Quick Start
```bash
# 1. Create metrics database
createdb sqlens_metrics

# 2. Run the proxy
export DATABASE_URL="postgresql://localhost/sqlens_metrics"
sqlens --bind 5433

# 3. Point your app to port 5433 instead of 5432
# That's it.
```

## Installation
```bash
# From source
cargo install --path .

# Or build manually
cargo build --release
./target/release/sqlens --help
```

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-H, --host` | `SQLENS_DB_HOST` | `localhost` | Target PostgreSQL host |
| `-p, --port` | `SQLENS_DB_PORT` | `5432` | Target PostgreSQL port |
| `-s, --str` | `DATABASE_URL` | — | Metrics storage connection string |
| `-b, --bind` | — | `5433` | Port SQLens listens on |
| `-i, --interval` | — | `300` | Metrics flush interval (seconds) |

## Metrics Schema
```sql
CREATE TABLE query_metrics (
    id BIGSERIAL PRIMARY KEY,
    fingerprint VARCHAR(64) UNIQUE NOT NULL,  -- pg_query fingerprint
    example_sql TEXT NOT NULL,                -- Sample query for debugging
    execution_count BIGINT NOT NULL,
    total_duration_secs DOUBLE PRECISION NOT NULL,
    min_duration_secs REAL NOT NULL,
    max_duration_secs REAL NOT NULL,
    avg_duration_secs REAL NOT NULL,
    first_seen TIMESTAMPTZ NOT NULL,
    last_seen TIMESTAMPTZ NOT NULL
);
```

## Example Queries
```sql
-- Top 10 slowest query patterns
SELECT fingerprint, example_sql, execution_count, 
       round(avg_duration_secs * 1000) as avg_ms
FROM query_metrics 
ORDER BY avg_duration_secs DESC 
LIMIT 10;

-- Highest total time (most impactful to optimize)
SELECT fingerprint, example_sql, execution_count,
       round(total_duration_secs::numeric, 2) as total_secs
FROM query_metrics 
ORDER BY total_duration_secs DESC 
LIMIT 10;

-- Most frequent queries
SELECT fingerprint, example_sql, execution_count
FROM query_metrics 
ORDER BY execution_count DESC 
LIMIT 10;

-- Queries not seen in 7 days (cleanup candidates)
SELECT fingerprint, example_sql, last_seen
FROM query_metrics 
WHERE last_seen < NOW() - INTERVAL '7 days';
```

## How Fingerprinting Works

SQLens uses [pg_query](https://github.com/pganalyze/pg_query) to normalize queries:
```
SELECT * FROM users WHERE id = 1      ─┐
SELECT * FROM users WHERE id = 42      ├─▶ fingerprint: "abc123"
SELECT * FROM users WHERE id = 99999  ─┘

SELECT * FROM orders WHERE id = 1     ───▶ fingerprint: "def456"
```

Same table + same structure = same fingerprint, regardless of literal values.

## Limitations

- **No TLS passthrough** — SQLens sees plaintext wire protocol. Use it on trusted networks or terminate TLS at a load balancer.
- **No transaction-mode pgbouncer** — Prepared statements don't persist across pgbouncer transactions. Use session mode if you need both.
- **Single-process** — One SQLens instance per metrics DB. Horizontal scaling not implemented.

## Requirements

- Rust 1.70+
- PostgreSQL 12+ (target and metrics)

## License

MIT
