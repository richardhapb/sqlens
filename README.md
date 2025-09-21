
# SQLens

A high-performance PostgreSQL proxy that helps you understand and optimize your database queries by providing detailed performance analytics without modifying your application code.

## What it Does

SQLens sits between your application and PostgreSQL database, transparently forwarding queries while collecting valuable performance metrics. It helps you:

- Monitor query patterns in real-time
- Identify slow-performing queries
- Track query frequency and execution times
- Make data-driven optimization decisions

## Features

- Transparent proxy between PostgreSQL clients and servers
- Real-time query monitoring and statistics collection
- Comprehensive performance metrics per query:
  - Total execution count
  - Average duration
  - Minimum/Maximum duration
  - Total cumulative duration
- Automatic metrics storage in PostgreSQL for historical analysis
- Zero configuration required for client applications

## Usage

```bash
# Set required environment variables
export DATABASE_URL="postgresql://user:pass@localhost:5432/metrics_db"  # You can use the --str argument as well
export SQLENS_HOST="localhost"  # Optional: PostgreSQL host (default: localhost)
export SQLENS_PORT="5432"      # Optional: PostgreSQL port (default: 5432)

# Run the proxy
sqlens --bind 5433 --interval 300  # Updates metrics every 5 minutes
```

## Configuration

Command line options:
- `-H, --host`: Database engine host (default: localhost)
- `-p, --port`: Database engine port (default: 5432) 
- `-s, --str`: Connection string of the Database engine (e.g. postgresql://myuser:mypass@localhost:5432/mydb)
- `-b, --bind`: Port where SQLens will listen (default: 5433)
- `-i, --interval`: Metrics update interval in seconds (default: 300)

## Requirements

- PostgreSQL database for storing metrics
- Rust 1.70 or later (for building)

## License

MIT
