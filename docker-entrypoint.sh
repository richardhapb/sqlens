#!/bin/bash
set -e

# Wait for the database to be ready (important for Docker Compose or Kubernetes)
echo "Waiting for database..."
/usr/local/bin/sqlx database create || true # Create database if it doesn't exist, ignore error if it does
until /usr/local/bin/sqlx database create || /usr/local/bin/sqlx database create; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done
echo "Postgres is up - executing migrations"

# migrations
/usr/local/bin/sqlx migrate run

# Execute the main application command passed to CMD
exec "$@"
