FROM rust:slim-bullseye AS builder
WORKDIR /app

# Install dependencies
# clang is required by `sql_query` crate
RUN apt-get update && \
    apt-get install -y \
    pkg-config \
    libssl-dev \
    git \
    clang  \ 
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

RUN cargo install sqlx-cli --no-default-features --features postgres

# Force sqlx to look for the .sqlx directory instead of a live DB
ENV SQLX_OFFLINE=true

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    libssl1.1 \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/sqlens .
COPY --from=builder /usr/local/cargo/bin/sqlx /usr/local/bin/sqlx
COPY --from=builder /app/migrations migrations/
COPY --from=builder /app/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 5433
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["./sqlens"]

