
FROM rust:slim-bullseye AS builder

WORKDIR /app

RUN cargo install sqlx-cli --no-default-features --features postgres

RUN apt-get update
RUN apt-get -y install pkg-config librust-openssl-sys-dev --no-install-recommends

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim

WORKDIR /app

COPY --from=builder /app/target/release/sqlens .
COPY --from=builder /usr/local/cargo/bin/sqlx /usr/local/bin/sqlx
COPY migrations migrations/

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 5433

ENTRYPOINT [ "docker-entrypoint.sh" ]

CMD ["./sqlens"]
