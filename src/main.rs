// lensql/src/main.rs

mod database;
mod proxy;
mod server;

use crate::proxy::tcp::forward_proxy;
use anyhow::bail;
use database::handler::PostgresCredentials;
use server::metrics::QueryStatistics;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tracing::{error, info, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    if let Err(_) = PostgresCredentials::connection_string() {
        bail!("DATABASE_URL environment variable must be set");
    }

    let listener = TcpListener::bind("0.0.0.0:5433").await?;
    info!("lensql proxy listening on 0.0.0.0:5433");

    let query_stats = Arc::new(RwLock::new(QueryStatistics::new()));
    let host = Arc::new(std::env::var("SQLENS_HOST").unwrap_or_else(|_| {
        trace!("SQLENS_HOST not set, using default");
        "localhost".into()
    }));
    let port = Arc::new(std::env::var("SQLENS_PORT").unwrap_or_else(|_| {
        trace!("SQLENS_PORT not set, using default");
        "5432".into()
    }));

    trace!(%host, %port, "SQLENS environment variables");

    loop {
        let (client_socket, addr) = listener.accept().await?;
        let query_stats_ref = query_stats.clone();

        let host = host.clone();
        let port = port.clone();
        tokio::spawn(async move {
            if let Err(e) = forward_proxy(client_socket, addr, query_stats_ref, &host, &port).await
            {
                error!(%e, "connection error");
            }
        });
    }
}
