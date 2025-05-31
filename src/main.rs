mod database;
mod proxy;
mod server;

use crate::proxy::tcp::forward_proxy;
use anyhow::bail;
use clap::Parser;
use database::handler::PostgresCredentials;
use server::metrics::QueryStatistics;
use std::sync::{Arc, RwLock};
use tokio::net::TcpListener;
use tracing::{error, info, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// An extremely fast SQL proxy that connects a client to an SQL engine.
/// Log, track, and store the data from the queries.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Host of the Database engine (e.g. localhost or 127.0.0.1)
    #[arg(short('H'), long)]
    host: Option<String>,

    /// Port of the Database engine (e.g. 5432)
    #[arg(short, long)]
    port: Option<String>,

    /// Port where sqlens will be listening
    #[arg(short, long, default_value_t=5433.to_string())]
    bind: String,

    /// Queries update interval (seconds) that is storaged in database and logged
    #[arg(short, long, default_value_t = 5 * 60)]
    interval: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();

    if let Err(_) = PostgresCredentials::connection_string() {
        bail!("DATABASE_URL environment variable must be set");
    }

    let listener = TcpListener::bind(format!("0.0.0.0:{}", args.bind)).await?;
    info!("sqlens proxy listening on 0.0.0.0:{}", args.bind);

    let query_stats = Arc::new(RwLock::new(QueryStatistics::new()));

    let (host, port) = get_database_host(&args);

    loop {
        let (client_socket, addr) = listener.accept().await?;
        let query_stats_ref = query_stats.clone();

        let host = host.clone();
        let port = port.clone();
        tokio::spawn(async move {
            if let Err(e) = forward_proxy(client_socket, addr, query_stats_ref, &host, &port, args.interval).await
            {
                error!(%host, %port, "connection error: {e}");
            }
        });
    }
}

fn get_database_host(args: &Args) -> (Arc<String>, Arc<String>) {
    let host = Arc::new(args.host.clone().unwrap_or_else(|| {
        std::env::var("SQLENS_HOST").unwrap_or_else(|_| {
            trace!("SQLENS_HOST not set, using default");
            "localhost".into()
        })
    }));

    let port = Arc::new(args.port.clone().unwrap_or_else(|| {
        std::env::var("SQLENS_PORT").unwrap_or_else(|_| {
            trace!("SQLENS_PORT not set, using default");
            "5432".into()
        })
    }));

    trace!(%host, %port, "SQLENS environment variables");

    (host, port)
}
