#![allow(dead_code)]

mod database;
mod proxy;
mod server;

use crate::database::handler::PostgresCredentials;
use crate::proxy::tcp::handle_connection;
use crate::server::metrics::QueryStatistics;
use crate::server::reporter::MetricsReporter;
use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{error, info, trace};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// An extremely fast SQL proxy that connects a client to an SQL engine.
/// Log, track, and store the data from the queries.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Host of the Database engine (e.g. localhost or 127.0.0.1, default: localhost)
    #[arg(short('H'), long)]
    host: Option<String>,

    /// Port of the Database engine (e.g. 5432, default: 5432)
    #[arg(short, long)]
    port: Option<u16>,

    /// Connection string where sqlens will store stats data (e.g. postgresql://myuser:mypass@localhost:5432/mydb)
    #[arg(short, long)]
    str: Option<String>,

    /// Port where sqlens will be listening
    #[arg(short, long, default_value_t = 5433)]
    bind: u16,

    /// Queries update interval (seconds) that is stored in database and logged (default: 300 = 5m)
    #[arg(short, long, default_value_t = 300)]
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

    // Shared metrics state
    let query_stats = Arc::new(RwLock::new(QueryStatistics::new()));

    // Database credentials for metrics storage
    let credentials = PostgresCredentials::try_new(&args).ok_or_else(|| {
        anyhow::anyhow!("Missing database configuration. Set DATABASE_URL or pass --str")
    })?;
    credentials.validate()?;

    // Start metrics reporter (background task)
    let reporter = MetricsReporter::new(
        query_stats.clone(),
        credentials.conn_str.clone(),
        args.interval,
    );
    let _reporter_handle = reporter.start();
    info!(interval_secs = args.interval, "metrics reporter started");

    // Target database
    let (target_host, target_port) = get_target_database(&args);
    info!(host = %target_host, port = target_port, "proxying to database");

    // Listen for connections
    let listener = TcpListener::bind(("0.0.0.0", args.bind)).await?;
    info!(port = args.bind, "sqlens proxy listening");

    loop {
        let (client_socket, client_addr) = listener.accept().await?;

        let stats = query_stats.clone();
        let host = target_host.clone();

        tokio::spawn(async move {
            if let Err(e) =
                handle_connection(client_socket, client_addr, stats, &host, target_port).await
            {
                error!(%client_addr, error = %e, "connection error");
            }
        });
    }
}

fn get_target_database(args: &Args) -> (String, u16) {
    let host = args.host.clone().unwrap_or_else(|| {
        std::env::var("SQLENS_DB_HOST").unwrap_or_else(|_| {
            trace!("SQLENS_DB_HOST not set, defaulting to localhost");
            "localhost".into()
        })
    });

    let port = args.port.unwrap_or_else(|| {
        std::env::var("SQLENS_DB_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or_else(|| {
                trace!("SQLENS_DB_PORT not set, defaulting to 5432");
                5432
            })
    });

    (host, port)
}
