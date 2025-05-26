// lensql/src/main.rs

mod proxy;
mod server;

use crate::proxy::tcp::forward_proxy;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let listener = TcpListener::bind("0.0.0.0:5433").await?;
    info!("lensql proxy listening on 0.0.0.0:5433");

    loop {
        let (client_socket, addr) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = forward_proxy(client_socket, addr).await {
                error!(%e, "connection error");
            }
        });
    }
}
