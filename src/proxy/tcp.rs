use super::query_tracker::QueryTracker;
use crate::database::handler::PostgresCredentials;
use crate::server::metrics::Stats;
use crate::server::reporter::MetricsReporter;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{info, warn};

pub async fn forward_proxy(
    client_socket: TcpStream,
    client_addr: SocketAddr,
    query_stats: Stats,
    database_host: &str,
    database_port: &str,
    credentials: PostgresCredentials,
    interval: u64,
) -> anyhow::Result<()> {
    let server_socket = TcpStream::connect(format!("{}:{}", database_host, database_port)).await?;
    info!(?client_addr, "New proxy connection established");

    client_socket.set_nodelay(true)?;
    server_socket.set_nodelay(true)?;

    let (client_read, client_write) = tokio::io::split(client_socket);
    let (server_read, server_write) = tokio::io::split(server_socket);

    let query_tracker = Arc::new(Mutex::new(QueryTracker::new()));

    // Start metrics reporter (once)
    static REPORTER_STARTED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    REPORTER_STARTED.get_or_init(|| {
        let reporter = MetricsReporter::new(query_stats.clone(), credentials, interval);
        reporter.start();
    });

    // Upstream: client -> server
    let upstream = spawn_upstream_handler(client_read, server_write, query_tracker.clone());

    // Downstream: server -> client
    let downstream = spawn_downstream_handler(
        server_read,
        client_write,
        query_tracker.clone(),
        query_stats.clone(),
    );

    let (upstream_result, downstream_result) = tokio::join!(upstream, downstream);

    if let Err(e) = upstream_result? {
        warn!("Upstream error: {}", e);
    }
    if let Err(e) = downstream_result? {
        warn!("Downstream error: {}", e);
    }

    info!(?client_addr, "Proxy connection closed");
    Ok(())
}

fn spawn_upstream_handler(
    mut client_read: tokio::io::ReadHalf<TcpStream>,
    mut server_write: tokio::io::WriteHalf<TcpStream>,
    tracker: Arc<Mutex<QueryTracker>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut buf = [0u8; 16384];

        loop {
            let n = match client_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    warn!("Client read error: {}", e);
                    break;
                }
            };

            // Parse client messages
            handle_client_message(&buf[..n], &tracker).await;

            // Forward to server
            server_write.write_all(&buf[..n]).await?;
        }

        server_write.shutdown().await?;
        Ok(())
    })
}

async fn handle_client_message(buf: &[u8], tracker: &Arc<Mutex<QueryTracker>>) {
    if buf.is_empty() {
        return;
    }

    match buf[0] {
        b'P' | b'Q' => {
            if let Some(sql) = parse_query_from_buffer(buf) {
                tracker.lock().await.start_query(sql);
            }
        }
        b'E' => {
            tracker.lock().await.start_execute();
        }
        _ => {}
    }
}

fn spawn_downstream_handler(
    mut server_read: tokio::io::ReadHalf<TcpStream>,
    mut client_write: tokio::io::WriteHalf<TcpStream>,
    tracker: Arc<Mutex<QueryTracker>>,
    query_stats: Stats,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut buf = [0u8; 16384];
        let mut accumulator = Vec::new();
        let mut connection_ready = false;

        loop {
            let n = match server_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    warn!("Server read error: {}", e);
                    break;
                }
            };

            // Forward immediately
            client_write.write_all(&buf[..n]).await?;

            // Parse for metrics
            if !connection_ready {
                if buf[..n].contains(&b'Z') {
                    connection_ready = true;
                }
                continue;
            }

            accumulator.extend_from_slice(&buf[..n]);
            let consumed = parse_server_messages(&accumulator, &tracker, &query_stats).await;
            accumulator.drain(..consumed);
        }

        client_write.shutdown().await?;
        Ok(())
    })
}

// Helper: parse query from Parse/Query messages
fn parse_query_from_buffer(buf: &[u8]) -> Option<String> {
    if buf.len() < 5 {
        return None;
    }

    match buf[0] {
        b'Q' => {
            let sql_bytes = &buf[5..buf.len().saturating_sub(1)];
            std::str::from_utf8(sql_bytes).ok().map(|s| s.to_string())
        }
        b'P' => {
            let mut offset = 5;

            // Skip statement name
            while offset < buf.len() && buf[offset] != 0 {
                offset += 1;
            }
            offset += 1;

            if offset >= buf.len() {
                return None;
            }

            // Extract query
            let query_start = offset;
            while offset < buf.len() && buf[offset] != 0 {
                offset += 1;
            }

            std::str::from_utf8(&buf[query_start..offset])
                .ok()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
        }
        _ => None,
    }
}

async fn parse_server_messages(
    buf: &[u8],
    tracker: &Arc<Mutex<QueryTracker>>,
    query_stats: &Stats,
) -> usize {
    let mut i = 0;

    while i + 5 <= buf.len() {
        let msg_type = buf[i];
        let msg_len = u32::from_be_bytes([buf[i + 1], buf[i + 2], buf[i + 3], buf[i + 4]]) as usize;

        if !(4..=1_000_000).contains(&msg_len) {
            i += 1;
            continue;
        }

        let total = 1 + msg_len;
        if i + total > buf.len() {
            return i;
        }

        if msg_type == b'Z' {
            let mut tracker = tracker.lock().await;
            if let Some((sql, duration)) = tracker.complete_query() {
                let mut stats = query_stats.write().await;
                stats.record_query(&sql, duration);
            }
        }

        i += total;
    }

    i
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryStatistics;
    use std::thread::sleep;
    use std::time::{Duration, Instant};
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_parse_server_message() {
        let buf = [b'Z', 0, 0, 0, 5, b'I'];
        let tracker = Arc::new(Mutex::new(QueryTracker::new()));
        let query_stats = Arc::new(RwLock::new(QueryStatistics::new()));

        // Add a pending query
        tracker
            .lock()
            .await
            .pending_queries
            .push_back(("SELECT 1".to_string(), Instant::now()));

        parse_server_messages(&buf, &tracker, &query_stats).await;

        assert_eq!(
            tracker.lock().await.pending_queries.len(),
            0,
            "Query should be completed after ReadyForQuery"
        );
    }

    #[test]
    fn test_complete_query() {
        let mut tracker = QueryTracker::new();
        let query = "SELECT * FROM something".to_string();

        tracker.start_query(query.clone());

        // Add some time
        sleep(Duration::from_millis(20));

        let result = tracker.complete_query();
        assert!(result.is_some());

        let (fingerprint, duration) = result.unwrap();

        assert!(fingerprint.contains("SELECT"));
        assert!(duration > Duration::from_millis(20));
        assert!(duration < Duration::from_millis(100));
    }
}
