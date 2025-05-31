use crate::server::metrics::{QueryStatistics, Stats};
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, instrument, trace, warn};

static UPDATE_LOOP: OnceLock<()> = OnceLock::new();

pub async fn forward_proxy(
    client_socket: TcpStream,
    client_addr: SocketAddr,
    query_stats: Stats,
    database_host: &str,
    database_port: &str,
    interval: u64,
) -> anyhow::Result<()> {
    let server_socket = TcpStream::connect(format!("{}:{}", database_host, database_port)).await?;
    info!(%client_addr, "New proxy connection established");

    // Configure TCP sockets
    client_socket.set_nodelay(true)?;
    server_socket.set_nodelay(true)?;

    // Channels
    let (mut client_read, mut client_write) = tokio::io::split(client_socket);
    let (mut server_read, mut server_write) = tokio::io::split(server_socket);

    trace!("Channels opened");

    // Shared state for query tracking
    let query_tracker = std::sync::Arc::new(tokio::sync::Mutex::new(QueryTracker::new()));

    // Client -> Server (upstream)
    let query_tracker_ref = query_tracker.clone();
    let upstream = tokio::spawn(async move {
        let mut buf = [0u8; 8192];

        loop {
            let n = match client_read.read(&mut buf).await {
                Ok(0) => {
                    trace!(client = ?client_addr, "Connection closed signal received from client");
                    break;
                }
                Ok(n) => {
                    trace!("Buffer received from client with length {}", n);
                    n
                }
                Err(e) => {
                    warn!("Error reading from client: {}", e);
                    break;
                }
            };

            // Check for SQL query messages
            if let Some(sql) = parse_query_message(&buf[..n]) {
                let mut tracker = query_tracker_ref.lock().await;
                trace!(sql = %sql, "Starting query");
                tracker.start_query(sql);
            }

            // Forward to server
            if let Err(e) = server_write.write_all(&buf[..n]).await {
                warn!("Error writing to server: {}", e);
                break;
            }
        }

        server_write.shutdown().await?;
        trace!("Server writer closed");
        Ok::<_, anyhow::Error>(())
    });

    // Loop that updates data
    let query_stats_ref = query_stats.clone();
    UPDATE_LOOP.get_or_init(|| {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(interval)).await;
                info!("Writing data to database");
                let report = query_stats_ref.read().unwrap().get_report();
                if let Err(result) =
                    QueryStatistics::write_to_database(query_stats_ref.clone()).await
                {
                    error!("Error writing data to database: {}", result);
                } else {
                    info!("Data inserted to database successfully");
                }
                trace!("\n\n{}", report);
            }
        });
    });

    // Server -> Client (downstream)
    let query_tracker_ref = query_tracker.clone();
    let query_stats_ref = query_stats.clone();
    let downstream = tokio::spawn(async move {
        let mut buf = [0u8; 8192];
        let mut total_bytes = 0;

        loop {
            let n = match server_read.read(&mut buf).await {
                Ok(0) => {
                    trace!(client = ?client_addr, "Connection closed signal received from server");
                    break;
                }
                Ok(n) => {
                    trace!("Buffer received from server with length {n}");
                    n
                }
                Err(e) => {
                    warn!("Error reading from server: {}", e);
                    break;
                }
            };

            total_bytes += n;

            // Parse server messages for timing asynchronously
            let query_tracker_ref = query_tracker_ref.clone();
            let query_stats_ref = query_stats_ref.clone();
            tokio::spawn(async move {
                trace!(buf_n = n, "Parsing server message");
                parse_server_messages(&buf[..n], &query_tracker_ref, query_stats_ref).await;
            });

            // Forward to client
            if let Err(e) = client_write.write_all(&buf[..n]).await {
                warn!("Error writing to client: {}", e);
                break;
            }
        }

        // Complete any remaining queries when connection closes
        {
            let mut tracker = query_tracker_ref.lock().await;
            trace!(queries = ?tracker.active_queries , "Completing remaining queries");
            tracker.complete_remaining_queries(query_stats);
        }

        info!(total_bytes, "Server connection closed");

        let _ = client_write.shutdown().await;
        Ok::<_, anyhow::Error>(())
    });

    // Wait for both tasks to complete
    let (upstream_result, downstream_result) = tokio::join!(upstream, downstream);

    if let Err(e) = upstream_result? {
        warn!("Upstream task error: {}", e);
    }
    if let Err(e) = downstream_result? {
        warn!("Downstream task error: {}", e);
    }

    info!(%client_addr, "Proxy connection closed");
    Ok(())
}

// Simple query tracker
#[derive(Debug)]
struct QueryTracker {
    active_queries: std::collections::VecDeque<(String, Instant)>,
}

impl QueryTracker {
    fn new() -> Self {
        Self {
            active_queries: std::collections::VecDeque::new(),
        }
    }

    fn start_query(&mut self, sql: String) {
        let start_time = Instant::now();
        debug!(sql = %sql.trim(), "Query started");
        self.active_queries.push_back((sql, start_time));
    }

    fn complete_query(&mut self) -> Option<(String, Duration)> {
        if let Some((sql, start_time)) = self.active_queries.pop_front() {
            let duration = start_time.elapsed();
            info!(
                sql = %sql.trim(),
                duration_ms = duration.as_millis(),
                "Query completed ✓"
            );

            Some((sql, duration))
        } else {
            // This happens when we get Z before Q, which is normal for initial connection setup
            info!("ReadyForQuery received (connection ready)");
            None
        }
    }

    fn complete_remaining_queries(&mut self, query_stats: Stats) {
        while let Some((sql, start_time)) = self.active_queries.pop_front() {
            let duration = start_time.elapsed();
            info!(
                sql = %sql.trim(),
                duration_ms = duration.as_millis(),
                "Query completed (connection closed)"
            );

            let mut stats = query_stats.write().unwrap_or_else(|e| {
                error!("QUERY_STATS is posioned, trying to recover");
                query_stats.clear_poison();
                e.into_inner()
            });
            stats.record_query(&sql, duration);
        }
    }
}

// Parse PostgreSQL Query message (type 'Q')
#[instrument(skip_all)]
fn parse_query_message(buf: &[u8]) -> Option<String> {
    if buf.len() < 5 || buf[0] != b'Q' {
        trace!("Returning None because incomplete buffer message");
        return None;
    }

    // Extract SQL text (skip message type + length, remove null terminator)
    let sql_bytes = &buf[5..buf.len().saturating_sub(1)];
    std::str::from_utf8(sql_bytes).ok().map(|s| s.to_string())
}

// Parse server messages for detailed timing
#[instrument(skip_all)]
async fn parse_server_messages(
    buf: &[u8],
    tracker: &std::sync::Arc<tokio::sync::Mutex<QueryTracker>>,
    query_stats: Stats,
) {
    let mut i = 0;
    let mut tracker_guard = tracker.lock().await; // Lock once for this function

    while i + 4 < buf.len() {
        let msg_type = buf[i];
        let msg_len = u32::from_be_bytes([buf[i + 1], buf[i + 2], buf[i + 3], buf[i + 4]]) as usize;

        if msg_len < 4 || i + 1 + msg_len > buf.len() {
            // Not enough bytes for the declared message length, or invalid length
            debug!(
                "Partial or invalid PostgreSQL message: type={}, len={}",
                msg_type as char, msg_len
            );
            break;
        }

        match msg_type {
            b'Q' => {
                // This is a query message, typically from client to server.
                // If it is here, it implies data read from server
                // contains client data, which shouldn't happen in a proxy setup.
                // Potentially a misinterpretation or out-of-sync state.
                warn!("Unexpected 'Q' message from server");
            }
            b'R' => {
                // AuthenticationRequest or AuthenticationOk
                debug!("Authentication or other 'R' message from server");
            }
            b'S' => {
                debug!("ParameterStatus (S) message from server");
            }
            b'K' => {
                debug!("BackendKeyData (K) message from server");
            }
            b'Z' => {
                // ReadyForQuery
                debug!("ReadyForQuery (Z) message found");
                if let Some((sql, duration)) = tracker_guard.complete_query() {
                    // Record detailed statistics
                    let mut stats = query_stats.write().unwrap_or_else(|e| {
                        error!("QUERY_STATS is posioned, trying to recover");
                        query_stats.clear_poison();
                        e.into_inner()
                    });

                    stats.record_query(&sql, duration);
                    debug!("Record inserted: {:?}", stats);
                }
            }
            b'T' => {
                // RowDescription
                debug!("RowDescription (T) message found");
                debug!("Results starting to arrive");
            }
            b'C' => {
                // CommandComplete
                debug!("CommandComplete (C) message found");
                // Don't complete query here, wait for Z
            }
            b'E' => {
                // ErrorResponse
                warn!("ErrorResponse (E) message from server");
            }
            _ => {
                debug!(
                    "Unhandled PostgreSQL message type: {} (char '{}')",
                    msg_type, msg_type as char
                );
            }
        }

        i += 1 + msg_len;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryStatistics;
    use std::sync::{Arc, Mutex, RwLock};
    use std::thread::sleep;
    use std::time::Duration;

    #[test]
    fn test_parse_query_message() {
        let mut buf = Vec::new();
        let query = "SELECT * FROM something";

        buf.push(b'Q');
        let len = (query.len() + 1 + 4) as u32; // Message length (query + null terminator + length field)
        buf.extend_from_slice(&len.to_be_bytes()); // Length as big-endian bytes
        buf.extend_from_slice(query.as_bytes()); // Query text
        buf.push(0); // Null terminator

        let result = parse_query_message(&buf);

        assert!(result.is_some());
        assert_eq!(result.unwrap(), query);
    }

    #[tokio::test]
    async fn test_parse_server_message() {
        // Create a 'Z' (ReadyForQuery) message with proper length encoding
        // Format: 'Z' followed by message length (5 bytes including the length itself), followed by transaction status byte
        let buf = [b'Z', 0, 0, 0, 5, b'I']; // 'I' indicates idle transaction state
        let tracker = std::sync::Arc::new(tokio::sync::Mutex::new(QueryTracker::new()));
        let query_stats = Arc::new(RwLock::new(QueryStatistics::new()));
        tracker
            .lock()
            .await
            .active_queries
            .push_back(("ALICE, 20, Berlin".to_string(), Instant::now()));

        parse_server_messages(&buf, &tracker, query_stats.clone()).await;

        assert_eq!(
            tracker.lock().await.active_queries.len(),
            0,
            "Query should be completed after ReadyForQuery"
        );
        assert_eq!(query_stats.read().unwrap().queries.len(), 1);
    }

    #[test]
    fn test_complete_query() {
        let tracker = Arc::new(Mutex::new(QueryTracker::new()));
        let query = "SELECT * FROM something".to_string();

        tracker.lock().unwrap().start_query(query.clone());

        // Add some time
        sleep(Duration::from_millis(20));

        let result = tracker.lock().unwrap().complete_query();
        assert!(result.is_some());

        let (sql, duration) = result.unwrap();

        assert_eq!(sql, query);
        assert!(duration > Duration::from_millis(20));
        assert!(duration < Duration::from_millis(100));
    }
}
