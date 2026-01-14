use crate::proxy::query_tracker::QueryTracker;
use crate::server::metrics::Stats;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, info, trace, warn};

/// Forward proxy connection handler
///
/// Each connection gets its own QueryTracker because PostgreSQL connections
/// are stateful (prepared statements, transaction state, etc.)
pub async fn handle_connection(
    client_socket: TcpStream,
    client_addr: SocketAddr,
    query_stats: Stats,
    target_host: &str,
    target_port: u16,
) -> anyhow::Result<()> {
    let server_socket = TcpStream::connect((target_host, target_port)).await?;
    info!(?client_addr, "connection_established");

    // Disable Nagle's algorithm for lower latency
    client_socket.set_nodelay(true)?;
    server_socket.set_nodelay(true)?;

    let (client_read, client_write) = tokio::io::split(client_socket);
    let (server_read, server_write) = tokio::io::split(server_socket);

    // Per-connection query tracker
    let tracker = Arc::new(Mutex::new(QueryTracker::new()));

    // Spawn bidirectional forwarding
    let upstream = spawn_upstream(client_read, server_write, tracker.clone());
    let downstream = spawn_downstream(server_read, client_write, tracker.clone(), query_stats);

    // Wait for both directions to complete
    let (up_result, down_result) = tokio::join!(upstream, downstream);

    if let Err(e) = up_result? {
        debug!(error = %e, "upstream_closed");
    }
    if let Err(e) = down_result? {
        debug!(error = %e, "downstream_closed");
    }

    info!(?client_addr, "connection_closed");
    Ok(())
}

/// Client -> Server (upstream) handler
fn spawn_upstream(
    mut client_read: tokio::io::ReadHalf<TcpStream>,
    mut server_write: tokio::io::WriteHalf<TcpStream>,
    tracker: Arc<Mutex<QueryTracker>>,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 32768];

        loop {
            let n = match client_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    debug!(error = %e, "client_read_error");
                    break;
                }
            };

            // Forward to server before parsing for less delay
            if let Err(e) = server_write.write_all(&buf[..n]).await {
                debug!(error = %e, "server_write_error");
                break;
            }

            // Parse client messages for tracking
            parse_client_messages(&buf[..n], &tracker).await;
        }

        let _ = server_write.shutdown().await;
        Ok(())
    })
}

/// Server -> Client (downstream) handler
fn spawn_downstream(
    mut server_read: tokio::io::ReadHalf<TcpStream>,
    mut client_write: tokio::io::WriteHalf<TcpStream>,
    tracker: Arc<Mutex<QueryTracker>>,
    stats: Stats,
) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 32768];
        let mut msg_buf = Vec::with_capacity(65536);
        let mut startup_complete = false;

        loop {
            let n = match server_read.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) => {
                    debug!(error = %e, "server_read_error");
                    break;
                }
            };

            // Forward to client immediately (less delay)
            if let Err(e) = client_write.write_all(&buf[..n]).await {
                debug!(error = %e, "client_write_error");
                break;
            }

            // Skip parsing until connection is ready
            if !startup_complete {
                // Look for ReadyForQuery in startup sequence
                if contains_ready_for_query(&buf[..n]) {
                    startup_complete = true;
                    trace!("startup_complete");
                }
                continue;
            }

            // Accumulate for parsing (messages can span reads)
            msg_buf.extend_from_slice(&buf[..n]);

            // Parse complete messages
            let consumed = parse_server_messages(&msg_buf, &tracker, &stats).await;
            if consumed > 0 {
                msg_buf.drain(..consumed);
            }
        }

        let _ = client_write.shutdown().await;
        Ok(())
    })
}

/// Parse PostgreSQL client messages
async fn parse_client_messages(buf: &[u8], tracker: &Arc<Mutex<QueryTracker>>) {
    let mut offset = 0;

    while offset < buf.len() {
        let msg_type = buf[offset];

        // Need at least 5 bytes for type + length
        if offset + 5 > buf.len() {
            break;
        }

        let msg_len = u32::from_be_bytes([
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
        ]) as usize;

        let total_size = 1 + msg_len;
        if offset + total_size > buf.len() {
            break; // Incomplete message
        }

        let msg_body = &buf[offset + 5..offset + total_size];

        match msg_type {
            b'Q' => {
                // Simple Query: query string (null-terminated)
                if let Some(sql) = extract_cstring(msg_body, 0) {
                    tracker.lock().await.handle_simple_query(sql);
                }
            }

            b'P' => {
                // Parse: statement_name + query + param_types
                if let Some((stmt_name, rest)) = extract_cstring_with_rest(msg_body, 0) {
                    if let Some(sql) = extract_cstring(rest, 0) {
                        tracker.lock().await.handle_parse(stmt_name, sql);
                    }
                }
            }

            b'B' => {
                // Bind: portal_name + statement_name + params...
                if let Some((portal, rest)) = extract_cstring_with_rest(msg_body, 0) {
                    if let Some(stmt) = extract_cstring(rest, 0) {
                        tracker.lock().await.handle_bind(portal, stmt);
                    }
                }
            }

            b'E' => {
                // Execute: portal_name + max_rows
                if let Some(portal) = extract_cstring(msg_body, 0) {
                    tracker.lock().await.handle_execute(&portal);
                }
            }

            b'C' => {
                // Close: type (S/P) + name
                if msg_body.len() >= 2 {
                    let close_type = msg_body[0];
                    if let Some(name) = extract_cstring(msg_body, 1) {
                        tracker.lock().await.handle_close(close_type, &name);
                    }
                }
            }

            b'S' => {
                // Sync: flush pending errors
                trace!("sync");
            }

            _ => {
                // Ignore other message types
            }
        }

        offset += total_size;
    }
}

/// Parse PostgreSQL server messages
async fn parse_server_messages(
    buf: &[u8],
    tracker: &Arc<Mutex<QueryTracker>>,
    stats: &Stats,
) -> usize {
    let mut offset = 0;

    while offset + 5 <= buf.len() {
        let msg_type = buf[offset];

        let msg_len = u32::from_be_bytes([
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
            buf[offset + 4],
        ]) as usize;

        // Sanity check
        if msg_len < 4 || msg_len > 1_000_000 {
            warn!(msg_type = ?char::from(msg_type), msg_len, "invalid_message_length");
            offset += 1;
            continue;
        }

        let total_size = 1 + msg_len;
        if offset + total_size > buf.len() {
            break; // Incomplete - wait for more data
        }

        match msg_type {
            b'C' => {
                // CommandComplete - query finished successfully
                let mut tracker = tracker.lock().await;
                if let Some(completed) = tracker.handle_command_complete() {
                    let mut stats = stats.write().await;
                    stats.record(&completed.fingerprint, &completed.sql, completed.duration);
                }
            }

            b'E' => {
                // ErrorResponse - query failed
                tracker.lock().await.handle_error();
            }

            b'Z' => {
                // ReadyForQuery - transaction boundary
                // This is NOT query completion - just means "ready for next command"
                trace!("ready_for_query");
            }

            _ => {
                // Ignore RowDescription, DataRow, etc.
            }
        }

        offset += total_size;
    }

    offset
}

/// Check for ReadyForQuery message in buffer
fn contains_ready_for_query(buf: &[u8]) -> bool {
    let mut i = 0;
    while i + 5 <= buf.len() {
        if buf[i] == b'Z' {
            let len = u32::from_be_bytes([buf[i + 1], buf[i + 2], buf[i + 3], buf[i + 4]]);
            if len == 5 {
                return true;
            }
        }
        i += 1;
    }
    false
}

/// Extract null-terminated string from buffer
fn extract_cstring(buf: &[u8], offset: usize) -> Option<String> {
    if offset >= buf.len() {
        return None;
    }

    let end = buf[offset..].iter().position(|&b| b == 0)?;
    std::str::from_utf8(&buf[offset..offset + end])
        .ok()
        .map(|s| s.to_string())
}

/// Extract null-terminated string and return rest of buffer
fn extract_cstring_with_rest(buf: &[u8], offset: usize) -> Option<(String, &[u8])> {
    if offset >= buf.len() {
        return None;
    }

    let end = buf[offset..].iter().position(|&b| b == 0)?;
    let s = std::str::from_utf8(&buf[offset..offset + end]).ok()?;
    let rest = &buf[offset + end + 1..];
    Some((s.to_string(), rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_cstring() {
        let buf = b"hello\0world\0";
        assert_eq!(extract_cstring(buf, 0), Some("hello".to_string()));
        assert_eq!(extract_cstring(buf, 6), Some("world".to_string()));
    }

    #[test]
    fn test_extract_cstring_with_rest() {
        let buf = b"stmt1\0SELECT 1\0more";
        let (name, rest) = extract_cstring_with_rest(buf, 0).unwrap();
        assert_eq!(name, "stmt1");
        assert_eq!(&rest[..8], b"SELECT 1");
    }

    #[test]
    fn test_contains_ready_for_query() {
        // Z + length(5) + status byte
        let buf = [b'Z', 0, 0, 0, 5, b'I'];
        assert!(contains_ready_for_query(&buf));

        let buf = [b'X', 0, 0, 0, 5, b'I'];
        assert!(!contains_ready_for_query(&buf));
    }
}
