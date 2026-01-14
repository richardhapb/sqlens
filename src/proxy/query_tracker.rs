use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, trace, warn};

/// Tracks queries through their full lifecycle in PostgreSQL wire protocol.
///
/// PostgreSQL has two query protocols:
/// 1. Simple Query (Q) - single query, blocking
/// 2. Extended Query (P/B/D/E/S) - prepared statements, can pipeline
///
/// This tracker handles both.
#[derive(Debug)]
pub struct QueryTracker {
    /// Named prepared statements: name -> (sql, fingerprint)
    /// Empty string key = unnamed statement
    prepared_statements: HashMap<String, PreparedStatement>,

    /// Named portals: portal_name -> statement_name
    /// Portal binds parameters to a prepared statement
    portals: HashMap<String, String>,

    /// In-flight queries awaiting completion
    /// Key: query fingerprint, Value: execution context
    /// Using fingerprint as key allows multiple instances of same query pattern
    in_flight: Vec<InFlightQuery>,

    /// For simple query protocol - only one active at a time
    simple_query: Option<InFlightQuery>,
}

#[derive(Debug, Clone)]
struct PreparedStatement {
    sql: String,
    fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct InFlightQuery {
    pub sql: String,
    pub fingerprint: String,
    pub started_at: Instant,
}

#[derive(Debug, Clone)]
pub struct CompletedQuery {
    pub sql: String,
    pub fingerprint: String,
    pub duration: Duration,
}

impl Default for QueryTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryTracker {
    pub fn new() -> Self {
        Self {
            prepared_statements: HashMap::with_capacity(16),
            portals: HashMap::with_capacity(8),
            in_flight: Vec::with_capacity(4),
            simple_query: None,
        }
    }

    /// Handle Simple Query (Q) message
    /// Format: 'Q' + len + query_string + '\0'
    pub fn handle_simple_query(&mut self, sql: String) {
        let fingerprint = compute_fingerprint(&sql);
        trace!(sql = %sql, fingerprint = %fingerprint, "simple_query_start");

        self.simple_query = Some(InFlightQuery {
            sql,
            fingerprint,
            started_at: Instant::now(),
        });
    }

    /// Handle Parse (P) message - creates a prepared statement
    /// Format: 'P' + len + statement_name + '\0' + query + '\0' + param_types
    pub fn handle_parse(&mut self, statement_name: String, sql: String) {
        let fingerprint = compute_fingerprint(&sql);
        trace!(
            statement = %statement_name,
            sql = %sql,
            fingerprint = %fingerprint,
            "parse"
        );

        self.prepared_statements
            .insert(statement_name, PreparedStatement { sql, fingerprint });
    }

    /// Handle Bind (B) message - creates a portal from a prepared statement
    /// Format: 'B' + len + portal_name + '\0' + statement_name + '\0' + params...
    pub fn handle_bind(&mut self, portal_name: String, statement_name: String) {
        trace!(portal = %portal_name, statement = %statement_name, "bind");
        self.portals.insert(portal_name, statement_name);
    }

    /// Handle Execute (E) message - executes a portal
    /// Format: 'E' + len + portal_name + '\0' + max_rows
    pub fn handle_execute(&mut self, portal_name: &str) {
        // Look up portal -> statement -> sql
        let statement_name = match self.portals.get(portal_name) {
            Some(name) => name.clone(),
            None => {
                // Unnamed portal implicitly uses unnamed statement
                String::new()
            }
        };

        let stmt = match self.prepared_statements.get(&statement_name) {
            Some(stmt) => stmt.clone(),
            None => {
                warn!(
                    portal = %portal_name,
                    statement = %statement_name,
                    "execute_missing_statement"
                );
                return;
            }
        };

        trace!(
            portal = %portal_name,
            fingerprint = %stmt.fingerprint,
            "execute_start"
        );

        self.in_flight.push(InFlightQuery {
            sql: stmt.sql,
            fingerprint: stmt.fingerprint,
            started_at: Instant::now(),
        });
    }

    /// Handle CommandComplete (C) message - a query finished
    /// Returns the completed query info if we were tracking it
    pub fn handle_command_complete(&mut self) -> Option<CompletedQuery> {
        // Simple query takes priority
        if let Some(query) = self.simple_query.take() {
            let duration = query.started_at.elapsed();
            debug!(
                duration_ms = duration.as_millis(),
                fingerprint = %query.fingerprint,
                "simple_query_complete"
            );
            return Some(CompletedQuery {
                sql: query.sql,
                fingerprint: query.fingerprint,
                duration,
            });
        }

        // Extended query protocol - FIFO order
        if !self.in_flight.is_empty() {
            let query = self.in_flight.remove(0);
            let duration = query.started_at.elapsed();
            debug!(
                duration_ms = duration.as_millis(),
                fingerprint = %query.fingerprint,
                "extended_query_complete"
            );
            return Some(CompletedQuery {
                sql: query.sql,
                fingerprint: query.fingerprint,
                duration,
            });
        }

        None
    }

    /// Handle ErrorResponse (E from server) - query failed
    pub fn handle_error(&mut self) {
        // Clear the oldest in-flight query (the one that errored)
        if let Some(query) = self.simple_query.take() {
            warn!(fingerprint = %query.fingerprint, "simple_query_error");
            return;
        }

        if !self.in_flight.is_empty() {
            let query = self.in_flight.remove(0);
            warn!(fingerprint = %query.fingerprint, "extended_query_error");
        }
    }

    /// Handle Close (C from client) - close prepared statement or portal
    pub fn handle_close(&mut self, close_type: u8, name: &str) {
        match close_type {
            b'S' => {
                trace!(statement = %name, "close_statement");
                self.prepared_statements.remove(name);
            }
            b'P' => {
                trace!(portal = %name, "close_portal");
                self.portals.remove(name);
            }
            _ => {}
        }
    }

    /// Reset state (e.g., on connection close or Sync after error)
    pub fn reset(&mut self) {
        self.in_flight.clear();
        self.simple_query = None;
        // Keep prepared statements - they persist until explicitly closed
    }

    /// Get count of in-flight queries (for monitoring)
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len() + self.simple_query.is_some() as usize
    }
}

/// Compute a stable fingerprint for query normalization
/// Queries that differ only in literal values get the same fingerprint
pub fn compute_fingerprint(sql: &str) -> String {
    match pg_query::fingerprint(sql) {
        Ok(fp) => fp.value.to_string(),
        Err(e) => {
            // Fallback: hash the normalized query
            warn!(error = %e, sql = %sql, "fingerprint_failed");
            format!("raw:{:x}", md5_hash(sql.trim()))
        }
    }
}

fn md5_hash(s: &str) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_simple_query_flow() {
        let mut tracker = QueryTracker::new();

        tracker.handle_simple_query("SELECT * FROM users WHERE id = 1".to_string());
        sleep(Duration::from_millis(10));

        let result = tracker.handle_command_complete();
        assert!(result.is_some());

        let completed = result.unwrap();
        assert!(completed.sql.contains("SELECT"));
        assert!(!completed.fingerprint.is_empty());
        assert!(completed.duration >= Duration::from_millis(10));
    }

    #[test]
    fn test_extended_query_flow() {
        let mut tracker = QueryTracker::new();

        // Parse (unnamed statement)
        tracker.handle_parse(String::new(), "SELECT $1::int".to_string());

        // Bind (unnamed portal to unnamed statement)
        tracker.handle_bind(String::new(), String::new());

        // Execute
        tracker.handle_execute("");
        sleep(Duration::from_millis(10));

        // CommandComplete
        let result = tracker.handle_command_complete();
        assert!(result.is_some());
        assert!(result.unwrap().fingerprint.len() > 0);
    }

    #[test]
    fn test_named_prepared_statement() {
        let mut tracker = QueryTracker::new();

        // Parse named statement
        tracker.handle_parse(
            "my_stmt".to_string(),
            "SELECT * FROM orders WHERE id = $1".to_string(),
        );

        // Execute it multiple times
        for _ in 0..3 {
            tracker.handle_bind("".to_string(), "my_stmt".to_string());
            tracker.handle_execute("");
            let result = tracker.handle_command_complete();
            assert!(result.is_some());
        }
    }

    #[test]
    fn test_pipelined_queries() {
        let mut tracker = QueryTracker::new();

        // Parse two statements
        tracker.handle_parse("stmt1".to_string(), "SELECT 1".to_string());
        tracker.handle_parse("stmt2".to_string(), "SELECT 2".to_string());

        // Pipeline: bind and execute both before waiting for results
        tracker.handle_bind("p1".to_string(), "stmt1".to_string());
        tracker.handle_execute("p1");

        tracker.handle_bind("p2".to_string(), "stmt2".to_string());
        tracker.handle_execute("p2");

        // Now get results - should be FIFO
        let r1 = tracker.handle_command_complete().unwrap();
        assert!(r1.sql.contains("SELECT 1"));

        let r2 = tracker.handle_command_complete().unwrap();
        assert!(r2.sql.contains("SELECT 2"));
    }

    #[test]
    fn test_fingerprint_normalization() {
        let f1 = compute_fingerprint("SELECT * FROM users WHERE id = 1");
        let f2 = compute_fingerprint("SELECT * FROM users WHERE id = 999");
        let f3 = compute_fingerprint("SELECT * FROM orders WHERE id = 1");

        assert_eq!(f1, f2, "Same query pattern should have same fingerprint");
        assert_ne!(
            f1, f3,
            "Different tables should have different fingerprints"
        );
    }

    #[test]
    fn test_error_handling() {
        let mut tracker = QueryTracker::new();

        tracker.handle_simple_query("SELECT * FROM nonexistent".to_string());
        tracker.handle_error();

        // Should be cleared
        assert!(tracker.handle_command_complete().is_none());
    }
}
