use std::time::Instant;
use tracing::{debug, trace, warn};

#[derive(Debug, Default)]
pub struct QueryTracker {
    // For simple query protocol: one query active at a time
    pub current_query: Option<(String, Instant)>,

    // For extended query protocol: track prepared statements
    last_prepared: Option<String>,
}

impl QueryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called when we see Parse (P) or Query (Q) messages
    pub fn start_query(&mut self, sql: String) {
        trace!(sql = %sql, "query_start");

        // Store for later Execute
        self.last_prepared = Some(sql.clone());

        // Track as current query
        self.current_query = Some((sql, Instant::now()));
    }

    /// Called when we see Execute (E) message
    pub fn start_execute(&mut self) {
        if let Some(ref sql) = self.last_prepared {
            trace!(sql = %sql, "execute_start");
            self.current_query = Some((sql.clone(), Instant::now()));
        } else {
            warn!("Execute without prior Parse");
        }
    }

    /// Called when we see CommandComplete (C) OR ReadyForQuery (Z)
    pub fn complete_query(&mut self) -> Option<(String, std::time::Duration)> {
        self.current_query.take().map(|(sql, start)| {
            let duration = start.elapsed();
            debug!(
                duration_ms = duration.as_millis(),
                sql = %sql,
                "query_complete"
            );
            (sql, duration)
        })
    }
}
