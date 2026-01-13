use std::collections::VecDeque;
use std::time::Instant;
use tracing::{debug, info};

/// Tracks in-flight queries in the PostgreSQL protocol
#[derive(Debug, Default)]
pub struct QueryTracker {
    pub pending_queries: VecDeque<(String, Instant)>,
    last_prepared: Option<String>,
}

impl QueryTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Start tracking a new query (from Parse or Query message)
    pub fn start_query(&mut self, sql: String) {
        debug!(%sql, "starting");
        self.last_prepared = Some(sql.clone());
        self.pending_queries.push_back((sql, Instant::now()));
    }

    /// Start tracking an Execute (reuses last prepared statement)
    pub fn start_execute(&mut self) {
        if let Some(ref sql) = self.last_prepared {
            debug!(%sql, "starting");
            self.pending_queries
                .push_back((sql.clone(), Instant::now()));
        }
    }

    /// Complete the oldest in-flight query, returning (sql, duration)
    pub fn complete_query(&mut self) -> Option<(String, std::time::Duration)> {
        let completed = self
            .pending_queries
            .pop_front()
            .map(|(sql, start)| (sql, start.elapsed()));

        if let Some((ref sql, elapsed)) = completed {
            info!(?elapsed, %sql, "completed");
        }

        completed
    }
}
