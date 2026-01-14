use crate::database::handler::PostgresHandler;
use crate::server::metrics::Stats;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Background reporter that periodically persists metrics to database
pub struct MetricsReporter {
    stats: Stats,
    handler: Arc<Mutex<Option<PostgresHandler>>>,
    interval: Duration,
    connection_string: String,
}

impl MetricsReporter {
    pub fn new(stats: Stats, connection_string: String, interval_secs: u64) -> Self {
        Self {
            stats,
            handler: Arc::new(Mutex::new(None)),
            interval: Duration::from_secs(interval_secs),
            connection_string,
        }
    }

    /// Start the background reporter
    /// Returns a handle to the spawned task
    pub fn start(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            self.run().await;
        })
    }

    async fn run(self) {
        info!(interval_secs = self.interval.as_secs(), "reporter_started");

        // Initial connection
        if let Err(e) = self.ensure_connected().await {
            error!(error = %e, "initial_connection_failed");
        }

        let mut interval = tokio::time::interval(self.interval);

        loop {
            interval.tick().await;

            if let Err(e) = self.flush().await {
                warn!(error = %e, "flush_failed");
            }
        }
    }

    async fn ensure_connected(&self) -> anyhow::Result<()> {
        let mut guard = self.handler.lock().await;

        if let Some(ref handler) = *guard {
            // Check if connection is still alive
            if handler.health_check().await.is_ok() {
                return Ok(());
            }
            warn!("connection_lost_reconnecting");
        }

        // Connect/reconnect
        let handler = PostgresHandler::new(&self.connection_string).await?;
        *guard = Some(handler);
        info!("database_connected");
        Ok(())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        // Ensure connection
        self.ensure_connected().await?;

        // Take snapshot (non-destructive)
        let snapshot = {
            let stats = self.stats.read().await;
            stats.snapshot()
        };

        if snapshot.is_empty() {
            debug!("no_metrics_to_flush");
            return Ok(());
        }

        debug!(count = snapshot.len(), "flushing_metrics");

        // Convert to DB rows
        let rows: Vec<_> = snapshot.iter().map(|s| s.to_db_row()).collect();

        // Persist
        let handler = self.handler.lock().await;
        if let Some(ref h) = *handler {
            let affected = h.upsert_metrics(rows).await?;
            info!(affected, "metrics_flushed");
        }

        Ok(())
    }
}

/// Standalone function to flush stats immediately
/// Useful for graceful shutdown
pub async fn flush_now(stats: &Stats, handler: &PostgresHandler) -> anyhow::Result<usize> {
    let snapshot = stats.read().await.snapshot();

    if snapshot.is_empty() {
        return Ok(0);
    }

    let rows: Vec<_> = snapshot.iter().map(|s| s.to_db_row()).collect();
    handler.upsert_metrics(rows).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::metrics::QueryStatistics;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_reporter_creation() {
        let stats = Arc::new(RwLock::new(QueryStatistics::new()));
        let reporter = MetricsReporter::new(stats, "postgres://test".to_string(), 60);
        assert_eq!(reporter.interval, Duration::from_secs(60));
    }
}
