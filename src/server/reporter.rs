use super::metrics::Stats;
use crate::{database::handler::PostgresCredentials, server::metrics::QueryStatistics};
use std::time::Duration;
use tracing::{error, info};

pub struct MetricsReporter {
    query_stats: Stats,
    credentials: PostgresCredentials,
    interval: Duration,
}

impl MetricsReporter {
    pub fn new(query_stats: Stats, credentials: PostgresCredentials, interval: u64) -> Self {
        Self {
            query_stats,
            credentials,
            interval: Duration::from_secs(interval),
        }
    }

    /// Start the periodic reporting loop
    pub fn start(self) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(self.interval).await;
                self.report().await;
            }
        });
    }

    async fn report(&self) {
        info!("=== Metrics Report ===");
        // Atomically swap out current stats
        let snapshot = {
            let mut stats = self.query_stats.write().await;
            let snapshot = std::mem::replace(&mut *stats, QueryStatistics::new());
            snapshot
        };

        // work with snapshot without holding lock
        info!("{}", snapshot.get_slow_query_report(10));
        info!("{}", snapshot.get_summary_report());

        if let Err(e) = self.write_snapshot_to_database(snapshot).await {
            error!("Failed to write metrics: {}", e);
        }
    }

    async fn write_snapshot_to_database(&self, snapshot: QueryStatistics) -> anyhow::Result<()> {
        use crate::database::handler::PostgresHandler;

        let handler = PostgresHandler::new(&self.credentials.conn_str).await?;
        handler.write_metrics(snapshot).await?;

        Ok(())
    }
}
