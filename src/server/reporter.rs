use super::metrics::Stats;
use crate::database::handler::PostgresCredentials;
use std::time::Duration;
use tracing::{error, info, warn};

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

        let stats = self.query_stats.read().await;

        // Print slow query analysis
        let slow_report = stats.get_slow_query_report(100);
        if slow_report.contains("No slow queries") {
            info!("{}", slow_report);
        } else {
            warn!("{}", slow_report);
        }

        // Print summary
        info!("{}", stats.get_summary_report());

        drop(stats); // Release read lock before write operation

        // Write to database
        if let Err(e) = self.write_to_database().await {
            error!("Failed to write metrics to database: {}", e);
        } else {
            info!("✓ Metrics written to database");
        }
    }

    async fn write_to_database(&self) -> anyhow::Result<()> {
        use crate::database::handler::PostgresHandler;

        let handler = PostgresHandler::new(&self.credentials.conn_str).await?;
        handler.write_metrics(self.query_stats.clone()).await?;

        // Clear stats after successful write
        let mut stats = self.query_stats.write().await;
        stats.clear();

        Ok(())
    }
}
