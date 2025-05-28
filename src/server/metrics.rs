use lazy_static::lazy_static;
use std::{
    collections::BTreeMap,
    fmt::Display,
    sync::{Arc, RwLock},
    time::Duration,
};

use crate::executor::handler::{PostgresCredentials, PostgresHandler};
use tracing::error;

lazy_static! {
    pub static ref QUERY_STATS: Arc<RwLock<QueryStatistics>> =
        Arc::new(RwLock::new(QueryStatistics::new()));
}

#[derive(Debug)]
pub struct QueryStatistics {
    /// BTreeMap for ordered iteration by key
    pub queries: BTreeMap<String, QueryStat>,

    /// Maximum number of slow queries to storage
    max_queries: usize,
}

impl QueryStatistics {
    fn new() -> Self {
        Self {
            queries: BTreeMap::new(),
            max_queries: 100, // storage 100 queries
        }
    }

    pub fn get_report(&self) -> String {
        if self.queries.is_empty() {
            return "No queries captured.\n".to_string()
        }

        let mut report = String::new();

        report.push_str("\n========================\n");
        report.push_str("      Queries Summary\n");
        report.push_str("========================\n\n");

        for (_, query) in self.queries.iter() {
            report.push_str(&format!("{}", query));
        }

        report
    }

    pub async fn write_to_database() -> anyhow::Result<()> {
        let conn_str = PostgresCredentials::connection_string()?;

        match PostgresHandler::new(&conn_str).await {
            Ok(handler) => {
                if let Err(result) = handler.write_metrics().await {
                    error!("Error inserting data to database: {}", result);
                }
            }
            Err(err) => {
                error!("Failed to create database handler: {}", err);
            }
        }

        Ok(())
    }

    pub fn record_query(&mut self, query: &str, duration: Duration) {
        let entry = self
            .queries
            .entry(query.to_string())
            .or_insert_with(QueryStat::new);

        entry.query = query.to_string();
        entry.count += 1;
        entry.total_duration = entry.total_duration + duration;
        entry.min_duration = if !entry.min_duration.is_zero() {
            entry.min_duration.min(duration)
        } else {
            duration
        };
        entry.max_duration = entry.max_duration.max(duration);
        entry.avg_duration = entry.total_duration.as_secs_f64() / entry.count as f64;

        // Prune if required.
        if self.queries.len() > self.max_queries {
            if let Some(fastest) = self.get_fastest() {
                self.queries.remove(&fastest);
            }
        }
    }

    fn get_fastest(&self) -> Option<String> {
        self.queries
            .iter()
            .min_by(|a, b| a.1.max_duration.cmp(&b.1.max_duration))
            .map(|q| q.0.clone())
    }
}

#[derive(Debug)]
pub struct QueryStat {
    pub query: String,
    pub count: usize,
    pub total_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub avg_duration: f64,
}

impl QueryStat {
    fn new() -> Self {
        Self {
            query: String::new(),
            count: 0,
            total_duration: Duration::from_secs(0),
            min_duration: Duration::from_secs(0),
            max_duration: Duration::from_secs(0),
            avg_duration: 0.0,
        }
    }
}

impl Display for QueryStat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write! {
        f,
                "QUERY: {}\n\ncount: {}, total_time: {}, average: {}, min: {}, max: {}\n\n-------------------------------\n\n",
                self.query,
                self.count,
                self.total_duration.as_secs_f64(),
                self.avg_duration,
                self.min_duration.as_secs_f64(),
                self.max_duration.as_secs_f64()
            }
    }
}
