use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub type Stats = Arc<RwLock<QueryStatistics>>;

#[derive(Debug)]
pub struct QueryStatistics {
    pub queries: BTreeMap<String, QueryStat>,
    max_queries: usize,
}

impl QueryStatistics {
    pub fn new() -> Self {
        Self {
            queries: BTreeMap::new(),
            max_queries: 100,
        }
    }

    /// Record a query execution
    pub fn record_query(&mut self, query: &str, duration: Duration) {
        let normalized = normalize_query(query);
        let entry = self.queries.entry(normalized).or_default();

        entry.query = query.to_string();
        entry.count += 1;
        entry.total_duration += duration;
        entry.min_duration = if entry.min_duration.is_zero() {
            duration
        } else {
            entry.min_duration.min(duration)
        };
        entry.max_duration = entry.max_duration.max(duration);
        entry.avg_duration = entry.total_duration.as_secs_f64() / entry.count as f64;

        // Prune if needed
        if self.queries.len() > self.max_queries
            && let Some(fastest) = self.get_fastest_query()
        {
            self.queries.remove(&fastest);
        }
    }

    /// Get queries slower than threshold
    pub fn get_slow_queries(&self, threshold_ms: u64) -> Vec<&QueryStat> {
        let mut slow: Vec<_> = self
            .queries
            .values()
            .filter(|q| {
                let avg_ms = (q.avg_duration * 1000.0) as u64;
                avg_ms > threshold_ms || q.max_duration.as_millis() as u64 > threshold_ms * 2
            })
            .collect();

        slow.sort_by(|a, b| b.total_duration.cmp(&a.total_duration));
        slow
    }

    /// Generate a summary report
    pub fn get_summary_report(&self) -> String {
        if self.queries.is_empty() {
            return "No queries captured.\n".to_string();
        }

        let mut report = String::from("\n=== Query Statistics ===\n\n");

        for (i, stat) in self.queries.values().enumerate() {
            report.push_str(&format!(
                "{}. [{} executions] avg: {:.2}ms, max: {:.2}ms, total: {:.2}s\n   {}\n\n",
                i + 1,
                stat.count,
                stat.avg_duration * 1000.0,
                stat.max_duration.as_secs_f64() * 1000.0,
                stat.total_duration.as_secs_f64(),
                truncate_query(&stat.query, 200)
            ));
        }

        report
    }

    /// Generate slow query report
    pub fn get_slow_query_report(&self, threshold_ms: u64) -> String {
        let slow = self.get_slow_queries(threshold_ms);

        if slow.is_empty() {
            return format!("No slow queries (threshold: {}ms)\n", threshold_ms);
        }

        let mut report = format!("\n=== Slow Queries (>{}ms) ===\n\n", threshold_ms);

        for (i, stat) in slow.iter().enumerate() {
            report.push_str(&format!(
                "{}. [{}x] avg: {:.0}ms | max: {:.0}ms | total: {:.2}s\n   {}\n\n",
                i + 1,
                stat.count,
                stat.avg_duration * 1000.0,
                stat.max_duration.as_secs_f64() * 1000.0,
                stat.total_duration.as_secs_f64(),
                truncate_query(&stat.query, 500)
            ));
        }

        report
    }

    pub fn clear(&mut self) {
        self.queries.clear();
    }

    fn get_fastest_query(&self) -> Option<String> {
        self.queries
            .iter()
            .min_by(|a, b| a.1.avg_duration.partial_cmp(&b.1.avg_duration).unwrap())
            .map(|(k, _)| k.clone())
    }
}

#[derive(Debug, Default, Clone)]
pub struct QueryStat {
    pub query: String,
    pub count: usize,
    pub total_duration: Duration,
    pub min_duration: Duration,
    pub max_duration: Duration,
    pub avg_duration: f64,
}

// Helper functions
fn normalize_query(sql: &str) -> String {
    sql.trim().to_string()
}

fn truncate_query(query: &str, max_len: usize) -> String {
    if query.len() > max_len {
        format!("{}...", &query[..max_len])
    } else {
        query.to_string()
    }
}
