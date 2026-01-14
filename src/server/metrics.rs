use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub type Stats = Arc<RwLock<QueryStatistics>>;

/// Query statistics aggregator
///
/// Key design decisions:
/// 1. Use fingerprint as primary key - queries that differ only in literals are grouped
/// 2. Store one example SQL per fingerprint for debugging
/// 3. Use LRU-style eviction based on last_seen, not speed
/// 4. Pre-allocate to avoid runtime allocations
#[derive(Debug)]
pub struct QueryStatistics {
    /// Fingerprint -> aggregated stats
    queries: HashMap<String, QueryStat>,

    /// Configuration
    config: StatsConfig,

    /// Total queries seen (even if evicted)
    total_queries_seen: u64,

    /// Total queries evicted
    total_evicted: u64,
}

#[derive(Debug, Clone)]
pub struct StatsConfig {
    /// Maximum unique query patterns to track
    pub max_queries: usize,

    /// Evict queries not seen in this duration
    pub eviction_age: Duration,

    /// Minimum executions to survive eviction
    pub min_executions_for_retention: u64,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self {
            max_queries: 500,
            eviction_age: Duration::from_secs(3600), // 1 hour
            min_executions_for_retention: 10,
        }
    }
}

impl QueryStatistics {
    pub fn new() -> Self {
        Self::with_config(StatsConfig::default())
    }

    pub fn with_config(config: StatsConfig) -> Self {
        Self {
            queries: HashMap::with_capacity(config.max_queries),
            config,
            total_queries_seen: 0,
            total_evicted: 0,
        }
    }

    /// Record a completed query
    pub fn record(&mut self, fingerprint: &str, sql: &str, duration: Duration) {
        self.total_queries_seen += 1;

        let entry = self
            .queries
            .entry(fingerprint.to_string())
            .or_insert_with(|| QueryStat::new(fingerprint.to_string(), sql.to_string()));

        entry.record(duration);

        // Eviction check - only when at capacity
        if self.queries.len() >= self.config.max_queries {
            self.evict_stale();
        }
    }

    /// Evict old/infrequent queries
    fn evict_stale(&mut self) {
        let now = std::time::Instant::now();
        let eviction_age = self.config.eviction_age;
        let min_execs = self.config.min_executions_for_retention;

        let before_count = self.queries.len();

        self.queries.retain(|_, stat| {
            let age = now.duration_since(stat.last_seen);

            // Keep if: recently seen OR high execution count
            age < eviction_age || stat.count >= min_execs
        });

        let evicted = before_count - self.queries.len();
        self.total_evicted += evicted as u64;

        if evicted > 0 {
            tracing::debug!(
                evicted,
                remaining = self.queries.len(),
                "evicted_stale_queries"
            );
        }
    }

    /// Get all statistics for persistence
    pub fn drain_for_persistence(&mut self) -> Vec<QueryStat> {
        self.queries.drain().map(|(_, v)| v).collect()
    }

    /// Take a snapshot without draining
    pub fn snapshot(&self) -> Vec<QueryStat> {
        self.queries.values().cloned().collect()
    }

    /// Get queries by various criteria
    pub fn get_slowest(&self, limit: usize) -> Vec<&QueryStat> {
        let mut sorted: Vec<_> = self.queries.values().collect();
        sorted.sort_by(|a, b| {
            b.avg_duration_secs()
                .partial_cmp(&a.avg_duration_secs())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.truncate(limit);
        sorted
    }

    pub fn get_most_frequent(&self, limit: usize) -> Vec<&QueryStat> {
        let mut sorted: Vec<_> = self.queries.values().collect();
        sorted.sort_by(|a, b| b.count.cmp(&a.count));
        sorted.truncate(limit);
        sorted
    }

    pub fn get_highest_total_time(&self, limit: usize) -> Vec<&QueryStat> {
        let mut sorted: Vec<_> = self.queries.values().collect();
        sorted.sort_by(|a, b| b.total_duration.cmp(&a.total_duration));
        sorted.truncate(limit);
        sorted
    }

    /// Get queries slower than threshold
    pub fn get_slow_queries(&self, threshold: Duration) -> Vec<&QueryStat> {
        self.queries
            .values()
            .filter(|q| q.avg_duration_secs() > threshold.as_secs_f64())
            .collect()
    }

    /// Summary stats
    pub fn summary(&self) -> StatsSummary {
        StatsSummary {
            unique_patterns: self.queries.len(),
            total_queries_seen: self.total_queries_seen,
            total_evicted: self.total_evicted,
            total_duration: self.queries.values().map(|q| q.total_duration).sum(),
        }
    }
}

impl Default for QueryStatistics {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct QueryStat {
    /// Stable fingerprint (queries differing only in literals have same fingerprint)
    pub fingerprint: String,

    /// Example SQL (first one seen with this fingerprint)
    pub example_sql: String,

    /// Execution count
    pub count: u64,

    /// Total time spent
    pub total_duration: Duration,

    /// Minimum execution time
    pub min_duration: Duration,

    /// Maximum execution time  
    pub max_duration: Duration,

    /// Last time this query pattern was seen
    pub last_seen: std::time::Instant,

    /// First time this query pattern was seen
    pub first_seen: std::time::Instant,
}

impl QueryStat {
    pub fn new(fingerprint: String, example_sql: String) -> Self {
        let now = std::time::Instant::now();
        Self {
            fingerprint,
            example_sql,
            count: 0,
            total_duration: Duration::ZERO,
            min_duration: Duration::MAX,
            max_duration: Duration::ZERO,
            last_seen: now,
            first_seen: now,
        }
    }

    pub fn record(&mut self, duration: Duration) {
        self.count += 1;
        self.total_duration += duration;
        self.min_duration = self.min_duration.min(duration);
        self.max_duration = self.max_duration.max(duration);
        self.last_seen = std::time::Instant::now();
    }

    pub fn avg_duration_secs(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.total_duration.as_secs_f64() / self.count as f64
        }
    }

    pub fn avg_duration_ms(&self) -> f64 {
        self.avg_duration_secs() * 1000.0
    }

    /// For database persistence
    pub fn to_db_row(&self) -> QueryStatRow {
        QueryStatRow {
            fingerprint: self.fingerprint.clone(),
            example_sql: self.example_sql.clone(),
            count: self.count as i64,
            total_duration_secs: self.total_duration.as_secs_f32(),
            min_duration_secs: self.min_duration.as_secs_f32(),
            max_duration_secs: self.max_duration.as_secs_f32(),
            avg_duration_secs: self.avg_duration_secs() as f32,
        }
    }
}

/// Row format for database persistence
#[derive(Debug, Clone)]
pub struct QueryStatRow {
    pub fingerprint: String,
    pub example_sql: String,
    pub count: i64,
    pub total_duration_secs: f32,
    pub min_duration_secs: f32,
    pub max_duration_secs: f32,
    pub avg_duration_secs: f32,
}

#[derive(Debug)]
pub struct StatsSummary {
    pub unique_patterns: usize,
    pub total_queries_seen: u64,
    pub total_evicted: u64,
    pub total_duration: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_aggregate() {
        let mut stats = QueryStatistics::new();

        // Same fingerprint, different literal values
        let fp = "abc123";
        stats.record(
            fp,
            "SELECT * FROM users WHERE id = 1",
            Duration::from_millis(10),
        );
        stats.record(
            fp,
            "SELECT * FROM users WHERE id = 2",
            Duration::from_millis(20),
        );
        stats.record(
            fp,
            "SELECT * FROM users WHERE id = 3",
            Duration::from_millis(30),
        );

        assert_eq!(stats.queries.len(), 1);

        let stat = stats.queries.get(fp).unwrap();
        assert_eq!(stat.count, 3);
        assert_eq!(stat.total_duration, Duration::from_millis(60));
        assert_eq!(stat.min_duration, Duration::from_millis(10));
        assert_eq!(stat.max_duration, Duration::from_millis(30));
        assert!((stat.avg_duration_ms() - 20.0).abs() < 0.1);
    }

    #[test]
    fn test_eviction() {
        let config = StatsConfig {
            max_queries: 5,
            eviction_age: Duration::from_millis(1),
            min_executions_for_retention: 100,
        };

        let mut stats = QueryStatistics::with_config(config);

        // Fill it up
        for i in 0..10 {
            stats.record(
                &format!("fp_{}", i),
                &format!("SELECT {}", i),
                Duration::from_millis(1),
            );
            std::thread::sleep(Duration::from_millis(2));
        }

        // Should have evicted some
        assert!(stats.queries.len() <= 5);
        assert!(stats.total_evicted > 0);
    }

    #[test]
    fn test_get_slowest() {
        let mut stats = QueryStatistics::new();

        stats.record("fast", "SELECT 1", Duration::from_millis(1));
        stats.record("medium", "SELECT 2", Duration::from_millis(50));
        stats.record("slow", "SELECT 3", Duration::from_millis(100));

        let slowest = stats.get_slowest(2);
        assert_eq!(slowest.len(), 2);
        assert_eq!(slowest[0].fingerprint, "slow");
        assert_eq!(slowest[1].fingerprint, "medium");
    }

    #[test]
    fn test_snapshot_vs_drain() {
        let mut stats = QueryStatistics::new();
        stats.record("fp1", "SELECT 1", Duration::from_millis(10));

        // Snapshot doesn't remove
        let snap = stats.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(stats.queries.len(), 1);

        // Drain removes
        let drained = stats.drain_for_persistence();
        assert_eq!(drained.len(), 1);
        assert_eq!(stats.queries.len(), 0);
    }
}
