use crate::Args;
use sqlx::PgPool;
use tracing::{debug, info, instrument};

use crate::server::metrics::QueryStatRow;

pub struct PostgresHandler {
    pool: PgPool,
}

impl PostgresHandler {
    pub async fn new(connection_string: &str) -> anyhow::Result<Self> {
        debug!("Connecting to metrics database");

        let pool = PgPool::connect(connection_string).await?;

        let handler = Self { pool };
        handler.ensure_schema().await?;

        Ok(handler)
    }

    /// Create schema - fingerprint the primary key concept
    /// Probably this never is executed, because  `sqlx` does the
    /// validation at compile time, but probably we will change
    /// to cached approach for improving CI UX.
    async fn ensure_schema(&self) -> anyhow::Result<()> {
        // Main table: fingerprint as unique identifier
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS query_metrics (
                id BIGSERIAL PRIMARY KEY,
                fingerprint VARCHAR(64) NOT NULL UNIQUE,
                example_sql TEXT NOT NULL,
                execution_count BIGINT NOT NULL DEFAULT 0,
                total_duration_secs DOUBLE PRECISION NOT NULL DEFAULT 0,
                min_duration_secs REAL NOT NULL DEFAULT 0,
                max_duration_secs REAL NOT NULL DEFAULT 0,
                avg_duration_secs REAL NOT NULL DEFAULT 0,
                first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Index for common queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_query_metrics_avg_duration 
            ON query_metrics(avg_duration_secs DESC)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_query_metrics_total_duration 
            ON query_metrics(total_duration_secs DESC)
            "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_query_metrics_count 
            ON query_metrics(execution_count DESC)
            "#,
        )
        .execute(&self.pool)
        .await?;

        info!("Schema verified");
        Ok(())
    }

    /// Batch upsert query metrics
    /// Uses fingerprint for deduplication
    #[instrument(skip(self, rows), fields(batch_size = rows.len()))]
    pub async fn upsert_metrics(&self, rows: Vec<QueryStatRow>) -> anyhow::Result<usize> {
        if rows.is_empty() {
            return Ok(0);
        }

        let len = rows.len();

        // Prepare arrays for UNNEST
        let fingerprints: Vec<_> = rows.iter().map(|r| r.fingerprint.clone()).collect();
        let example_sqls: Vec<_> = rows
            .iter()
            .map(|r| truncate(&r.example_sql, 10000))
            .collect();
        let counts: Vec<i64> = rows.iter().map(|r| r.count).collect();
        let total_durations: Vec<f64> = rows.iter().map(|r| r.total_duration_secs as f64).collect();
        let min_durations: Vec<f32> = rows.iter().map(|r| r.min_duration_secs).collect();
        let max_durations: Vec<f32> = rows.iter().map(|r| r.max_duration_secs).collect();

        let result = sqlx::query(
            r#"
            INSERT INTO query_metrics (
                fingerprint,
                example_sql,
                execution_count,
                total_duration_secs,
                min_duration_secs,
                max_duration_secs,
                avg_duration_secs,
                last_seen
            )
            SELECT 
                u.fingerprint,
                u.example_sql,
                u.execution_count,
                u.total_duration_secs,
                u.min_duration_secs,
                u.max_duration_secs,
                CASE WHEN u.execution_count > 0 
                     THEN u.total_duration_secs / u.execution_count 
                     ELSE 0 
                END as avg_duration_secs,
                NOW()
            FROM UNNEST(
                $1::varchar[],
                $2::text[],
                $3::bigint[],
                $4::double precision[],
                $5::real[],
                $6::real[]
            ) AS u(fingerprint, example_sql, execution_count, total_duration_secs, min_duration_secs, max_duration_secs)
            ON CONFLICT (fingerprint) DO UPDATE SET
                execution_count = query_metrics.execution_count + EXCLUDED.execution_count,
                total_duration_secs = query_metrics.total_duration_secs + EXCLUDED.total_duration_secs,
                min_duration_secs = LEAST(query_metrics.min_duration_secs, EXCLUDED.min_duration_secs),
                max_duration_secs = GREATEST(query_metrics.max_duration_secs, EXCLUDED.max_duration_secs),
                avg_duration_secs = (query_metrics.total_duration_secs + EXCLUDED.total_duration_secs) 
                                  / (query_metrics.execution_count + EXCLUDED.execution_count),
                last_seen = NOW()
            "#,
        )
        .bind(&fingerprints)
        .bind(&example_sqls)
        .bind(&counts)
        .bind(&total_durations)
        .bind(&min_durations)
        .bind(&max_durations)
        .execute(&self.pool)
        .await?;

        let affected = result.rows_affected() as usize;
        info!(
            batch_size = len,
            rows_affected = affected,
            "metrics_persisted"
        );

        Ok(affected)
    }

    /// Get slowest queries by average duration
    pub async fn get_slowest_queries(&self, limit: i64) -> anyhow::Result<Vec<StoredQueryMetric>> {
        let records = sqlx::query_as!(
            StoredQueryMetric,
            r#"
            SELECT 
                fingerprint,
                example_sql,
                execution_count,
                total_duration_secs,
                min_duration_secs,
                max_duration_secs,
                avg_duration_secs
            FROM query_metrics
            ORDER BY avg_duration_secs DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get queries with highest total time (impact queries)
    pub async fn get_highest_impact_queries(
        &self,
        limit: i64,
    ) -> anyhow::Result<Vec<StoredQueryMetric>> {
        let records = sqlx::query_as!(
            StoredQueryMetric,
            r#"
            SELECT 
                fingerprint,
                example_sql,
                execution_count,
                total_duration_secs,
                min_duration_secs,
                max_duration_secs,
                avg_duration_secs
            FROM query_metrics
            ORDER BY total_duration_secs DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get most frequent queries
    pub async fn get_most_frequent_queries(
        &self,
        limit: i64,
    ) -> anyhow::Result<Vec<StoredQueryMetric>> {
        let records = sqlx::query_as!(
            StoredQueryMetric,
            r#"
            SELECT 
                fingerprint,
                example_sql,
                execution_count,
                total_duration_secs,
                min_duration_secs,
                max_duration_secs,
                avg_duration_secs
            FROM query_metrics
            ORDER BY execution_count DESC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Get queries slower than threshold
    pub async fn get_slow_queries(
        &self,
        threshold_ms: f32,
        limit: i64,
    ) -> anyhow::Result<Vec<StoredQueryMetric>> {
        let threshold_secs = threshold_ms / 1000.0;

        let records = sqlx::query_as!(
            StoredQueryMetric,
            r#"
            SELECT 
                fingerprint,
                example_sql,
                execution_count,
                total_duration_secs,
                min_duration_secs,
                max_duration_secs,
                avg_duration_secs
            FROM query_metrics
            WHERE avg_duration_secs > $1
            ORDER BY avg_duration_secs DESC
            LIMIT $2
            "#,
            threshold_secs,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(records)
    }

    /// Cleanup old queries not seen in N days
    pub async fn cleanup_stale(&self, days: i32) -> anyhow::Result<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM query_metrics 
            WHERE last_seen < NOW() - make_interval(days => $1)
              AND execution_count < 100
            "#,
        )
        .bind(days)
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            info!(deleted, days, "cleaned_stale_metrics");
        }

        Ok(deleted)
    }

    /// Health check
    pub async fn health_check(&self) -> anyhow::Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Get pool reference for advanced use
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Stored query metric from database
#[derive(Debug, Clone)]
pub struct StoredQueryMetric {
    pub fingerprint: String,
    pub example_sql: String,
    pub execution_count: i64,
    pub total_duration_secs: f64,
    pub min_duration_secs: f32,
    pub max_duration_secs: f32,
    pub avg_duration_secs: f32,
}

impl StoredQueryMetric {
    pub fn avg_duration_ms(&self) -> f32 {
        self.avg_duration_secs * 1000.0
    }

    pub fn total_duration_ms(&self) -> f64 {
        self.total_duration_secs * 1000.0
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.len() > max_len {
        format!("{}...", &s[..max_len])
    } else {
        s.to_string()
    }
}

#[derive(Clone)]
pub struct PostgresCredentials {
    pub conn_str: String,
}

impl PostgresCredentials {
    pub fn try_new(args: &Args) -> Option<Self> {
        match &args.str {
            Some(conn_str) => Some(Self {
                conn_str: conn_str.to_string(),
            }),
            None => std::env::var("DATABASE_URL")
                .ok()
                .map(|conn_str| Self { conn_str }),
        }
    }

    /// Validate connection string format
    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.conn_str.starts_with("postgres://") && !self.conn_str.starts_with("postgresql://")
        {
            anyhow::bail!(
                "Invalid connection string format. Must start with postgres:// or postgresql://"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Integration tests require a real database
    // Run with: DATABASE_URL=postgres://... cargo test -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_upsert_metrics() {
        let url = std::env::var("DATABASE_URL").expect("DATABASE_URL required");
        let handler = PostgresHandler::new(&url).await.unwrap();

        let rows = vec![
            QueryStatRow {
                fingerprint: "test_fp_1".to_string(),
                example_sql: "SELECT 1".to_string(),
                count: 10,
                total_duration_secs: 0.5,
                min_duration_secs: 0.01,
                max_duration_secs: 0.1,
                avg_duration_secs: 0.05,
            },
            QueryStatRow {
                fingerprint: "test_fp_2".to_string(),
                example_sql: "SELECT 2".to_string(),
                count: 5,
                total_duration_secs: 1.0,
                min_duration_secs: 0.1,
                max_duration_secs: 0.5,
                avg_duration_secs: 0.2,
            },
        ];

        let affected = handler.upsert_metrics(rows).await.unwrap();
        assert!(affected > 0);

        // Query back
        let slowest = handler.get_slowest_queries(10).await.unwrap();
        assert!(!slowest.is_empty());
    }
}
