use sqlx::{PgPool, postgres::PgRow};
use std::sync::RwLock;

use crate::server::metrics::QueryStatistics;

pub struct PostgresHandler {
    pub pool: PgPool,
}

impl PostgresHandler {
    pub async fn new(sql_str: &str) -> anyhow::Result<Self> {
        Ok(Self {
            pool: sqlx::PgPool::connect(sql_str).await?,
        })
    }

    pub async fn do_query(&self, query: &str) -> anyhow::Result<Vec<PgRow>> {
        let rows = sqlx::query(query).fetch_all(&self.pool).await?;

        Ok(rows)
    }

    pub async fn write_metrics(&self, queries: &RwLock<QueryStatistics>) -> anyhow::Result<()> {
        // Create all the vectors before getting the lock
        let (
            mut query_vec,
            mut counts,
            mut total_durations,
            mut min_durations,
            mut max_durations,
            mut avg_durations,
        );

        {
            let stats = &queries.read().unwrap().queries;
            let stats_len = stats.len();

            query_vec = Vec::with_capacity(stats_len);
            counts = Vec::with_capacity(stats_len);
            total_durations = Vec::with_capacity(stats_len);
            min_durations = Vec::with_capacity(stats_len);
            max_durations = Vec::with_capacity(stats_len);
            avg_durations = Vec::with_capacity(stats_len);

            for (_, stat) in stats {
                query_vec.push(stat.query.clone());
                counts.push(stat.count as i64);
                total_durations.push(stat.total_duration.as_secs_f32());
                min_durations.push(stat.min_duration.as_secs_f32());
                max_durations.push(stat.max_duration.as_secs_f32());
                avg_durations.push(stat.avg_duration as f32);
            }
        }

        sqlx::query!(r#"
            INSERT INTO queries (query, count, total_duration, min_duration, max_duration, avg_duration)
            SELECT * FROM UNNEST ($1::varchar[], $2::bigint[], $3::real[], $4::real[], $5::real[], $6::real[])
            ON CONFLICT (query) DO UPDATE 
            SET 
            count = queries.count,
            total_duration = queries.total_duration,
            min_duration = queries.min_duration,
            max_duration = queries.max_duration,
            avg_duration = queries.avg_duration
            "#,
            &query_vec[..],
            &counts,
            &total_durations,
            &min_durations,
            &max_durations,
            &avg_durations
        ).execute(&self.pool).await?;

        Ok(())
    }
}

pub struct PostgresCredentials {
    user: String,
    database: String,
    password: String,
    host: String,
    port: u16,
}

impl PostgresCredentials {
    pub fn new() -> anyhow::Result<Self> {
        let user = std::env::var("SQLENS_USER").unwrap_or_else(|_| "secret".into());
        let database = std::env::var("SQLENS_DATABASE").unwrap_or_else(|_| "localhost".into());
        let password = std::env::var("SQLENS_PASSWORD").unwrap_or_else(|_| "secret".into());
        let host = std::env::var("SQLENS_HOST").unwrap_or_else(|_| "localhost".into());
        let port = std::env::var("SQLENS_PORT").unwrap_or_else(|_| 5432.to_string());

        Ok(Self {
            user,
            database,
            password,
            host,
            port: port.parse()?,
        })
    }

    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.user, self.password, self.host, self.port, self.database
        )
    }
}
