use std::env::VarError;

use sqlx::{PgPool, postgres::PgRow};
use tracing::info;

use crate::server::metrics::QUERY_STATS;

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

    pub async fn write_metrics(&self) -> anyhow::Result<()> {
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
            let stats = &QUERY_STATS.read().unwrap().queries;
            if stats.is_empty() {
                info!("There are no queries to insert into the database.");
                return Ok(())
            }
            let stats_len = stats.len();

            query_vec = Vec::with_capacity(stats_len);
            counts = Vec::with_capacity(stats_len);
            total_durations = Vec::with_capacity(stats_len);
            min_durations = Vec::with_capacity(stats_len);
            max_durations = Vec::with_capacity(stats_len);
            avg_durations = Vec::with_capacity(stats_len);

            for (_, stat) in stats.iter() {
                println!("{}", stat);
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
            count = EXCLUDED.count,
            total_duration = EXCLUDED.total_duration,
            min_duration = EXCLUDED.min_duration,
            max_duration = EXCLUDED.max_duration,
            avg_duration = EXCLUDED.avg_duration
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

pub struct PostgresCredentials;

impl PostgresCredentials {
    pub fn connection_string() -> Result<String, VarError> {
        std::env::var("DATABASE_URL")
    }
}
