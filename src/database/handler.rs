use std::env::VarError;

use sqlx::PgPool;
use tracing::info;

use crate::server::metrics::Stats;


pub struct PostgresHandler {
    pub pool: PgPool,
}

impl PostgresHandler {
    pub async fn new(sql_str: &str) -> anyhow::Result<Self> {
        Ok(Self {
            pool: sqlx::PgPool::connect(sql_str).await?,
        })
    }

    pub async fn write_metrics(&self, query_stats: Stats) -> anyhow::Result<()> {
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
            let stats = &query_stats.read().unwrap().queries;
            if stats.is_empty() {
                info!("There are no queries to insert into the database.");
                return Ok(());
            }
            let stats_len = stats.len();

            query_vec = Vec::with_capacity(stats_len);
            counts = Vec::with_capacity(stats_len);
            total_durations = Vec::with_capacity(stats_len);
            min_durations = Vec::with_capacity(stats_len);
            max_durations = Vec::with_capacity(stats_len);
            avg_durations = Vec::with_capacity(stats_len);

            for (_, stat) in stats.iter() {
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
            count = EXCLUDED.count + queries.count,
            total_duration = EXCLUDED.total_duration + queries.total_duration,
            min_duration = LEAST(EXCLUDED.min_duration, queries.min_duration),
            max_duration = GREATEST(EXCLUDED.max_duration, queries.max_duration),
            avg_duration = (queries.total_duration + EXCLUDED.total_duration) / (queries.count + EXCLUDED.count)
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
