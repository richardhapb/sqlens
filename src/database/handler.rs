use sqlx::PgPool;
use tracing::{info, instrument, trace};

use crate::{Args, server::metrics::QueryStatistics};

pub struct PostgresHandler {
    pub pool: PgPool,
}

impl PostgresHandler {
    pub async fn new(sql_str: &str) -> anyhow::Result<Self> {
        trace!("Creating new Postgres connection");
        Ok(Self {
            pool: sqlx::PgPool::connect(sql_str).await?,
        })
    }

    #[instrument(skip_all)]
    pub async fn write_metrics(&self, query_stats: QueryStatistics) -> anyhow::Result<()> {
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
            let stats = &query_stats.queries;
            if stats.is_empty() {
                info!("There are no queries to insert into the database.");
                return Ok(());
            }
            let stats_len = stats.len();

            trace!(n = stats_len, "Creating vectors for SQL batch insertion");

            query_vec = Vec::with_capacity(stats_len);
            counts = Vec::with_capacity(stats_len);
            total_durations = Vec::with_capacity(stats_len);
            min_durations = Vec::with_capacity(stats_len);
            max_durations = Vec::with_capacity(stats_len);
            avg_durations = Vec::with_capacity(stats_len);

            trace!(
                ?query_vec,
                ?counts,
                ?total_durations,
                ?min_durations,
                ?max_durations,
                ?avg_durations,
                "Inserting data"
            );

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
            None => {
                if let Ok(conn_str) = std::env::var("DATABASE_URL") {
                    Some(Self { conn_str })
                } else {
                    None
                }
            }
        }
    }
}
