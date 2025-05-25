use sqlx::{PgPool, postgres::PgRow};

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
}
