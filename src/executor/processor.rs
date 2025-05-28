use crate::executor::handler::{PostgresCredentials, PostgresHandler};
use crate::executor::info::ResponseInfo;
use async_trait::async_trait;
use futures::{Sink, SinkExt, stream};
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::ErrorInfo;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::response::NoticeResponse;
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use sqlx::Row;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::{error, info};

pub struct DummyProcessor;
#[async_trait]
impl NoopStartupHandler for DummyProcessor {
    async fn post_startup<C>(
        &self,
        client: &mut C,
        _message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client.metadata_mut().insert(
            "credentials".into(),
            PostgresCredentials::connection_string(),
        );

        info!(
            "connected {:?}: {:?}",
            client.socket_addr(),
            client.metadata()
        );
        Ok(())
    }
}

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(
        &self,
        client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        client
            .send(PgWireBackendMessage::NoticeResponse(NoticeResponse::from(
                ErrorInfo::new(
                    "NOTICE".into(),
                    "01000".into(),
                    format!("Query received {}", query),
                ),
            )))
            .await?;

        let db_url = client.metadata().get("credentials").ok_or_else(|| {
            error!("Missing credentials");
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".into(),
                "FATAL".into(),
                "Missing credentials in metadata".into(),
            )))
        })?;

        match PostgresHandler::new(db_url).await {
            Ok(handler) => match handler.do_query(query).await {
                Ok(rows) => {
                    let info = ResponseInfo::from_rows(&rows);

                    if info.is_empty {
                        return Ok(vec![Response::Execution(
                            Tag::new("OK").with_rows(info.rows_n),
                        )]);
                    }

                    let mut schema: Vec<FieldInfo> = vec![];
                    for column in info.columns.iter() {
                        schema.push(FieldInfo::new(
                            column.name.into(),
                            None,
                            None,
                            column.field_type.clone(),
                            column.format,
                        ));
                    }

                    let schema_ref = Arc::new(schema);

                    let mut encoded_rows = vec![];

                    for row in &rows {
                        let mut encoder = DataRowEncoder::new(schema_ref.clone());

                        for column in info.columns.iter() {
                            match column.field_type {
                                Type::INT4 => {
                                    let val: Option<i32> = row.try_get(column.name).ok();
                                    encoder.encode_field(&val)?;
                                }
                                Type::VARCHAR | Type::TEXT => {
                                    let val: Option<String> = row.try_get(column.name).ok();
                                    encoder.encode_field(&val)?;
                                }
                                _ => {
                                    encoder.encode_field(&None::<String>)?; // fallback
                                }
                            }
                        }

                        encoded_rows.push(Ok(encoder.finish()?));
                    }

                    return Ok(vec![Response::Query(QueryResponse::new(
                        schema_ref.clone(),
                        stream::iter(encoded_rows),
                    ))]);
                }
                Err(err) => {
                    error!("Error in database: {}", err);
                    return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Database error: {}", err),
                    ))));
                }
            },
            Err(err) => {
                error!("Connection error: {}", err);
                return Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Connection error: {}", err),
                ))));
            }
        };
    }
}

pub struct DummyProcessorFactory {
    pub handler: Arc<DummyProcessor>,
}

impl PgWireServerHandlers for DummyProcessorFactory {
    type StartupHandler = DummyProcessor;
    type SimpleQueryHandler = DummyProcessor;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}
