use crate::config::{normalize_bootstrap_peers, resolve_database_for_sql, DEFAULT_DATABASE};
use crate::models::{QueryColumnDef, QueryValue};
use crate::{
    ClientError, ClientOptions, ConnectionInfo, DistDbClient, ExecuteResponse, QueryResponse,
    QueryRow, QueryTimings,
};
use common::helpers::utils::md5_hash;
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResult, ConnectorTransport, DataQuery,
    ResponseStatus,
};
use peerlib::{ConnectorP2pConfig, ConnectorP2pTransport, ConnectorPeer};
use serde::de::DeserializeOwned;
use serde_json::{Map, Value};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) struct ClientInner {
    pub(crate) transport: ConnectorP2pTransport,
    pub(crate) options: ClientOptions,
    pub(crate) request_seq: u64,
    pub(crate) connected: bool,
    pub(crate) current_database: Option<String>,
}

impl DistDbClient {

    pub fn new(mut options: ClientOptions) -> Result<Self, ClientError> {

        options.servers = normalize_bootstrap_peers(options.servers.clone());
        
        if options.servers.is_empty() {
            return Err(ClientError::Config(
                "at least one normalized server address is required".to_string(),
            ));
        }

        let mut p2p_config = ConnectorP2pConfig::new("/distdb/kad/1.0.0")
            .with_bootstrap_peers(options.servers.clone())
            .with_tls_mode(options.tls_mode.as_common());

        if let Some(path) = &options.tls_ca_path {
            p2p_config = p2p_config.with_tls_ca_path(path.clone());
        }

        let mut transport = ConnectorP2pTransport::new(p2p_config);

        for addr in &options.servers {
            transport.upsert_peer(ConnectorPeer {
                peer_id: addr.clone(),
                addrs: vec![addr.clone()],
                is_discovered: false,
            });
        }

        let inner = ClientInner {
            transport,
            request_seq: 0,
            connected: false,
            current_database: options.database.clone(),
            options,
        };

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })

    }

    pub async fn connect(&self) -> Result<ConnectionInfo, ClientError> {

        let inner = Arc::clone(&self.inner);

        tokio::task::spawn_blocking(move || {
            
            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            if let Some(peer_id) = guard.options.peer_id.clone() {
                guard.transport.select_peer(peer_id)?;
            }

            guard.transport.connect_active_peer()?;

            if let Some(password) = guard.options.password.clone() {
                authenticate_sync(&mut guard, &password)?;
            }

            if let Some(database) = guard.options.database.clone() {
                guard.current_database = Some(database);
            }

            guard.connected = true;

            let active_peer_id = guard
                .transport
                .active_peer_id()
                .unwrap_or("<none>")
                .to_string();

            let session_id = guard.transport.session_id().ok().flatten();

            Ok(ConnectionInfo {
                active_peer_id,
                session_id,
                user: guard.options.user.clone(),
                database: guard.current_database.clone(),
            })

        })
        .await
        .map_err(|err| ClientError::Runtime(format!("connect task failed: {err}")))?

    }

    pub async fn disconnect(&self) -> Result<(), ClientError> {

        let inner = Arc::clone(&self.inner);

        tokio::task::spawn_blocking(move || {
            
            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            guard.transport.disconnect_active_peer();
            guard.connected = false;
            
            Ok(())

        })
        .await
        .map_err(|err| ClientError::Runtime(format!("disconnect task failed: {err}")))?

    }

    pub async fn set_database(&self, database: impl Into<String>) -> Result<(), ClientError> {

        let inner = Arc::clone(&self.inner);
        let database = database.into();

        tokio::task::spawn_blocking(move || {
            
            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            ensure_connected(&guard)?;
            guard.current_database = Some(database);
            
            Ok(())

        })
        .await
        .map_err(|err| ClientError::Runtime(format!("set_database task failed: {err}")))?

    }

    pub async fn query(&self, sql: impl Into<String>) -> Result<QueryResponse, ClientError> {

        let inner = Arc::clone(&self.inner);
        let sql = sql.into();

        tokio::task::spawn_blocking(move || {

            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            ensure_connected(&guard)?;

            let database_id = resolve_database_for_sql(guard.current_database.as_deref(), &sql)?;
            let response = send_query_sync(&mut guard, &database_id, &sql)?;

            match response.result {
                
                ConnectorResult::Query(result) => {
                    Ok(query_response_from_wire(response.request_id, response.status, result))
                }
                
                ConnectorResult::Error(message) => Err(ClientError::Protocol(message)),

                _ => Err(ClientError::Protocol(
                    "query returned non-query payload".to_string(),
                )),
            
            }

        })
        .await
        .map_err(|err| ClientError::Runtime(format!("query task failed: {err}")))?

    }

    pub async fn query_as<T>(&self, sql: impl Into<String>) -> Result<Vec<T>, ClientError>
    where
        T: DeserializeOwned,
    {
        
        let response = self.query(sql).await?;
        let mut decoded = Vec::with_capacity(response.rows.len());

        for row in response.rows {
            let mut object = Map::new();
            for (index, column) in response.columns.iter().enumerate() {
                if let Some(value) = row.values.get(index) {
                    object.insert(column.name.clone(), query_value_to_json(value));
                } else {
                    object.insert(column.name.clone(), Value::Null);
                }
            }

            let entity = serde_json::from_value::<T>(Value::Object(object))
                .map_err(|err| ClientError::Decode(err.to_string()))?;
            decoded.push(entity);
        }

        Ok(decoded)

    }

    pub async fn execute(&self, sql: impl Into<String>) -> Result<ExecuteResponse, ClientError> {

        let inner = Arc::clone(&self.inner);
        let sql = sql.into();

        tokio::task::spawn_blocking(move || {
            
            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            ensure_connected(&guard)?;

            let database_id = resolve_database_for_sql(guard.current_database.as_deref(), &sql)?;
            let response = send_query_sync(&mut guard, &database_id, &sql)?;

            match response.result {
                
                ConnectorResult::Mutation(result) => Ok(ExecuteResponse::Mutation {
                    request_id: response.request_id,
                    status: response.status.to_string(),
                    affected_rows: result.affected_rows,
                }),

                ConnectorResult::Schema(result) => Ok(ExecuteResponse::Schema {
                    request_id: response.request_id,
                    status: response.status.to_string(),
                    table_id: result.table_id,
                    schema_revision: result.schema_revision,
                }),

                ConnectorResult::Query(result) => {
                    Ok(ExecuteResponse::Query(query_response_from_wire(
                        response.request_id,
                        response.status,
                        result,
                    )))
                }

                ConnectorResult::Error(message) => Err(ClientError::Protocol(message)),

            }

        })
        .await
        .map_err(|err| ClientError::Runtime(format!("execute task failed: {err}")))?
    
    }

}

fn ensure_connected(inner: &ClientInner) -> Result<(), ClientError> {

    if inner.connected {
        return Ok(());
    }

    Err(ClientError::Transport(
        "no active peer connection; call connect() first".to_string(),
    ))

}

fn authenticate_sync(inner: &mut ClientInner, password: &str) -> Result<(), ClientError> {

    let token = md5_hash(password);
    let auth_sql = format!("password_token {token}");
    let _ = send_query_sync(inner, DEFAULT_DATABASE, &auth_sql)?;
    Ok(())

}

fn send_query_sync(
    inner: &mut ClientInner,
    database_id: impl Into<String>,
    sql: &str,
) -> Result<connector::ConnectorResponse, ClientError> {

    let request = ConnectorRequest::new(
        next_request_id(inner),
        ConnectorCommand::Query {
            query: DataQuery {
                database_id: database_id.into(),
                sql: sql.to_string(),
            },
        },
    );

    inner.transport.request(&request).map_err(Into::into)
    
}

fn next_request_id(inner: &mut ClientInner) -> String {
    inner.request_seq += 1;
    format!("clientlib-req-{}", inner.request_seq)
}

fn query_response_from_wire(
    request_id: String,
    status: ResponseStatus,
    wire: connector::QueryResult,
) -> QueryResponse {

    let columns = wire
        .columns
        .iter()
        .enumerate()
        .map(|(ordinal, column)| QueryColumnDef {
            ordinal,
            name: column.field_name.clone(),
            sql_type: column.field_type.sql_variant_display_name(),
            nullable: column.nullable,
            indexed: format!("{:?}", column.indexed),
        })
        .collect::<Vec<_>>();

    let wire_columns = wire.columns.clone();

    let rows = wire
        .rows
        .into_iter()
        .map(|row| QueryRow {
            values: row
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    wire_columns
                        .get(index)
                        .map(|column| decode_query_value(&value, &column.field_type))
                        .unwrap_or_else(|| QueryValue::Bytes(value))
                })
                .collect::<Vec<_>>(),
        })
        .collect::<Vec<_>>();

    let row_count = rows.len();

    QueryResponse {
        request_id,
        status: status.to_string(),
        columns,
        rows,
        row_count,
        timings: QueryTimings {
            server_parse_ms: wire.timings.server_parse_ms,
            server_execute_ms: wire.timings.server_execute_ms,
            server_total_ms: wire.timings.server_total_ms,
            network_round_trip_ms: wire.timings.network_round_trip_ms,
            cache: wire.timings.cache.map(|cache| format!("{cache:?}")),
        },
    }

}

fn decode_query_value(value: &[u8], field_kind: &common::schema::FieldKind) -> QueryValue {

    if value.is_empty() {
        return QueryValue::Null;
    }

    match field_kind {

        common::schema::FieldKind::Int(_) => {
            let text = String::from_utf8_lossy(value).to_string();
            text.parse::<i64>()
                .map(QueryValue::Int)
                .unwrap_or(QueryValue::Text(text))
        },

        common::schema::FieldKind::UInt(_) => {
            let text = String::from_utf8_lossy(value).to_string();
            text.parse::<u64>()
                .map(QueryValue::UInt)
                .unwrap_or(QueryValue::Text(text))
        },

        common::schema::FieldKind::Float(_) => {
            QueryValue::Float(String::from_utf8_lossy(value).to_string())
        },

        common::schema::FieldKind::Blob => QueryValue::Bytes(value.to_vec()),

        _ => QueryValue::Text(String::from_utf8_lossy(value).to_string()),

    }

}

fn query_value_to_json(value: &QueryValue) -> Value {

    match value {
        
        QueryValue::Null => Value::Null,
        
        QueryValue::Int(raw) => Value::from(*raw),
        
        QueryValue::UInt(raw) => Value::from(*raw),
        
        QueryValue::Float(raw) => Value::String(raw.clone()),
        
        QueryValue::Text(raw) => Value::String(raw.clone()),
        
        QueryValue::Bytes(raw) => Value::Array(
            raw.iter()
                .map(|byte| Value::from(*byte as u64))
                .collect::<Vec<_>>(),
        ),

    }

}

#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;

