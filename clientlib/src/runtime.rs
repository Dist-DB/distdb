use crate::config::{normalize_bootstrap_peers, resolve_database_for_sql, DEFAULT_DATABASE};
use crate::models::{QueryColumnDef, QueryValue};
use crate::{
    ClientError, ClientOptions, ConnectionInfo, DistDbChannel, DistDbClient, ExecuteResponse, QueryResponse,
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
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug)]
pub(crate) struct ClientInner {
    pub(crate) transport: ConnectorP2pTransport,
    pub(crate) options: ClientOptions,
    pub(crate) request_seq: u64,
    pub(crate) connected: bool,
    pub(crate) current_database: Option<String>,
    pub(crate) current_connection: Option<ConnectionInfo>,
}

impl DistDbClient {

    fn new_with_registry(
        mut options: ClientOptions,
        active_connections: Arc<Mutex<Vec<ConnectionInfo>>>,
        client_handles: Arc<Mutex<Vec<Weak<Mutex<ClientInner>>>>>,
    ) -> Result<Self, ClientError> {

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
            current_connection: None,
            options,
        };

        let inner = Arc::new(Mutex::new(inner));

        {
            let mut handles = client_handles
                .lock()
                .map_err(|_| ClientError::Runtime("client handle registry lock poisoned".to_string()))?;
            handles.push(Arc::downgrade(&inner));
        }

        Ok(Self {
            inner,
            active_connections,
            client_handles,
        })

    }

    fn options_snapshot(&self) -> Result<ClientOptions, ClientError> {

        let guard = self
            .inner
            .lock()
            .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

        Ok(guard.options.clone())

    }

    pub fn new(options: ClientOptions) -> Result<Self, ClientError> {

        let registry = Arc::new(Mutex::new(Vec::<ConnectionInfo>::new()));
        let handles = Arc::new(Mutex::new(Vec::<Weak<Mutex<ClientInner>>>::new()));
        Self::new_with_registry(options, registry, handles)

    }

    pub async fn connect(&self) -> Result<ConnectionInfo, ClientError> {

        let inner = Arc::clone(&self.inner);
        let active_connections = Arc::clone(&self.active_connections);

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

            let connection = ConnectionInfo {
                active_peer_id,
                session_id,
                user: guard.options.user.clone(),
                database: guard.current_database.clone(),
            };

            guard.current_connection = Some(connection.clone());

            let mut registry = active_connections
                .lock()
                .map_err(|_| ClientError::Runtime("active connection registry lock poisoned".to_string()))?;
            register_active_connection(&mut registry, &connection);

            Ok(connection)

        })
        .await
        .map_err(|err| ClientError::Runtime(format!("connect task failed: {err}")))?

    }

    pub async fn connect_channel(&self) -> Result<DistDbChannel, ClientError> {

        let options = self.options_snapshot()?;
        let channel_client = DistDbClient::new_with_registry(
            options,
            Arc::clone(&self.active_connections),
            Arc::clone(&self.client_handles),
        )?;
        let _ = channel_client.connect().await?;

        Ok(DistDbChannel {
            client: channel_client,
        })

    }

    pub async fn connect_channels(&self, count: usize) -> Result<Vec<DistDbChannel>, ClientError> {

        if count == 0 {
            return Err(ClientError::Config(
                "connect_channels requires count >= 1".to_string(),
            ));
        }

        let options = self.options_snapshot()?;
        let mut channels = Vec::with_capacity(count);

        for _ in 0..count {
            let channel_client = DistDbClient::new_with_registry(
                options.clone(),
                Arc::clone(&self.active_connections),
                Arc::clone(&self.client_handles),
            )?;
            let _ = channel_client.connect().await?;
            channels.push(DistDbChannel {
                client: channel_client,
            });
        }

        Ok(channels)

    }

    pub async fn disconnect(&self) -> Result<(), ClientError> {

        let inner = Arc::clone(&self.inner);
        let active_connections = Arc::clone(&self.active_connections);

        tokio::task::spawn_blocking(move || {
            
            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            let disconnected = guard.current_connection.take();
            guard.transport.disconnect_active_peer();
            guard.connected = false;

            if let Some(connection) = disconnected {
                let mut registry = active_connections
                    .lock()
                    .map_err(|_| ClientError::Runtime("active connection registry lock poisoned".to_string()))?;
                unregister_active_connection(&mut registry, &connection);
            }
            
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

pub(crate) async fn close_all_connections(client: &DistDbClient) -> Result<(), ClientError> {

    let client_handles = Arc::clone(&client.client_handles);
    let active_connections = Arc::clone(&client.active_connections);

    tokio::task::spawn_blocking(move || {

        let tracked_inners: Vec<Arc<Mutex<ClientInner>>> = {
            let mut handles = client_handles
                .lock()
                .map_err(|_| ClientError::Runtime("client handle registry lock poisoned".to_string()))?;

            let mut upgraded = Vec::with_capacity(handles.len());
            handles.retain(|weak| {
                if let Some(inner) = weak.upgrade() {
                    upgraded.push(inner);
                    true
                } else {
                    false
                }
            });

            upgraded
        };

        for inner in tracked_inners {
            let mut guard = inner
                .lock()
                .map_err(|_| ClientError::Runtime("client state lock poisoned".to_string()))?;

            guard.transport.disconnect_active_peer();
            guard.connected = false;
            guard.current_connection = None;
        }

        let mut registry = active_connections
            .lock()
            .map_err(|_| ClientError::Runtime("active connection registry lock poisoned".to_string()))?;
        registry.clear();

        Ok(())

    })
    .await
    .map_err(|err| ClientError::Runtime(format!("close_all_connections task failed: {err}")))?

}

fn same_connection(left: &ConnectionInfo, right: &ConnectionInfo) -> bool {
    left.active_peer_id == right.active_peer_id
        && left.session_id == right.session_id
        && left.user == right.user
        && left.database == right.database
}

fn register_active_connection(registry: &mut Vec<ConnectionInfo>, connection: &ConnectionInfo) {
    if !registry.iter().any(|existing| same_connection(existing, connection)) {
        registry.push(connection.clone());
    }
}

fn unregister_active_connection(registry: &mut Vec<ConnectionInfo>, connection: &ConnectionInfo) {
    registry.retain(|existing| !same_connection(existing, connection));
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

