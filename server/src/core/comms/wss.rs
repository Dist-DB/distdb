use std::future::Future;

use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, DataQuery,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::core::comms::surface::{
    InboundChannelMessage, InboundChannelSurface, RustWssInboundSurface,
};

pub const DISTDB_WSS_PATH: &str = "/connector/wss";
pub const DISTDB_WSS_SUBPROTOCOL: &str = "distdb-connector-v1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WssFrameError {
    UnsupportedMessageType,
    MissingPayload,
    Decode(String),
    Encode(String),
    TlsPolicy(String),
}

impl std::fmt::Display for WssFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedMessageType => write!(f, "unsupported websocket message type"),
            Self::MissingPayload => write!(f, "missing websocket payload"),
            Self::Decode(message) => write!(f, "decode failed: {message}"),
            Self::Encode(message) => write!(f, "encode failed: {message}"),
            Self::TlsPolicy(message) => write!(f, "tls policy violation: {message}"),
        }
    }
}

impl std::error::Error for WssFrameError {}

#[derive(Debug)]
pub enum WssHandlerError {
    Frame(WssFrameError),
    Transport(String),
    Protocol(String),
    Executor(String),
}

impl std::fmt::Display for WssHandlerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Frame(err) => write!(f, "frame error: {err}"),
            Self::Transport(message) => write!(f, "transport error: {message}"),
            Self::Protocol(message) => write!(f, "protocol error: {message}"),
            Self::Executor(message) => write!(f, "executor error: {message}"),
        }
    }
}

impl std::error::Error for WssHandlerError {}

impl From<WssFrameError> for WssHandlerError {
    fn from(value: WssFrameError) -> Self {
        Self::Frame(value)
    }
}

pub async fn handle_wss_inbound_stream<S, F, Fut>(
    mut stream: WebSocketStream<S>,
    mut execute_request: F,
) -> Result<(), WssHandlerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    F: FnMut(ConnectorRequest) -> Fut,
    Fut: Future<Output = Result<ConnectorResponse, String>>,
{
    let surface = RustWssInboundSurface;
    handle_wss_inbound_stream_with_surface(&surface, &mut stream, &mut execute_request).await
}

pub async fn handle_wss_inbound_stream_with_surface<S, F, Fut, Surf>(
    surface: &Surf,
    stream: &mut WebSocketStream<S>,
    execute_request: &mut F,
) -> Result<(), WssHandlerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Surf: InboundChannelSurface,
    F: FnMut(ConnectorRequest) -> Fut,
    Fut: Future<Output = Result<ConnectorResponse, String>>,
{
    if surface.supports_service_control_plane() {
        return Err(WssHandlerError::Protocol(
            "wss inbound surface must not enable service control-plane routing".to_string(),
        ));
    }

    let mut text_request_seq = 0u64;

    while let Some(inbound) = stream.next().await {
        let message = inbound
            .map_err(|err| WssHandlerError::Transport(err.to_string()))?;

        match message {
            Message::Close(_) => return Ok(()),
            Message::Ping(payload) => {
                stream
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|err| WssHandlerError::Transport(err.to_string()))?;
            }
            Message::Pong(_) => {}
            Message::Binary(payload) => {
                let inbound_message = surface
                    .decode_inbound_payload(&payload)
                    .map_err(WssHandlerError::Protocol)?;

                let request = match inbound_message {
                    InboundChannelMessage::Connector(request) => request,
                    InboundChannelMessage::Service(_) => {
                        return Err(WssHandlerError::Protocol(
                            "wss channel does not allow service control-plane messages"
                                .to_string(),
                        ));
                    }
                };

                let response = execute_request(request)
                    .await
                    .map_err(WssHandlerError::Executor)?;

                let response_payload = surface
                    .encode_connector_response_payload(&response)
                    .map_err(WssHandlerError::Protocol)?;

                stream
                    .send(Message::Binary(response_payload))
                    .await
                    .map_err(|err| WssHandlerError::Transport(err.to_string()))?;
            }
            Message::Text(sql) => {
                text_request_seq = text_request_seq.saturating_add(1);
                let request = ConnectorRequest::new(
                    format!("wss-text-req-{text_request_seq}"),
                    ConnectorCommand::Query {
                        query: DataQuery {
                            database_id: "main".to_string(),
                            sql,
                        },
                    },
                );

                let response = execute_request(request)
                    .await
                    .map_err(WssHandlerError::Executor)?;

                stream
                    .send(Message::Text(render_text_response(&response)))
                    .await
                    .map_err(|err| WssHandlerError::Transport(err.to_string()))?;
            }
            Message::Frame(_) => {
                return Err(WssHandlerError::Frame(
                    WssFrameError::UnsupportedMessageType,
                ));
            }
        }
    }

    Ok(())
}

fn render_text_response(response: &ConnectorResponse) -> String {
    let ok = response.status.to_string() == "applied";
    let code = if ok { 0 } else { 1 };

    let payload = match &response.result {
        ConnectorResult::Mutation(result) => json!({
            "ok": ok,
            "code": code,
            "error": Value::Null,
            "result": {
                "type": "mutation",
                "affected_rows": result.affected_rows,
            }
        }),
        ConnectorResult::Schema(result) => json!({
            "ok": ok,
            "code": code,
            "error": Value::Null,
            "result": {
                "type": "schema",
                "table_id": result.table_id,
                "schema_revision": result.schema_revision,
            }
        }),
        ConnectorResult::Error(message) => json!({
            "ok": false,
            "code": 1,
            "error": message,
            "result": Value::Null,
        }),
        ConnectorResult::Query(result) => {
            let columns = result
                .columns
                .iter()
                .map(|column| {
                    json!({
                        "seqno": column.seqno,
                        "name": column.field_name,
                        "datatype": column.field_type.to_sql_string(),
                        "datatype_variant": column.field_type,
                        "nullable": column.nullable,
                        "indexed": index_kind_name(column.indexed),
                        "default_value_bytes": column.default_value,
                        "metadata": column.metadata,
                    })
                })
                .collect::<Vec<_>>();

            let rows = result
                .rows
                .iter()
                .map(|row| {
                    row
                        .iter()
                        .map(|field| String::from_utf8_lossy(field).to_string())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            json!({
                "ok": ok,
                "code": code,
                "error": Value::Null,
                "result": {
                    "type": "query",
                    "columns": columns,
                    "rows": rows,
                    "timings": result.timings,
                    "row_count": result.rows.len(),
                }
            })
        }
    };

    payload.to_string()
}

fn index_kind_name(indexed: connector::FieldIndex) -> &'static str {
    match indexed {
        connector::FieldIndex::None => "none",
        connector::FieldIndex::Indexed => "indexed",
        connector::FieldIndex::PrimaryKey => "primary_key",
    }
}

pub fn validate_wss_tls_policy(
    tls_mode: common::TlsMode,
    tls_acceptor_configured: bool,
) -> Result<(), WssFrameError> {
    if tls_mode != common::TlsMode::Required {
        return Err(WssFrameError::TlsPolicy(
            "wss requires tls=required; off/optional are not allowed".to_string(),
        ));
    }

    if !tls_acceptor_configured {
        return Err(WssFrameError::TlsPolicy(
            "wss requires a configured tls acceptor".to_string(),
        ));
    }

    Ok(())
}

pub fn is_wss_path(path: &str) -> bool {
    path.trim() == DISTDB_WSS_PATH
}

pub fn encode_connector_response_message(
    response: &ConnectorResponse,
) -> Result<Message, WssFrameError> {
    let payload = bincode::serialize(response)
        .map_err(|err| WssFrameError::Encode(err.to_string()))?;
    Ok(Message::Binary(payload))
}

pub fn decode_connector_request_message(
    message: Message,
) -> Result<ConnectorRequest, WssFrameError> {
    let payload = match message {
        Message::Binary(bytes) => bytes,
        Message::Close(_) => return Err(WssFrameError::MissingPayload),
        Message::Ping(_) | Message::Pong(_) | Message::Text(_) | Message::Frame(_) => {
            return Err(WssFrameError::UnsupportedMessageType);
        }
    };

    if payload.is_empty() {
        return Err(WssFrameError::MissingPayload);
    }

    bincode::deserialize::<ConnectorRequest>(&payload)
        .map_err(|err| WssFrameError::Decode(err.to_string()))
}

#[cfg(test)]
#[path = "wss_test.rs"]
mod tests;
