use super::{
    DISTDB_WSS_PATH,
    WssHandlerError,
    WssFrameError,
    decode_connector_request_message,
    encode_connector_response_message,
    handle_wss_inbound_stream,
    is_wss_path,
    validate_wss_tls_policy,
};
use crate::core::comms::p2p_wire::encode_service_message;
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, MutationResult,
    ResponseStatus,
};
use futures_util::{SinkExt, StreamExt};
use peerlib::{PeerNode, ServiceMessage};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{WebSocketStream, accept_async, client_async};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::Role;

#[test]
fn validate_wss_tls_policy_requires_tls_required() {
    let result = validate_wss_tls_policy(common::TlsMode::Off, true);
    assert!(matches!(result, Err(WssFrameError::TlsPolicy(_))));

    let result = validate_wss_tls_policy(common::TlsMode::Optional, true);
    assert!(matches!(result, Err(WssFrameError::TlsPolicy(_))));

    let result = validate_wss_tls_policy(common::TlsMode::Required, true);
    assert!(result.is_ok());
}

#[test]
fn validate_wss_tls_policy_requires_acceptor() {
    let result = validate_wss_tls_policy(common::TlsMode::Required, false);
    assert!(matches!(result, Err(WssFrameError::TlsPolicy(_))));
}

#[test]
fn is_wss_path_matches_exact_connector_path() {
    assert!(is_wss_path(DISTDB_WSS_PATH));
    assert!(!is_wss_path("/connector/ws"));
    assert!(!is_wss_path("/connector/wss/extra"));
}

#[test]
fn decode_connector_request_message_decodes_binary_bincode() {
    let request = ConnectorRequest::new(
        "req-1",
        ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    );

    let payload = bincode::serialize(&request).expect("request must serialize");
    let decoded = decode_connector_request_message(Message::Binary(payload))
        .expect("binary frame should decode");

    assert_eq!(decoded.request_id, "req-1");
    match decoded.command {
        ConnectorCommand::CreateDatabase { database_name } => {
            assert_eq!(database_name, "main");
        }
        other => panic!("unexpected command: {other:?}"),
    }
}

#[test]
fn decode_connector_request_message_rejects_non_binary_messages() {
    let text = decode_connector_request_message(Message::Text("hello".to_string()));
    assert!(matches!(text, Err(WssFrameError::UnsupportedMessageType)));

    let ping = decode_connector_request_message(Message::Ping(vec![1, 2, 3]));
    assert!(matches!(ping, Err(WssFrameError::UnsupportedMessageType)));

    let close = decode_connector_request_message(Message::Close(None));
    assert!(matches!(close, Err(WssFrameError::MissingPayload)));
}

#[test]
fn encode_connector_response_message_emits_binary_payload() {
    let response = ConnectorResponse {
        request_id: "req-2".to_string(),
        status: ResponseStatus::Applied,
        result: ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    };

    let message = encode_connector_response_message(&response)
        .expect("response should encode");

    let payload = match message {
        Message::Binary(payload) => payload,
        other => panic!("expected binary frame, got {other:?}"),
    };

    let decoded: ConnectorResponse =
        bincode::deserialize(&payload).expect("payload should decode");

    assert_eq!(decoded.request_id, "req-2");
    assert_eq!(decoded.status, ResponseStatus::Applied);
}

#[tokio::test]
async fn handle_wss_inbound_stream_processes_connector_roundtrip() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);

    let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server_ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;

    let handler_task = tokio::spawn(async move {
        handle_wss_inbound_stream(server_ws, |request| async move {
            Ok(ConnectorResponse {
                request_id: request.request_id,
                status: ResponseStatus::Applied,
                result: ConnectorResult::Mutation(MutationResult { affected_rows: 7 }),
            })
        })
        .await
    });

    let request = ConnectorRequest::new(
        "req-wss-1",
        ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    );
    let payload = bincode::serialize(&request).expect("request should serialize");

    client_ws
        .send(Message::Binary(payload))
        .await
        .expect("client send should succeed");

    let inbound = client_ws
        .next()
        .await
        .expect("expected response frame")
        .expect("response frame should decode");

    let response_payload = match inbound {
        Message::Binary(bytes) => bytes,
        other => panic!("expected binary response frame, got {other:?}"),
    };

    let response: ConnectorResponse =
        bincode::deserialize(&response_payload).expect("response payload should deserialize");

    assert_eq!(response.request_id, "req-wss-1");
    assert_eq!(response.status, ResponseStatus::Applied);

    client_ws
        .send(Message::Close(None))
        .await
        .expect("client close should succeed");

    let handler_result = handler_task
        .await
        .expect("handler task should join");
    assert!(handler_result.is_ok());
}

#[tokio::test]
async fn handle_wss_inbound_stream_rejects_service_control_plane_payload() {
    let (client_io, server_io) = tokio::io::duplex(16 * 1024);

    let mut client_ws = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
    let server_ws = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;

    let handler_task = tokio::spawn(async move {
        handle_wss_inbound_stream(server_ws, |_request| async move {
            Ok(ConnectorResponse {
                request_id: "unexpected".to_string(),
                status: ResponseStatus::Applied,
                result: ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            })
        })
        .await
    });

    let service_message = ServiceMessage::NodeAnnounce(PeerNode {
        id: "node-1".to_string(),
        addrs: vec!["/ip4/127.0.0.1/tcp/19401".to_string()],
        is_local: false,
    });
    let service_payload = encode_service_message(&service_message)
        .expect("service payload should encode");

    client_ws
        .send(Message::Binary(service_payload))
        .await
        .expect("client send should succeed");

    let handler_result = handler_task
        .await
        .expect("handler task should join");

    assert!(matches!(
        handler_result,
        Err(WssHandlerError::Protocol(_))
    ));
}

#[tokio::test]
async fn integration_wss_handler_roundtrip_over_tcp_websocket() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener addr");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("server accept should succeed");
        let ws_stream = accept_async(stream)
            .await
            .expect("server websocket handshake should succeed");

        handle_wss_inbound_stream(ws_stream, |request| async move {
            Ok(ConnectorResponse {
                request_id: request.request_id,
                status: ResponseStatus::Applied,
                result: ConnectorResult::Mutation(MutationResult { affected_rows: 3 }),
            })
        })
        .await
    });

    let client_stream = TcpStream::connect(addr)
        .await
        .expect("client tcp connect should succeed");

    let url = format!("ws://{}/connector/wss", addr);
    let (mut client_ws, _response) = client_async(url, client_stream)
        .await
        .expect("client websocket handshake should succeed");

    let request = ConnectorRequest::new(
        "req-int-1",
        ConnectorCommand::CreateDatabase {
            database_name: "main".to_string(),
        },
    );

    let payload = bincode::serialize(&request).expect("request should serialize");
    client_ws
        .send(Message::Binary(payload))
        .await
        .expect("send request should succeed");

    let inbound = client_ws
        .next()
        .await
        .expect("response frame expected")
        .expect("response frame should decode");

    let response_payload = match inbound {
        Message::Binary(bytes) => bytes,
        other => panic!("expected binary response frame, got {other:?}"),
    };

    let response: ConnectorResponse =
        bincode::deserialize(&response_payload).expect("response payload should deserialize");

    assert_eq!(response.request_id, "req-int-1");
    assert_eq!(response.status, ResponseStatus::Applied);

    client_ws
        .send(Message::Close(None))
        .await
        .expect("close frame should send");

    let server_result = server_task.await.expect("server task should join");
    assert!(server_result.is_ok());
}

#[tokio::test]
async fn integration_wss_handler_rejects_service_payload_over_tcp_websocket() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener addr");

    let server_task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("server accept should succeed");
        let ws_stream = accept_async(stream)
            .await
            .expect("server websocket handshake should succeed");

        handle_wss_inbound_stream(ws_stream, |_request| async move {
            Ok(ConnectorResponse {
                request_id: "unexpected".to_string(),
                status: ResponseStatus::Applied,
                result: ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            })
        })
        .await
    });

    let client_stream = TcpStream::connect(addr)
        .await
        .expect("client tcp connect should succeed");

    let url = format!("ws://{}/connector/wss", addr);
    let (mut client_ws, _response) = client_async(url, client_stream)
        .await
        .expect("client websocket handshake should succeed");

    let service_message = ServiceMessage::NodeAnnounce(PeerNode {
        id: "node-z".to_string(),
        addrs: vec!["/ip4/127.0.0.1/tcp/19401".to_string()],
        is_local: false,
    });
    let service_payload = encode_service_message(&service_message)
        .expect("service payload should encode");

    client_ws
        .send(Message::Binary(service_payload))
        .await
        .expect("send service payload should succeed");

    let server_result = server_task.await.expect("server task should join");
    assert!(matches!(server_result, Err(WssHandlerError::Protocol(_))));
}
