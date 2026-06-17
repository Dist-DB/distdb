use crate::core::app::ServerApp;
use crate::core::control::affinity::build_database_schema_summaries_from_app;
use crate::core::control::p2p_wire::{decode_service_message, multiaddr_to_socket_addr};
use crate::core::control::outbound_transport::send_service_message_to_addr;
use crate::core::control::schema_catalog::{
    build_schema_definitions_for_database, resolve_schema_catalog, schema_catalog_signature,
};
use crate::core::control::session::{extract_auth_token, ServerConnectionSession};
use crate::core::control::tcp_transport::TcpServerTransport;
use crate::core::control::tls_support::BoxedConnectorStream;
use crate::core::control::wire_io::{write_response_frame, write_service_message_to_stream};
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, FieldDef, FieldIndex,
    FieldType, MutationResult, QueryResult, QueryTimings,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::p2p::protocol::{
    AffinityJoinResponse, DataSnapshotResponse, SchemaCatalogResponse, ServiceMessage,
    TlsCertEnrollResponse,
    TransactionsSinceResponse,
};
use serverlib::{
    AffinityProcessor, ServerP2pEvent, ServerP2pRuntime, encode_wal_frame,
    import_p2p_ca_pem_if_missing, sign_tls_enrollment_csr,
};
use common::p2p::{
    decode_ca_bootstrap_request, encode_ca_bootstrap_response, is_ca_bootstrap_frame,
    CaBootstrapResponse,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::sync::Mutex;

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";

pub fn node_announce_dedup_key(node: &NodeDescriptor) -> String {
    format!("{}|{}", node.id.0, node.addrs.join(","))
}

pub fn is_server_peer_discovery_query(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();

    normalized == SERVER_PEER_DISCOVERY_SQL || normalized == "show server peers"
}

pub async fn maybe_server_peer_discovery_response(
    request: &ConnectorRequest,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    local_node: &NodeDescriptor,
    service_registry: &Arc<Mutex<HashMap<String, Vec<String>>>>,
) -> Option<ConnectorResponse> {
    
    let ConnectorCommand::Query { query } = &request.command else {
        return None;
    };

    if !is_server_peer_discovery_query(&query.sql) {
        return None;
    }

    let mut peers = {
        let runtime = p2p_runtime.lock().await;
        runtime.network().discover_peers()
    };

    if is_valid_server_node(local_node) {
        peers.push(local_node.clone());
    }

    let mut seen_ids = HashSet::new();
    peers.retain(|peer| seen_ids.insert(peer.id.0.clone()));

    let service_snapshot = {
        let registry = service_registry.lock().await;
        registry.clone()
    };

    let rows = peers
        .into_iter()
        .map(|peer| {
            let services = service_snapshot
                .get(&peer.id.0)
                .cloned()
                .unwrap_or_default()
                .join(",");
            vec![
                peer.id.0.into_bytes(),
                peer.addrs.join(",").into_bytes(),
                services.into_bytes(),
            ]
        })
        .collect::<Vec<_>>();

    let response = ConnectorResponse::applied(
        request.request_id.clone(),
        ConnectorResult::Query(QueryResult {
            columns: vec![
                FieldDef {
                    seqno: 1,
                    field_name: "peer_id".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 2,
                    field_name: "addrs".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
                FieldDef {
                    seqno: 3,
                    field_name: "services".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ],
            rows,
            timings: QueryTimings {
                server_parse_ms: 0,
                server_execute_ms: 0,
                server_total_ms: 0,
                network_round_trip_ms: None,
                cache: None,
            },
        }),
    );

    Some(response)
}

pub fn is_valid_server_node(node: &NodeDescriptor) -> bool {

    if node.id.0.trim().is_empty() {
        return false;
    }

    if node.addrs.is_empty() {
        return false;
    }

    node.addrs
        .iter()
        .all(|addr| multiaddr_to_socket_addr(addr).is_some())

}

#[expect(clippy::too_many_arguments, reason = "necessary for handling connector stream with access to app, p2p runtime, affinity processor, and connection context")]
pub async fn handle_connector_stream(
    mut stream: BoxedConnectorStream,
    app: Arc<Mutex<ServerApp>>,
    p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    seen_node_announces: Arc<Mutex<HashSet<String>>>,
    service_registry: Arc<Mutex<HashMap<String, Vec<String>>>>,
    ca_root_enabled: bool,
    local_node: NodeDescriptor,
    peer_addr: String,
    connection_id: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    let mut session = ServerConnectionSession::new(peer_addr.clone(), connection_id);

    async fn rollback_active_session_transaction(app: &Arc<Mutex<ServerApp>>, session_id: &str) {
        let mut app = app.lock().await;
        if app.rollback_session_transaction(session_id) {
            log::info!(
                "rolled back active transaction due to disconnect session_id={}",
                session_id
            );
        }
    }

    if let Err(err) = write_response_frame(
        &mut stream,
        ConnectorResponse::rejected(
            SERVER_PASSWORD_CHALLENGE_REQUEST_ID,
            session.challenge_message(),
        ),
    )
    .await
    {
        rollback_active_session_transaction(&app, &session.session_id).await;
        return Err(err);
    }

    loop {

        let mut len_buf = [0u8; 4];

        if let Err(err) = stream.read_exact(&mut len_buf).await {
            if matches!(
                err.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) {
                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Ok(());
            }
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Err(Box::new(err));
        }

        let frame_len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; frame_len];

        if let Err(err) = stream.read_exact(&mut payload).await {
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Err(Box::new(err));
        }

        if is_ca_bootstrap_frame(&payload) {
            let node_data_dir = {
                let app_guard = app.lock().await;
                app_guard.node_data_dir().clone()
            };

            let response = if let Some(_req) = decode_ca_bootstrap_request(&payload) {
                match serverlib::load_p2p_ca_pem(&node_data_dir) {
                    Ok(Some(ca_cert_pem)) => CaBootstrapResponse {
                        ok: true,
                        ca_cert_pem: Some(ca_cert_pem),
                        error: None,
                    },
                    Ok(None) => CaBootstrapResponse {
                        ok: false,
                        ca_cert_pem: None,
                        error: Some("no CA cert is available on this node".to_string()),
                    },
                    Err(err) => CaBootstrapResponse {
                        ok: false,
                        ca_cert_pem: None,
                        error: Some(format!("failed loading CA cert: {err}")),
                    },
                }
            } else {
                CaBootstrapResponse {
                    ok: false,
                    ca_cert_pem: None,
                    error: Some("malformed CA bootstrap request".to_string()),
                }
            };

            if let Some(encoded) = encode_ca_bootstrap_response(&response) {
                let len = encoded.len() as u32;
                use tokio::io::AsyncWriteExt;
                let _ = stream.write_all(&len.to_le_bytes()).await;
                let _ = stream.write_all(&encoded).await;
            }

            log::debug!(
                "served CA bootstrap request from {} ok={}",
                peer_addr,
                response.ok
            );

            session.mark_disconnect();
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Ok(());
        }

        if let Some(message) = decode_service_message(&payload) {
            if let ServiceMessage::NodeAnnounce(node) = &message && !is_valid_server_node(node) {
                log::debug!(
                    "ignoring invalid server node announce id='{}' addrs='{}' from {}",
                    node.id.0,
                    node.addrs.join(","),
                    peer_addr
                );
                continue;
            }

            let message_for_fanout = message.clone();
            let mut runtime = p2p_runtime.lock().await;
            if let Err(err) = runtime.handle_event(ServerP2pEvent::MessageReceived {
                from_peer_id: peer_addr.clone(),
                message,
            }) {
                log::debug!("server p2p message handling failed from {}: {}", peer_addr, err);
            }

            if let ServiceMessage::TlsCaDistribution(distribution) = &message_for_fanout {
                let node_data_dir = {
                    let app_guard = app.lock().await;
                    app_guard.node_data_dir().clone()
                };

                match import_p2p_ca_pem_if_missing(&node_data_dir, &distribution.ca_cert_pem) {
                    Ok(true) => {
                        log::info!(
                            "imported p2p CA certificate from issuer_node_id={} via peer={}",
                            distribution.issuer_node_id,
                            peer_addr
                        );
                    }
                    Ok(false) => {
                        log::debug!(
                            "ignored p2p CA distribution from issuer_node_id={} because local CA already exists",
                            distribution.issuer_node_id
                        );
                    }
                    Err(err) => {
                        log::warn!(
                            "failed importing p2p CA distribution from issuer_node_id={}: {}",
                            distribution.issuer_node_id,
                            err
                        );
                    }
                }

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::TlsCertEnrollRequest(enroll_req) = &message_for_fanout {

                if !ca_root_enabled {
                    let response_message = ServiceMessage::TlsCertEnrollResponse(
                        TlsCertEnrollResponse {
                            request_id: enroll_req.request_id.clone(),
                            ok: false,
                            error: Some("tls enrollment disabled on this node; ca_root is not enabled".to_string()),
                            node_cert_pem: None,
                            ca_cert_pem: None,
                        },
                    );

                    if let Err(err) =
                        write_service_message_to_stream(&mut stream, &response_message).await
                    {
                        log::warn!(
                            "failed sending tls enrollment rejection to {}: {}",
                            peer_addr,
                            err
                        );
                    }

                    session.mark_disconnect();
                    rollback_active_session_transaction(&app, &session.session_id).await;
                    return Ok(());
                }
                
                let node_data_dir = {
                    let app_guard = app.lock().await;
                    app_guard.node_data_dir().clone()
                };

                let response = match sign_tls_enrollment_csr(&node_data_dir, &enroll_req.csr_pem) {
                    Ok((node_cert_pem, ca_cert_pem)) => TlsCertEnrollResponse {
                        request_id: enroll_req.request_id.clone(),
                        ok: true,
                        error: None,
                        node_cert_pem: Some(node_cert_pem),
                        ca_cert_pem: Some(ca_cert_pem),
                    },
                    Err(err) => TlsCertEnrollResponse {
                        request_id: enroll_req.request_id.clone(),
                        ok: false,
                        error: Some(err),
                        node_cert_pem: None,
                        ca_cert_pem: None,
                    },
                };

                let response_message = ServiceMessage::TlsCertEnrollResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed sending tls enrollment response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::ServiceAnnounce(announcement) = &message_for_fanout {
                {
                    let mut services = service_registry.lock().await;
                    services.insert(announcement.node_id.clone(), announcement.services.clone());
                }

                let valid_addrs = announcement
                    .addrs
                    .iter()
                    .filter(|addr| multiaddr_to_socket_addr(addr).is_some())
                    .cloned()
                    .collect::<Vec<_>>();

                if !valid_addrs.is_empty() {
                    runtime.network_mut().upsert_discovered_peer(NodeDescriptor {
                        id: NodeId(announcement.node_id.clone()),
                        addrs: valid_addrs,
                        is_local: false,
                    });
                }

                log::debug!(
                    "received service announce node_id={} services={}",
                    announcement.node_id,
                    announcement.services.join(",")
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Ok(());
            }

            if let ServiceMessage::AffinityJoinRequest(join_req) = &message_for_fanout {
                
                let requester_addrs = join_req
                    .requester_addrs
                    .iter()
                    .filter(|addr| multiaddr_to_socket_addr(addr).is_some())
                    .cloned()
                    .collect::<Vec<_>>();

                if !requester_addrs.is_empty() {
                    runtime.network_mut().upsert_discovered_peer(NodeDescriptor {
                        id: NodeId(join_req.requester_node_id.clone()),
                        addrs: requester_addrs,
                        is_local: false,
                    });
                }

                let summaries = {
                    let app_guard = app.lock().await;
                    build_database_schema_summaries_from_app(&app_guard)
                };

                let processor_lock = affinity_processor.lock().await;

                let response = if let Some(processor) = processor_lock.as_ref() {

                    if let Some(doc) = processor.document() {
                        let mut merged_doc = doc.clone();
                        for summary in summaries {
                            merged_doc.upsert_database_schema(summary);
                        }

                        AffinityJoinResponse {
                            request_id: join_req.request_id.clone(),
                            ok: true,
                            error: None,
                            document: Some(merged_doc),
                        }

                    } else {

                        AffinityJoinResponse {
                            request_id: join_req.request_id.clone(),
                            ok: false,
                            error: Some("processor has no document yet".to_string()),
                            document: None,
                        }

                    }

                } else {

                    AffinityJoinResponse {
                        request_id: join_req.request_id.clone(),
                        ok: false,
                        error: Some("affinity not configured".to_string()),
                        document: None,
                    }
                
                };

                drop(processor_lock);

                let response_message = ServiceMessage::AffinityJoinResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send affinity join response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent affinity join response to {} for request_id={}",
                    peer_addr,
                    join_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::SchemaCatalogRequest(schema_req) = &message_for_fanout {

                log::debug!(
                    "received schema catalog request from {} database={}",
                    peer_addr,
                    schema_req.database_id
                );

                let app_guard = app.lock().await;
                let (ok, error, schema_definitions) =
                    match build_schema_definitions_for_database(&app_guard, &schema_req.database_id)
                    {
                        Ok(definitions) => (true, None, definitions),
                        Err(err) => (false, Some(err), Vec::new()),
                    };
                let (schema_identifier, schema_hash) = resolve_schema_catalog(
                    &app_guard,
                    &schema_req.database_id,
                )
                .map(schema_catalog_signature)
                .unwrap_or((0, None));
                
                let database_name = resolve_schema_catalog(&app_guard, &schema_req.database_id)
                    .map(|cat| cat.database_name().to_string())
                    .unwrap_or_default();
                
                drop(app_guard);

                let response = SchemaCatalogResponse {
                    request_id: schema_req.request_id.clone(),
                    ok,
                    error,
                    schema_identifier,
                    schema_hash,
                    schema_definitions,
                    database_name,
                };

                let response_message = ServiceMessage::SchemaCatalogResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send schema catalog response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent schema catalog response to {} for request_id={}",
                    peer_addr,
                    schema_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Ok(());

            }

            if let ServiceMessage::DataSnapshotRequest(snapshot_req) = &message_for_fanout {
                
                log::debug!(
                    "received data snapshot request from {} database={} tables={:?}",
                    peer_addr,
                    snapshot_req.database_id,
                    snapshot_req.table_names
                );

                let snapshot_data: Vec<(String, Vec<String>)> = Vec::new();

                let response = DataSnapshotResponse {
                    request_id: snapshot_req.request_id.clone(),
                    ok: true,
                    error: None,
                    snapshot_data,
                };

                let response_message = ServiceMessage::DataSnapshotResponse(response);
                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send data snapshot response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent data snapshot response to {} for request_id={}",
                    peer_addr,
                    snapshot_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::TransactionsSinceRequest(txn_req) = &message_for_fanout {

                log::debug!(
                    "received transactions since request from {} database={} from_tx={:?}",
                    peer_addr,
                    txn_req.database_id,
                    txn_req.from_transaction_id
                );

                let app_guard = app.lock().await;
                let stream_cursors = txn_req
                    .from_stream_transaction_ids
                    .iter()
                    .map(|(stream_id, tx_id)| (stream_id.clone(), *tx_id))
                    .collect::<HashMap<_, _>>();
                let (ok, error, transactions) = match app_guard.export_wal_records_for_database(
                    &txn_req.database_id,
                    txn_req.from_transaction_id,
                    Some(&stream_cursors),
                ) {
                    Ok(records) => {
                        let encoded = records
                            .into_iter()
                            .filter_map(|frame| match encode_wal_frame(&frame) {
                                Ok(encoded) => Some(encoded),
                                Err(err) => {
                                    log::warn!(
                                        "failed encoding WAL frame for response: {}",
                                        err
                                    );
                                    None
                                }
                            })
                            .collect::<Vec<_>>();
                        (true, None, encoded)
                    }
                    Err(err) => (false, Some(err), Vec::new()),
                };

                drop(app_guard);

                let response = TransactionsSinceResponse {
                    request_id: txn_req.request_id.clone(),
                    ok,
                    error,
                    transactions,
                };

                let response_message = ServiceMessage::TransactionsSinceResponse(response);

                if let Err(err) = write_service_message_to_stream(&mut stream, &response_message).await {
                    log::warn!(
                        "failed to send transactions since response to {}: {}",
                        peer_addr,
                        err
                    );
                }

                log::debug!(
                    "sent transactions since response to {} for request_id={}",
                    peer_addr,
                    txn_req.request_id
                );

                session.mark_disconnect();
                rollback_active_session_transaction(&app, &session.session_id).await;
                
                return Ok(());

            }

            if let ServiceMessage::NodeAnnounce(node) = message_for_fanout {

                let dedup_key = node_announce_dedup_key(&node);
                let should_fanout = {
                    let mut seen = seen_node_announces.lock().await;
                    seen.insert(dedup_key)
                };

                if should_fanout {
                    for announce_addr in &node.addrs {
                        let Some(target_addr) = multiaddr_to_socket_addr(announce_addr) else {
                            continue;
                        };

                        if let Err(err) = send_service_message_to_addr(
                            &target_addr,
                            &ServiceMessage::NodeAnnounce(local_node.clone()),
                        ) {
                            log::debug!(
                                "server p2p direct announce reply to {} failed: {}",
                                target_addr,
                                err
                            );
                        }
                    }

                    let mut target_addrs = HashSet::new();
                    for peer in runtime.network().discover_peers() {
                        for addr in peer.addrs {
                            if !node.addrs.contains(&addr) {
                                target_addrs.insert(addr);
                            }
                        }
                    }

                    drop(runtime);

                    for target in target_addrs {
                        let Some(target_addr) = multiaddr_to_socket_addr(&target) else {
                            continue;
                        };

                        if let Err(err) = send_service_message_to_addr(
                            &target_addr,
                            &ServiceMessage::NodeAnnounce(node.clone()),
                        ) {
                            log::debug!(
                                "server p2p fanout announce to {} failed: {}",
                                target_addr,
                                err
                            );
                        }
                    }
                }

            }

            session.mark_disconnect();
            rollback_active_session_transaction(&app, &session.session_id).await;

            return Ok(());

        }

        let request = match bincode::deserialize::<ConnectorRequest>(&payload) {
            Ok(request) => request,
            Err(_) => {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err("invalid connector or p2p frame payload".into());
            }
        };

        log::debug!(
            "server handling connector request_id={} from {}",
            request.request_id,
            peer_addr
        );

        if let Some(response) =
            maybe_server_peer_discovery_response(&request, &p2p_runtime, &local_node, &service_registry).await
        {
            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }
            continue;
        }

        if !session.authenticated {

            let auth_outcome = match &request.command {
                ConnectorCommand::Query { query } => {
                    extract_auth_token(&query.sql).map(|token| session.authenticate_if_valid_token(token))
                }
                _ => None,
            };

            let response = match auth_outcome {
                Some(true) => ConnectorResponse::applied(
                    request.request_id,
                    ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
                ),
                Some(false) => ConnectorResponse::rejected(request.request_id, "invalid password"),
                None => ConnectorResponse::rejected(
                    request.request_id,
                    "authentication required; run `password <password>;` first",
                ),
            };

            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }

            continue;

        }

        session.record_request(&request);

        let response = {
            let mut app = app.lock().await;
            app.handle_connector_request_for_session(&request, &session.session_id)
        };

        if let Err(err) = write_response_frame(&mut stream, response).await {
            rollback_active_session_transaction(&app, &session.session_id).await;
            return Err(err);
        }

    }

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn node_announce_dedup_key_is_stable() {
        let node = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };

        let key1 = node_announce_dedup_key(&node);
        let key2 = node_announce_dedup_key(&node);
        assert_eq!(key1, key2);
    }

    #[test]
    fn is_valid_server_node_requires_non_empty_id_and_multiaddrs() {

        let valid = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };
        
        assert!(is_valid_server_node(&valid));

        let empty_id = NodeDescriptor {
            id: NodeId("".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        };

        assert!(!is_valid_server_node(&empty_id));

        let bad_addr = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["127.0.0.1:4001".to_string()],
            is_local: false,
        };
        
        assert!(!is_valid_server_node(&bad_addr));

    }

    #[test]
    fn is_server_peer_discovery_query_detects_internal_and_alias() {
        assert!(is_server_peer_discovery_query("__distdb_show_server_peers__"));
        assert!(is_server_peer_discovery_query("show server peers"));
        assert!(is_server_peer_discovery_query("SHOW SERVER PEERS;"));
        assert!(!is_server_peer_discovery_query("show peers"));
    }
}
