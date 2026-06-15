use server::core::app::ServerApp;
use server::core::config::{ServerRuntimeConfig, DEFAULT_LOCAL_NODE_ID, DEFAULT_LOCAL_SWARM_ID};

use common::helpers::stable_id;
use common::helpers::utils::md5_hash;
use common::helpers::{aes_decrypt, aes_encrypt};
use common::{PeerSession, SessionLog, SessionLogEventType};
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, FieldDef, FieldIndex,
    FieldType, MutationResult, QueryResult, QueryTimings,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::p2p::protocol::{
    AffinityJoinRequest, AffinityJoinResponse, AffinityReplicationAction, DataSnapshotResponse,
    SchemaCatalogResponse, ServiceMessage, TransactionsSinceResponse,
};
use serverlib::p2p::transport::Transport;
use serverlib::{
    AffinityDocument, AffinityMember, AffinityMemberStatus, AffinityProcessor,
    AffinityStorage, DatabaseSchemaSummary, KademliaDiscoveryConfig, KademliaDiscoveryService,
    ReplicationPhaseExecutor, ReplicationSecuritySummary, ServerP2pEvent, ServerP2pNetwork,
    ServerP2pRuntime, TransactionRecord,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashMap, collections::HashSet, net::Ipv4Addr};

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";
const SERVICE_MESSAGE_MAGIC: &[u8; 4] = b"SDSP";

// we change these later (accessing the database structures...)

const SERVER_TEMP_PASSWORD: &str = "password";
const SERVER_TEMP_USER: &str = "root";
const SERVER_TEMP_TOKEN_SALT: &[u8; 8] = b"distdbv1";

#[derive(Debug, Clone, PartialEq, Eq)]
struct AffinityStartupConfig {
    affinity_id: String,
    affinity_key: String,
}

#[derive(Debug, Default)]
struct TcpServerTransport {
    peer_addrs: Vec<String>,
}

impl TcpServerTransport {
    fn new(peer_addrs: Vec<String>) -> Self {
        Self { peer_addrs }
    }
}

impl Transport for TcpServerTransport {
    fn send(&mut self, peer_id: &str, message: ServiceMessage) -> serverlib::helpers::error::Result<()> {
        let addr = multiaddr_to_socket_addr(peer_id)
            .ok_or_else(|| serverlib::helpers::error::ServerLibError::Network(format!("invalid peer address '{peer_id}'")))?;
        send_service_message_to_addr(&addr, &message)
    }

    fn broadcast(&mut self, message: ServiceMessage) -> serverlib::helpers::error::Result<()> {
        if self.peer_addrs.is_empty() {
            return Ok(());
        }

        let mut success_count = 0usize;
        for peer in &self.peer_addrs {
            let Some(addr) = multiaddr_to_socket_addr(peer) else {
                log::warn!("server p2p transport cannot parse peer addr='{}'", peer);
                continue;
            };

            match send_service_message_to_addr(&addr, &message) {
                Ok(()) => {
                    success_count += 1;
                    log::debug!("server p2p transport delivered message to {}", addr);
                }
                Err(err) => {
                    log::debug!("server p2p transport delivery failed to {}: {}", addr, err);
                }
            }
        }

        if success_count == 0 {
            log::warn!(
                "server p2p transport broadcast could not reach any configured peer (message={:?})",
                message
            );
            return Ok(());
        }

        Ok(())
    }
}

#[derive(Debug)]
struct ServerConnectionSession {
    peer_addr: String,
    challenge_id: String,
    session_id: String,
    session: PeerSession,
    log: SessionLog,
    authenticated: bool,
    encrypted_password_md5_token: String,
}

impl ServerConnectionSession {
    
    fn new(peer_addr: String, connection_id: usize) -> Self {

        let challenge_id = format!("challenge-{}-{connection_id}", now_millis());
        
        let session_id = md5_hash(format!("{}:{}:{}", 
            SERVER_TEMP_USER, 
            peer_addr, 
            challenge_id).as_str()
        );
        
        let session = PeerSession::new()
            .with_user_id(SERVER_TEMP_USER)
            .with_session_id(session_id.clone());

        let expected_md5_token = md5_hash(SERVER_TEMP_PASSWORD);
        let security_secret = security_context_secret(SERVER_TEMP_USER, "bootstrap");
        let encrypted_password_md5_token = aes_encrypt(
            &expected_md5_token,
            &security_secret,
            SERVER_TEMP_TOKEN_SALT,
        );

        let mut log = SessionLog::new();

        log.add_entry(
            SessionLogEventType::Connect,
            format!("connector peer connected from {peer_addr}"),
            true,
        );

        log.add_entry(
            SessionLogEventType::Authenticate,
            format!(
                "password challenge issued id={challenge_id} user={}",
                SERVER_TEMP_USER
            ),
            true,
        );

        Self {
            peer_addr,
            challenge_id,
            session_id,
            session,
            log,
            authenticated: false,
            encrypted_password_md5_token,
        }

    }

    fn challenge_message(&self) -> String {
        format!(
            "password challenge required challenge_id={} session_id={} peer={}",
            self.challenge_id, self.session_id, self.peer_addr
        )
    }

    fn record_request(&mut self, request: &ConnectorRequest) {

        let event_type = match &request.command {

            ConnectorCommand::Query { query } => {
                self.session.current_database = Some(query.database_id.clone());
                SessionLogEventType::QueryExecute
            },

            ConnectorCommand::Schema { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::SchemaChange
            },

            ConnectorCommand::Mutation { database_id, .. } => {
                self.session.current_database = Some(database_id.clone());
                SessionLogEventType::Other
            },

            ConnectorCommand::CreateDatabase { database_name } => {
                self.session.current_database = Some(database_name.clone());
                SessionLogEventType::Other
            },

        };

        self.log.add_entry(
            event_type,
            format!(
                "request_id={} routed by server session db={}",
                request.request_id,
                self.session.current_database.as_deref().unwrap_or("<none>")
            ),
            true,
        );

    }

    fn mark_disconnect(&mut self) {

        self.session.clear_connection_state();
        
        self.log.add_entry(
            SessionLogEventType::Disconnect,
            "connector peer disconnected",
            true,
        );

    }

    fn authenticate_if_valid_token(&mut self, candidate_password_md5_token: &str) -> bool {

        let security_secret = security_context_secret(SERVER_TEMP_USER, "bootstrap");
        let expected_password_md5_token =
            aes_decrypt(&self.encrypted_password_md5_token, &security_secret);

        if candidate_password_md5_token == expected_password_md5_token {
            self.authenticated = true;
            self.session.auth_token = Some(format!("{}-authenticated", SERVER_TEMP_USER));

            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!(
                    "temporary password accepted user={} token={}",
                    SERVER_TEMP_USER, candidate_password_md5_token
                ),
                true,
            );

            true
        } else {
            self.log.add_entry(
                SessionLogEventType::Authenticate,
                format!(
                    "temporary password rejected user={} token={}",
                    SERVER_TEMP_USER, candidate_password_md5_token
                ),
                false,
            );

            false
        }

    }

}

fn security_context_secret(user_id: &str, database_id: &str) -> String {
    format!("distdb-security:{}:{}", user_id, database_id)
}

fn parse_server_list_from_args(args: &[String]) -> Vec<String> {

    let server_entries = args
        .iter()
        .find_map(|arg| arg.strip_prefix("servers=").map(ToOwned::to_owned))
        .map(|list| {
            list.split(',')
                .map(|addr| addr.trim().to_string())
                .filter_map(|addr| normalize_bootstrap_addr(&addr))
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();

    let mut seen = HashSet::new();
    let mut server_list = Vec::new();
    for addr in server_entries {
        if seen.insert(addr.clone()) {
            server_list.push(addr);
        }
    }

    server_list
}

fn parse_affinity_startup_config(args: &[String]) -> Option<AffinityStartupConfig> {

    let affinity_spec = args
        .iter()
        .find_map(|arg| arg.strip_prefix("affinity=").map(str::trim))?;

    if affinity_spec.is_empty() {
        return None;
    }

    let (affinity_id, affinity_password) = match affinity_spec.split_once(':') {
        Some((id, pwd)) => {
            let id_str = id.trim();
            let pwd_str = pwd.trim();
            if id_str.is_empty() || pwd_str.is_empty() {
                return None;
            }
            (id_str.to_string(), pwd_str.to_string())
        }
        None => return None,
    };

    let affinity_key = stable_id(&[affinity_id.as_str(), affinity_password.as_str()]);

    Some(AffinityStartupConfig {
        affinity_id,
        affinity_key,
    })

}

fn build_affinity_document_snapshot(
    config: &AffinityStartupConfig,
    local_node: &NodeDescriptor,
    discovered_peers: Vec<NodeDescriptor>,
) -> AffinityDocument {

    let mut members = discovered_peers
        .into_iter()
        .map(|peer| AffinityMember {
            node_id: peer.id,
            addrs: peer.addrs,
            status: AffinityMemberStatus::Unknown,
            last_seen_epoch_ms: now_millis(),
        })
        .collect::<Vec<_>>();

    members.push(AffinityMember {
        node_id: local_node.id.clone(),
        addrs: local_node.addrs.clone(),
        status: AffinityMemberStatus::Online,
        last_seen_epoch_ms: now_millis(),
    });

    let mut dedup = std::collections::HashMap::new();
    for member in members {
        dedup.insert(member.node_id.0.clone(), member);
    }

    AffinityDocument {
        affinity_id: config.affinity_id.clone(),
        affinity_revision: 1,
        members: dedup.into_values().collect(),
        databases: Vec::<DatabaseSchemaSummary>::new(),
        replication_security: ReplicationSecuritySummary {
            policy_revision: 1,
            key_id: Some(config.affinity_key.clone()),
            updated_epoch_ms: now_millis(),
        },
    }

}

fn build_database_schema_summaries_from_app(app: &ServerApp) -> Vec<DatabaseSchemaSummary> {

    let mut summaries = app
        .catalogs()
        .iter()
        .map(|(_, catalog)| {
            let database_id = catalog.database_id.0.clone();
            let mut table_ids = catalog.table_ids();
            table_ids.sort();

            let schema_identifier = catalog.schema_epoch().max(1);
            let schema_fingerprint = md5_hash(
                format!(
                    "{}:{}:{}",
                    database_id,
                    schema_identifier,
                    table_ids.join(",")
                )
                .as_str(),
            );

            DatabaseSchemaSummary {
                database_id: database_id.clone(),
                schema_identifier,
                schema_hash: Some(schema_fingerprint),
            }
        })
        .collect::<Vec<_>>();

    summaries.sort_by(|a, b| a.database_id.cmp(&b.database_id));
    summaries

}

#[allow(dead_code)]
fn send_affinity_join_requests(
    config: &AffinityStartupConfig,
    local_node: &NodeDescriptor,
    discovered_peers: &[NodeDescriptor],
) -> Vec<AffinityJoinResponse> {
    let mut responses = Vec::new();

    for peer in discovered_peers {
        let request_id = format!(
            "{}_{}",
            local_node.id.0,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let join_req = AffinityJoinRequest {
            request_id,
            affinity_id: config.affinity_id.clone(),
            requester_node_id: local_node.id.0.clone(),
            requester_addrs: local_node.addrs.clone(),
            affinity_key: config.affinity_key.clone(),
        };

        let message = ServiceMessage::AffinityJoinRequest(join_req);
        let mut delivered = false;

        for peer_addr in &peer.addrs {
            let Some(socket_addr) = multiaddr_to_socket_addr(peer_addr) else {
                continue;
            };

            log::debug!(
                "sending affinity replication action={} to={} affinity_id={}",
                AffinityReplicationAction::JoinRequest.as_str(),
                socket_addr,
                config.affinity_id
            );

            match send_service_request_to_addr(&socket_addr, &message) {
                Ok(Some(ServiceMessage::AffinityJoinResponse(resp))) => {
                    responses.push(resp);
                    delivered = true;
                    log::debug!(
                        "sent affinity join request and received response peer_id={} addr={}",
                        peer.id.0,
                        socket_addr
                    );
                    break;
                }
                Ok(Some(other)) => {
                    log::warn!(
                        "unexpected message while awaiting join response from peer_id={} addr={}: {:?}",
                        peer.id.0,
                        socket_addr,
                        other
                    );
                }
                Ok(None) => {
                    log::debug!(
                        "no join response received from peer_id={} addr={}",
                        peer.id.0,
                        socket_addr
                    );
                }
                Err(err) => {
                    log::warn!(
                        "failed to send affinity join request to peer_id={} addr={}: {}",
                        peer.id.0,
                        socket_addr,
                        err
                    );
                }
            }
        }

        if !delivered {
            log::warn!(
                "failed to deliver affinity join request to any address for peer_id={}",
                peer.id.0
            );
        }
    }

    responses
}

#[allow(dead_code)]
fn merge_affinity_documents_from_responses(
    base_document: &mut AffinityDocument,
    responses: Vec<AffinityJoinResponse>,
) {
    for response in responses {
        if !response.ok {
            log::warn!("affinity join response failed: {:?}", response.error);
            continue;
        }

        if let Some(remote_doc) = response.document {
            let member_count = remote_doc.members.len();
            let database_count = remote_doc.databases.len();

            for member in remote_doc.members {
                base_document.upsert_member(member);
            }

            for database in remote_doc.databases {
                base_document.upsert_database_schema(database);
            }

            log::debug!(
                "merged affinity document from peer: members={} databases={}",
                member_count,
                database_count
            );
        }
    }
}

async fn execute_affinity_join_sequence(
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    affinity_storage: Arc<AffinityStorage>,
    config: &AffinityStartupConfig,
    local_node: &NodeDescriptor,
    discovered_peers: &[NodeDescriptor],
) {
    if discovered_peers.is_empty() {
        log::debug!("no discovered peers for affinity join");
        return;
    }

    log::info!(
        "starting affinity join sequence with {} peers",
        discovered_peers.len()
    );

    let responses = send_affinity_join_requests(config, local_node, discovered_peers);

    if !responses.is_empty() {
        log::info!(
            "affinity join received {} responses from peers",
            responses.len()
        );

        let mut processor = affinity_processor.lock().await;
        if let Some(ref mut proc) = processor.as_mut() {
            if let Some(base_doc) = proc.document() {
                let mut updated_doc = base_doc.clone();
                merge_affinity_documents_from_responses(&mut updated_doc, responses);
                proc.apply_affinity_document(updated_doc.clone());

                // Mark first sync step (control plane join) as completed
                proc.mark_sync_step_completed(0);

                // Save updated document to storage
                if let Err(err) = affinity_storage.save(&updated_doc) {
                    log::error!("failed to save affinity document after join: {}", err);
                } else {
                    log::info!(
                        "saved affinity document after join affinity_id={} revision={}",
                        updated_doc.affinity_id, updated_doc.affinity_revision
                    );
                }

                // Save checkpoint after marking step
                if let Some(checkpoint) = proc.checkpoint() {
                    if let Err(err) = affinity_storage.save_checkpoint(checkpoint) {
                        log::error!("failed to save checkpoint after join: {}", err);
                    } else {
                        log::debug!("saved checkpoint after join");
                    }
                }
            }
        }
    }

    log::debug!("affinity join sequence completed");
}

fn initialize_affinity_with_persistence(
    config: Option<&AffinityStartupConfig>,
    local_node: &NodeDescriptor,
    discovered_peers: Vec<NodeDescriptor>,
    data_dir: &std::path::Path,
) -> (Option<AffinityProcessor>, AffinityStorage) {
    let storage = AffinityStorage::new(data_dir);

    let Some(config) = config else {
        return (None, storage);
    };

    // Try to load persisted document first
    let document = match storage.load(&config.affinity_id) {
        Ok(Some(doc)) => {
            log::info!(
                "loaded persisted affinity document affinity_id={} revision={}",
                doc.affinity_id,
                doc.affinity_revision
            );
            doc
        }
        Ok(None) => {
            log::debug!("no persisted affinity document found, building from peers");
            build_affinity_document_snapshot(config, local_node, discovered_peers)
        }
        Err(err) => {
            log::warn!(
                "failed to load persisted affinity document: {}, building from peers",
                err
            );
            build_affinity_document_snapshot(config, local_node, discovered_peers)
        }
    };

    // Create and initialize processor
    let mut processor = AffinityProcessor::new(local_node.id.clone());
    processor.begin_join();
    processor.apply_affinity_document(document);

    // Try to load and restore checkpoint if it exists
    match storage.load_checkpoint(&config.affinity_id) {
        Ok(Some(checkpoint)) => {
            processor.restore_checkpoint(checkpoint);
            log::info!(
                "restored checkpoint for resumable replication affinity_id={}",
                config.affinity_id
            );
        }
        Ok(None) => {
            log::debug!("no checkpoint found, starting fresh");
            processor.initialize_checkpoint(serverlib::AffinitySyncPhase::ControlPlane);
        }
        Err(err) => {
            log::warn!(
                "failed to load checkpoint: {}, starting fresh",
                err
            );
            processor.initialize_checkpoint(serverlib::AffinitySyncPhase::ControlPlane);
        }
    }

    match processor.build_sync_plan() {
        Ok(plan) => {
            log::info!(
                "affinity processor initialized with persistence affinity_id={} planned_steps={}",
                config.affinity_id,
                plan.len()
            );
        }
        Err(err) => {
            log::warn!("affinity processor initialization failed: {}", err);
            processor.set_degraded(err.to_string());
        }
    }

    (Some(processor), storage)
}

fn advertised_listen_addr_from_args(args: &[String], listen_addr: &str) -> String {
    if let Some(explicit) = args
        .iter()
        .find_map(|arg| arg.strip_prefix("advertise_addr=").map(ToOwned::to_owned))
    {
        let explicit = explicit.trim().to_string();
        if !explicit.is_empty() {
            return explicit;
        }
    }

    if listen_addr == "0.0.0.0" {
        return "127.0.0.1".to_string();
    }

    listen_addr.to_string()
}

fn normalize_bootstrap_addr(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with("/") {
        return Some(trimmed.to_string());
    }

    if let Ok(port) = trimmed.parse::<u16>() {
        return Some(format!("/ip4/127.0.0.1/tcp/{port}"));
    }

    if let Some(port_str) = trimmed.strip_prefix(':') {
        let port = port_str.parse::<u16>().ok()?;
        return Some(format!("/ip4/127.0.0.1/tcp/{port}"));
    }

    let (host, port) = match trimmed.rsplit_once(':') {
        Some((host, port_str)) => {
            let parsed_port = port_str.parse::<u16>().ok()?;
            (host.trim(), parsed_port)
        }
        None => (trimmed, common::DEFAULT_SERVER_PORT),
    };

    if host.is_empty() {
        return None;
    }

    let host_prefix = if host.parse::<Ipv4Addr>().is_ok() {
        "ip4"
    } else {
        "dns"
    };

    Some(format!("/{host_prefix}/{host}/tcp/{port}"))
}

fn bootstrap_nodes_from_server_list(server_list: &[String]) -> Vec<NodeDescriptor> {
    server_list
        .iter()
        .map(|addr| NodeDescriptor {
            id: NodeId(format!("bootstrap-{}", stable_id(&[addr]))),
            addrs: vec![addr.clone()],
            is_local: false,
        })
        .collect()
}

fn multiaddr_to_socket_addr(addr: &str) -> Option<String> {
    let parts = addr.trim_matches('/').split('/').collect::<Vec<_>>();
    if parts.len() < 4 {
        return None;
    }

    match (parts[0], parts[2]) {
        ("ip4", "tcp") | ("dns", "tcp") => {
            let host = parts[1];
            let port = parts[3].parse::<u16>().ok()?;
            Some(format!("{}:{}", host, port))
        }
        _ => None,
    }
}

fn send_service_message_to_addr(
    addr: &str,
    message: &ServiceMessage,
) -> serverlib::helpers::error::Result<()> {
    let mut stream = std::net::TcpStream::connect(addr).map_err(|err| {
        serverlib::helpers::error::ServerLibError::Network(format!(
            "connect to {} failed: {}",
            addr, err
        ))
    })?;

    stream
        .set_write_timeout(Some(std::time::Duration::from_millis(500)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set write timeout for {} failed: {}",
                addr, err
            ))
        })?;

    let payload = encode_service_message(message).ok_or_else(|| {
        serverlib::helpers::error::ServerLibError::Network(
            "unsupported service message for wire encoding".to_string(),
        )
    })?;

    let len = payload.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "write service frame to {} failed: {}",
                addr, err
            ))
        })?;

    Ok(())
}

fn send_service_request_to_addr(
    addr: &str,
    message: &ServiceMessage,
) -> serverlib::helpers::error::Result<Option<ServiceMessage>> {
    let mut stream = std::net::TcpStream::connect(addr).map_err(|err| {
        serverlib::helpers::error::ServerLibError::Network(format!(
            "connect to {} failed: {}",
            addr, err
        ))
    })?;

    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set write timeout for {} failed: {}",
                addr, err
            ))
        })?;

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set read timeout for {} failed: {}",
                addr, err
            ))
        })?;

    let payload = encode_service_message(message).ok_or_else(|| {
        serverlib::helpers::error::ServerLibError::Network(
            "unsupported service message for wire encoding".to_string(),
        )
    })?;

    let len = payload.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "write service frame to {} failed: {}",
                addr, err
            ))
        })?;

    // Listener sends an initial connector challenge frame to every new socket.
    // Service-message callers skip non-service frames and consume the next frame.
    for _ in 0..2 {
        let mut header = [0u8; 4];
        if let Err(err) = stream.read_exact(&mut header) {
            let timed_out = matches!(
                err.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            );
            if timed_out {
                return Ok(None);
            }

            return Err(serverlib::helpers::error::ServerLibError::Network(format!(
                "read response header from {} failed: {}",
                addr, err
            )));
        }

        let payload_len = u32::from_le_bytes(header) as usize;
        let mut response_payload = vec![0u8; payload_len];
        stream
            .read_exact(&mut response_payload)
            .map_err(|err| {
                serverlib::helpers::error::ServerLibError::Network(format!(
                    "read response payload from {} failed: {}",
                    addr, err
                ))
            })?;

        if let Some(message) = decode_service_message(&response_payload) {
            return Ok(Some(message));
        }
    }

    Err(serverlib::helpers::error::ServerLibError::Network(format!(
        "decode response message from {} failed",
        addr
    )))
}

fn node_announce_dedup_key(node: &NodeDescriptor) -> String {
    format!("{}|{}", node.id.0, node.addrs.join(","))
}

fn is_server_peer_discovery_query(sql: &str) -> bool {
    let normalized = sql
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase();

    normalized == SERVER_PEER_DISCOVERY_SQL || normalized == "show server peers"
}

async fn maybe_server_peer_discovery_response(
    request: &ConnectorRequest,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    local_node: &NodeDescriptor,
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

    let rows = peers
        .into_iter()
        .map(|peer| {
            vec![
                peer.id.0.into_bytes(),
                peer.addrs.join(",").into_bytes(),
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

fn is_valid_server_node(node: &NodeDescriptor) -> bool {
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

fn encode_service_message(message: &ServiceMessage) -> Option<Vec<u8>> {
    let mut payload = SERVICE_MESSAGE_MAGIC.to_vec();
    let encoded = bincode::serialize(message).ok()?;
    payload.extend_from_slice(&encoded);
    Some(payload)
}

fn decode_service_message(payload: &[u8]) -> Option<ServiceMessage> {
    if payload.len() < SERVICE_MESSAGE_MAGIC.len() {
        return None;
    }

    if &payload[..SERVICE_MESSAGE_MAGIC.len()] != SERVICE_MESSAGE_MAGIC {
        return None;
    }

    bincode::deserialize(&payload[SERVICE_MESSAGE_MAGIC.len()..]).ok()
}

fn resolve_schema_catalog<'a>(
    app: &'a ServerApp,
    database_id: &str,
) -> Option<&'a serverlib::DatabaseCatalog> {

    if let Some(catalog) = app.catalogs().get(database_id) {
        return Some(catalog);
    }

    if let Ok(normalized_id) = serverlib::DatabaseId::from_database_name(database_id) {
        if let Some(catalog) = app.catalogs().get(&normalized_id.0) {
            return Some(catalog);
        }
    }

    app.catalogs()
        .values()
        .find(|catalog| catalog.database_id.0 == database_id)
        
}

fn load_schema_catalog_from_disk(
    app: &ServerApp,
    database_id: &str,
) -> Option<serverlib::DatabaseCatalog> {
    
    let mut candidate_ids = vec![database_id.to_string()];

    if let Ok(normalized_id) = serverlib::DatabaseId::from_database_name(database_id) {
        if !candidate_ids.contains(&normalized_id.0) {
            candidate_ids.push(normalized_id.0);
        }
    }

    for candidate_id in candidate_ids {
        let catalog_path = app.node_data_dir().join(
            common::helpers::format::FileKind::Catalog.file_name(&candidate_id),
        );

        if !catalog_path.exists() {
            continue;
        }

        match serverlib::DatabaseCatalog::load_from_path(&catalog_path) {
            Ok(catalog) => return Some(catalog),
            Err(err) => {
                log::warn!(
                    "failed loading schema catalog from disk database_id={} path={} err={}",
                    database_id,
                    catalog_path.display(),
                    err
                );
            }
        }
    }

    None
}

fn schema_catalog_signature(catalog: &serverlib::DatabaseCatalog) -> (u64, Option<String>) {
    let mut table_ids = catalog.table_ids();
    table_ids.sort();

    let schema_identifier = catalog.schema_epoch().max(1);
    let schema_hash = md5_hash(
        format!(
            "{}:{}:{}",
            catalog.database_id.0,
            schema_identifier,
            table_ids.join(",")
        )
        .as_str(),
    );

    (schema_identifier, Some(schema_hash))
}

fn build_schema_definitions_for_database(
    app: &ServerApp,
    database_id: &str,
) -> Result<Vec<String>, String> {

    let catalog = resolve_schema_catalog(app, database_id)
        .cloned()
        .or_else(|| load_schema_catalog_from_disk(app, database_id))
        .ok_or_else(|| format!("database '{}' not found", database_id))?;

    let mut table_ids = catalog.table_ids();
    table_ids.sort();

    let mut statements = Vec::new();

    for table_id in table_ids {
        let Some(schema) = catalog.table_schema(&table_id) else {
            continue;
        };

        let mut fields = schema.fields.clone();
        fields.sort_by_key(|f| f.seqno);

        let mut parts = fields
            .iter()
            .map(|field| {
                field
                    .to_sql_string()
                    .replace(" BIGINT SIGNED", " BIGINT")
                    .replace(" INT SIGNED", " INT")
            })
            .collect::<Vec<_>>();

        let primary_keys = fields
            .iter()
            .filter(|field| matches!(field.indexed, common::schema::FieldIndex::PrimaryKey))
            .map(|field| field.field_name.clone())
            .collect::<Vec<_>>();

        if !primary_keys.is_empty() {
            parts.push(format!("PRIMARY KEY ({})", primary_keys.join(", ")));
        }

        statements.push(format!(
            "CREATE TABLE {} ({});",
            table_id,
            parts.join(", ")
        ));
    }

    Ok(statements)
}

fn initialize_server_p2p_runtime(
    node_id: &str,
    swarm_id: &str,
    advertise_addr: &str,
    port: u16,
    server_list: &[String],
) -> Result<ServerP2pRuntime<TcpServerTransport>, Box<dyn std::error::Error>> {
    let bootstrap_nodes = bootstrap_nodes_from_server_list(server_list);
    let discovery = KademliaDiscoveryService::new(
        NodeId(node_id.to_string()),
        KademliaDiscoveryConfig::new(format!("/distdb/kad/{swarm_id}"))
            .with_bootstrap_nodes(bootstrap_nodes),
    );

    let network = ServerP2pNetwork::new(discovery, TcpServerTransport::new(server_list.to_vec()));
    let mut runtime = ServerP2pRuntime::new(network);

    for peer in runtime.network().discover_peers() {
        runtime.handle_event(ServerP2pEvent::PeerDiscovered(peer))?;
    }

    let local_node = NodeDescriptor {
        id: NodeId(node_id.to_string()),
        addrs: vec![format!("/ip4/{advertise_addr}/tcp/{port}")],
        is_local: true,
    };
    runtime.network_mut().broadcast_announce(local_node)?;

    Ok(runtime)
}

fn spawn_p2p_heartbeat_task(
    runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    local_node: NodeDescriptor,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(30));

        loop {
            ticker.tick().await;

            let mut runtime = runtime.lock().await;
            if let Err(err) = runtime
                .network_mut()
                .broadcast_announce(local_node.clone())
            {
                log::warn!("server p2p heartbeat announce failed: {}", err);
                continue;
            }

            let peer_count = runtime.network().discover_peers().len();
            log::debug!("server p2p heartbeat ok discovered_peers={}", peer_count);
        }
    })
}

fn spawn_affinity_replication_task(
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    affinity_storage: Arc<AffinityStorage>,
    app: Arc<Mutex<ServerApp>>,
    p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_config: AffinityStartupConfig,
    local_node: NodeDescriptor,
) -> JoinHandle<()> {
    
    tokio::spawn(async move {

        let mut ticker = interval(Duration::from_millis(500));
        let mut executor = ReplicationPhaseExecutor::new();
        let mut last_affinity_refresh_at = std::time::Instant::now() - Duration::from_secs(30);
        // Per-database stream cursors: database_id -> (stream_id -> last seen TransactionId).
        // Persisted across ticks so WAL catchup never replays from the beginning.
        let mut wal_cursors: HashMap<String, HashMap<String, serverlib::TransactionId>> = HashMap::new();
        // Per-database last sync timestamp — throttles continuous WAL catchup so we don't
        // hammer peers every 500 ms tick, especially when the database is empty.
        let mut last_wal_sync_at: HashMap<String, std::time::Instant> = HashMap::new();

        loop {

            ticker.tick().await;

            let mut processor = affinity_processor.lock().await;

            if let Some(ref mut proc) = processor.as_mut() {
                // Only execute replication if processor is in Syncing state
                if let serverlib::AffinityProcessorState::Syncing(_) = proc.state() {

                    match proc.build_sync_plan() {

                        Ok(plan) => {
                            let current_idx = executor.current_sync_index();
                            if let Some(step) = plan.get(current_idx) {
                                if matches!(step.phase, serverlib::AffinitySyncPhase::SchemaCatalog) {
                                    if let Some(database_id) = &step.database_id {
                                        let affinity_id = proc
                                            .document()
                                            .map(|doc| doc.affinity_id.clone())
                                            .unwrap_or_default();

                                        if let Err(err) = execute_live_schema_catalog_sync(
                                            &app,
                                            &p2p_runtime,
                                            &affinity_id,
                                            database_id,
                                            step.schema_identifier.unwrap_or(0),
                                            proc.document().and_then(|doc| {
                                                doc.databases
                                                    .iter()
                                                    .find(|db| db.database_id == *database_id)
                                                    .and_then(|db| db.schema_hash.clone())
                                            }),
                                        )
                                        .await
                                        {
                                            log::warn!(
                                                "live schema catalog sync failed affinity_id={} database_id={}: {}",
                                                affinity_id,
                                                database_id,
                                                err
                                            );
                                        }
                                    }
                                }

                                if matches!(step.phase, serverlib::AffinitySyncPhase::WalCatchup) {
                                    if let Some(database_id) = &step.database_id {
                                        let affinity_id = proc
                                            .document()
                                            .map(|doc| doc.affinity_id.clone())
                                            .unwrap_or_default();

                                        let db_cursors = wal_cursors.get(database_id).cloned();
                                        match execute_live_wal_catchup_sync(
                                            &app,
                                            &p2p_runtime,
                                            &affinity_id,
                                            database_id,
                                            None,
                                            db_cursors.as_ref(),
                                            None,
                                        )
                                        .await
                                        {
                                            Ok(updated) => {
                                                if !updated.is_empty() {
                                                    wal_cursors.insert(database_id.clone(), updated);
                                                }
                                            }
                                            Err(err) => {
                                                log::warn!(
                                                    "live WAL catchup sync failed affinity_id={} database_id={}: {}",
                                                    affinity_id,
                                                    database_id,
                                                    err
                                                );
                                            }
                                        }
                                    }
                                }

                            }

                            match executor.execute_next_phase(proc, &plan) {
                                Ok(completed) => {
                                    // Save checkpoint after each phase
                                    if let Some(checkpoint) = proc.checkpoint() {
                                        if let Err(err) = affinity_storage.save_checkpoint(checkpoint) {
                                            log::error!("failed to save checkpoint after replication phase: {}", err);
                                        }
                                    }

                                    // If all phases completed, mark processor as ready
                                    if completed {
                                        proc.set_ready();
                                        log::info!("affinity replication completed, processor is ready");

                                        // Save final document state
                                        if let Some(doc) = proc.document() {
                                            if let Err(err) = affinity_storage.save(doc) {
                                                log::error!("failed to save final affinity document: {}", err);
                                            }
                                        }
                                    } else {
                                        log::debug!("affinity replication phase completed, continuing to next phase");
                                    }
                                }
                                Err(err) => {
                                    log::error!("affinity replication phase failed: {}", err);
                                    proc.set_degraded(format!("replication failed: {}", err));
                                }
                            }
                        },
                        
                        Err(err) => {
                            log::warn!("failed to build replication sync plan: {}", err);
                        }
                    
                    }

                }

                if let serverlib::AffinityProcessorState::Ready = proc.state() {

                    if last_affinity_refresh_at.elapsed() >= Duration::from_secs(2) {
                        
                        last_affinity_refresh_at = std::time::Instant::now();

                        let discovered_peers = {
                            let runtime = p2p_runtime.lock().await;
                            runtime.network().discover_peers()
                        };

                        let responses = send_affinity_join_requests(
                            &affinity_config,
                            &local_node,
                            &discovered_peers,
                        );

                        if !responses.is_empty() {

                            if let Some(base_doc) = proc.document() {
                                let mut merged_doc = base_doc.clone();
                                let previous_database_count = merged_doc.databases.len();
                                merge_affinity_documents_from_responses(&mut merged_doc, responses);

                                if merged_doc != *base_doc {
                                    proc.apply_affinity_document(merged_doc.clone());

                                    if let Err(err) = affinity_storage.save(&merged_doc) {
                                        log::error!(
                                            "failed to save refreshed affinity document: {}",
                                            err
                                        );
                                    }

                                    if merged_doc.databases.len() > previous_database_count {
                                        log::info!(
                                            "refreshed affinity document added databases old_count={} new_count={}",
                                            previous_database_count,
                                            merged_doc.databases.len()
                                        );
                                            // New databases need schema + WAL catchup — re-enter
                                            // Syncing so the executor runs those phases.
                                            // apply_affinity_document already set state to Syncing.
                                            executor.reset();
                                        } else {
                                            // Doc changed (e.g. member status) but no new databases;
                                            // stay Ready.
                                            proc.set_ready();
                                    }

                                }
                            
                            }
                        
                        }

                    }

                    let Some(document) = proc.document() else {
                        continue;
                    };

                    let affinity_id = document.affinity_id.clone();
                    let database_ids = document
                        .databases
                        .iter()
                        .map(|db| db.database_id.clone())
                        .collect::<Vec<_>>();

                    let peer_targets = {
                        let runtime = p2p_runtime.lock().await;
                        runtime
                            .network()
                            .discover_peers()
                            .into_iter()
                            .filter(|peer| !peer.is_local)
                            .flat_map(|peer| peer.addrs)
                            .collect::<Vec<_>>()
                    };

                    for database_id in database_ids {
                        let should_sync = last_wal_sync_at
                            .get(&database_id)
                            .map(|last| last.elapsed() >= Duration::from_secs(5))
                            .unwrap_or(true);

                        if !should_sync {
                            continue;
                        }
                        last_wal_sync_at.insert(database_id.clone(), std::time::Instant::now());

                        let db_cursors = wal_cursors.get(&database_id).cloned();
                        for target_addr in &peer_targets {
                            match execute_live_wal_catchup_sync(
                                &app,
                                &p2p_runtime,
                                &affinity_id,
                                &database_id,
                                None,
                                db_cursors.as_ref(),
                                Some(target_addr),
                            )
                            .await
                            {
                                Ok(updated) => {
                                    if !updated.is_empty() {
                                        wal_cursors.insert(database_id.clone(), updated);
                                    }
                                }
                                Err(err) => {
                                    log::warn!(
                                        "continuous WAL catchup failed affinity_id={} database_id={} target={}: {}",
                                        affinity_id,
                                        database_id,
                                        target_addr,
                                        err
                                    );
                                }
                            }
                        }
                    }
                }

            }

        }

    })

}

fn apply_schema_definitions_to_local_database(
    app: &mut ServerApp,
    database_id: &str,
    schema_definitions: &[String],
) -> Result<(), String> {
    app.apply_affinity_schema_definitions(database_id, schema_definitions)
}

fn encode_wal_frame(frame: &(String, TransactionRecord)) -> Result<String, String> {
    let bytes = bincode::serialize(frame)
        .map_err(|err| format!("failed to serialize WAL frame: {}", err))?;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut encoded, "{:02x}", b);
    }
    Ok(encoded)
}

fn decode_wal_frame(encoded: &str) -> Result<(String, TransactionRecord), String> {
    if encoded.len() % 2 != 0 {
        return Err("invalid WAL frame encoding length".to_string());
    }

    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    let chars = encoded.as_bytes();
    let mut i = 0usize;
    while i < chars.len() {
        let chunk = std::str::from_utf8(&chars[i..i + 2])
            .map_err(|err| format!("invalid WAL frame utf8: {}", err))?;
        let value = u8::from_str_radix(chunk, 16)
            .map_err(|err| format!("invalid WAL frame hex '{}': {}", chunk, err))?;
        bytes.push(value);
        i += 2;
    }

    bincode::deserialize::<(String, TransactionRecord)>(&bytes)
        .map_err(|err| format!("failed to deserialize WAL frame: {}", err))
}

async fn execute_live_schema_catalog_sync(
    app: &Arc<Mutex<ServerApp>>,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_id: &str,
    database_id: &str,
    expected_schema_identifier: u64,
    expected_schema_hash: Option<String>,
) -> Result<(), String> {
    if affinity_id.is_empty() {
        return Ok(());
    }

    let request_id = format!(
        "schema-sync-{}-{}",
        database_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );

    let peer_targets = {
        let runtime = p2p_runtime.lock().await;
        let peers = runtime.network().discover_peers();

        let mut targets = Vec::new();
        for peer in peers {
            if peer.is_local {
                continue;
            }

            for addr in peer.addrs {
                targets.push(addr);
            }
        }
        targets
    };

    if peer_targets.is_empty() {
        log::debug!(
            "no remote peers available for schema sync affinity_id={} database_id={}",
            affinity_id,
            database_id
        );
        return Ok(());
    }

    let mut applied_any = false;
    for target in peer_targets {
        let Some(socket_addr) = multiaddr_to_socket_addr(&target) else {
            continue;
        };

        let message = ServiceMessage::SchemaCatalogRequest(
            serverlib::p2p::protocol::SchemaCatalogRequest {
                request_id: request_id.clone(),
                affinity_id: affinity_id.to_string(),
                database_id: database_id.to_string(),
                expected_schema_identifier,
                expected_schema_hash: expected_schema_hash.clone(),
            },
        );

        log::debug!(
            "sending affinity replication action={} to={} affinity_id={} database_id={}",
            AffinityReplicationAction::SchemaCatalogRequest.as_str(),
            socket_addr,
            affinity_id,
            database_id
        );

        let Ok(Some(ServiceMessage::SchemaCatalogResponse(response))) =
            send_service_request_to_addr(&socket_addr, &message)
        else {
            continue;
        };

        if response.request_id != request_id {
            continue;
        }

        if !response.ok {
            let err = response
                .error
                .unwrap_or_else(|| "unknown schema sync error".to_string());
            log::warn!(
                "peer rejected schema catalog request affinity_id={} database_id={}: {}",
                affinity_id,
                database_id,
                err
            );
            continue;
        }

        let mut app_guard = app.lock().await;
        apply_schema_definitions_to_local_database(
            &mut app_guard,
            database_id,
            &response.schema_definitions,
        )?;
        if !response.database_name.is_empty() {
            let _ = app_guard.set_affinity_catalog_database_name(database_id, &response.database_name);
        }
        applied_any = true;
    }

    if applied_any {
        log::info!(
            "applied remote schema catalog affinity_id={} database_id={}",
            affinity_id,
            database_id
        );
    }

    Ok(())
}

async fn execute_live_wal_catchup_sync(
    app: &Arc<Mutex<ServerApp>>,
    p2p_runtime: &Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_id: &str,
    database_id: &str,
    from_transaction_id: Option<serverlib::TransactionId>,
    from_stream_transaction_ids: Option<&HashMap<String, serverlib::TransactionId>>,
    target_addr: Option<&str>,
) -> Result<HashMap<String, serverlib::TransactionId>, String> {
    if affinity_id.is_empty() {
        return Ok(HashMap::new());
    }

    let request_id = format!(
        "wal-sync-{}-{}",
        database_id,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );

    let peer_targets = {
        let runtime = p2p_runtime.lock().await;
        let peers = runtime.network().discover_peers();

        let mut targets = Vec::new();
        for peer in peers {
            if peer.is_local {
                continue;
            }

            for addr in peer.addrs {
                if target_addr.map(|target| target == addr).unwrap_or(true) {
                    targets.push(addr);
                }
            }
        }
        targets
    };

    if peer_targets.is_empty() {
        log::debug!(
            "no remote peers available for WAL catchup affinity_id={} database_id={}",
            affinity_id,
            database_id
        );
        return Ok(HashMap::new());
    }

    let mut max_seen_by_stream: HashMap<String, u64> = HashMap::new();
    let mut imported = Vec::new();
    for target in peer_targets {
        let Some(socket_addr) = multiaddr_to_socket_addr(&target) else {
            continue;
        };

        let message = ServiceMessage::TransactionsSinceRequest(
            serverlib::p2p::protocol::TransactionsSinceRequest {
                request_id: request_id.clone(),
                affinity_id: affinity_id.to_string(),
                database_id: database_id.to_string(),
                from_transaction_id,
                from_stream_transaction_ids: from_stream_transaction_ids
                    .map(|map| map.iter().map(|(k, v)| (k.clone(), *v)).collect())
                    .unwrap_or_default(),
            },
        );

        log::debug!(
            "sending affinity replication action={} to={} affinity_id={} database_id={} from_tx={:?}",
            AffinityReplicationAction::TransactionsSinceRequest.as_str(),
            socket_addr,
            affinity_id,
            database_id,
            from_transaction_id
        );

        let Ok(Some(ServiceMessage::TransactionsSinceResponse(response))) =
            send_service_request_to_addr(&socket_addr, &message)
        else {
            continue;
        };

        if response.request_id != request_id {
            continue;
        }
        if !response.ok {
            continue;
        }

        if !response.transactions.is_empty() {
            log::info!(
                "live WAL catchup received frames affinity_id={} database_id={} from_peer={} frame_count={}",
                affinity_id,
                database_id,
                socket_addr,
                response.transactions.len()
            );
        }

        for encoded in response.transactions {
            match decode_wal_frame(&encoded) {
                Ok(frame) => {
                    max_seen_by_stream
                        .entry(frame.0.clone())
                        .and_modify(|current| {
                            if frame.1.id.0 > *current {
                                *current = frame.1.id.0;
                            }
                        })
                        .or_insert(frame.1.id.0);
                    imported.push(frame);
                }
                Err(err) => {
                    log::warn!("failed decoding WAL frame during catchup: {}", err);
                }
            }
        }
    }

    if imported.is_empty() {
        return Ok(HashMap::new());
    }

    log::info!(
        "live WAL catchup importing frames affinity_id={} database_id={} frame_count={}",
        affinity_id,
        database_id,
        imported.len()
    );

    let mut app_guard = app.lock().await;
    app_guard.import_wal_records(database_id, imported)?;

    Ok(max_seen_by_stream
        .into_iter()
        .map(|(stream, tx)| (stream, serverlib::TransactionId(tx)))
        .collect())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = std::env::args().collect::<Vec<_>>();
    let server_list = parse_server_list_from_args(&args);
    let affinity_config = parse_affinity_startup_config(&args);

    let node_id = args
        .iter()
        .find_map(|arg| arg.strip_prefix("node_id=").map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_LOCAL_NODE_ID.to_string());

    let swarm_id = args
        .iter()
        .find_map(|arg| arg.strip_prefix("swarm_id=").map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_LOCAL_SWARM_ID.to_string());

    let data_dir = args
        .iter()
        .find_map(|arg| arg.strip_prefix("datadir=").map(std::path::PathBuf::from))
        .unwrap_or_else(|| std::path::PathBuf::from("./data"));

    let listen_addr = args
        .iter()
        .find_map(|arg| arg.strip_prefix("listen_addr=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let port: u16 = args
        .iter()
        .find_map(|arg| arg.strip_prefix("port=").and_then(|v| v.parse().ok()))
        .unwrap_or(common::DEFAULT_SERVER_PORT);

    let advertise_addr = advertised_listen_addr_from_args(&args, &listen_addr);

    log::info!("starting server node_id={} with runtime config: data_dir={}, listen_addr={}, port={}", 
        node_id, data_dir.display(), listen_addr, port);
    if advertise_addr != listen_addr {
        log::info!(
            "server p2p advertise_addr resolved to {} (listen_addr was {})",
            advertise_addr,
            listen_addr
        );
    }

    let runtime = initialize_server_p2p_runtime(
        &node_id,
        &swarm_id,
            &advertise_addr,
        port,
        &server_list,
    )?;

    let peer_addrs = runtime
        .network()
        .discover_peers()
        .iter()
        .flat_map(|peer| peer.addrs.clone())
        .collect::<Vec<_>>();

    if !peer_addrs.is_empty() {
        log::info!(
            "serverlist bootstrap peers registered for kademlia: {}",
            peer_addrs.join(", ")
        );
    }

    let local_node = NodeDescriptor {
        id: NodeId(node_id.clone()),
            addrs: vec![format!("/ip4/{advertise_addr}/tcp/{port}")],
        is_local: true,
    };

    let discovered_peers_for_affinity = runtime.network().discover_peers();

    let p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>> = Arc::new(Mutex::new(runtime));
    let p2p_heartbeat_task = spawn_p2p_heartbeat_task(Arc::clone(&p2p_runtime), local_node.clone());

    let (affinity_processor, affinity_storage) = initialize_affinity_with_persistence(
        affinity_config.as_ref(),
        &local_node,
        discovered_peers_for_affinity.clone(),
        &data_dir,
    );

    let affinity_processor: Arc<Mutex<Option<AffinityProcessor>>> = Arc::new(Mutex::new(affinity_processor));
    let affinity_storage = Arc::new(affinity_storage);

    let config = ServerRuntimeConfig {
        node_id,
        swarm_id,
        data_dir,
        listen_addrs: vec![format!("/ip4/{listen_addr}/tcp/{port}")],
    };

    let mut app = ServerApp::new(config)?;
    app.bootstrap()?;

    let result = app.run_wal_smoke_test()?;

    log::info!(
        "server runtime initialized for node={} with {} active WAL worker(s) and {} probe records",
        app.node_id(),
        result.active_workers,
        result.records_in_primary_table
    );

    let tcp_bind_addr = format!("{}:{}", listen_addr, port);
    let listener = TcpListener::bind(&tcp_bind_addr).await?;
    log::info!("connector request listener bound at {}", tcp_bind_addr);

    let app = Arc::new(Mutex::new(app));
    let app_for_listener = Arc::clone(&app);
    let p2p_runtime_for_listener = Arc::clone(&p2p_runtime);
    let affinity_processor_for_listener = Arc::clone(&affinity_processor);
    let seen_node_announces = Arc::new(Mutex::new(HashSet::<String>::new()));
    let seen_node_announces_for_listener = Arc::clone(&seen_node_announces);
    let active_connections = Arc::new(AtomicUsize::new(0));
    let active_connections_for_listener = Arc::clone(&active_connections);
    let local_node_for_listener = local_node.clone();

    tokio::spawn(async move {

        loop {

            match listener.accept().await {

                Ok((stream, peer_addr)) => {
                    
                    let connection_id =
                        active_connections_for_listener.fetch_add(1, Ordering::SeqCst) + 1;
                    
                    log::info!(
                        "connector peer connected from {} (active_connections={})",
                        peer_addr,
                        connection_id
                    );
                    
                    let app = Arc::clone(&app_for_listener);
                    let p2p_runtime = Arc::clone(&p2p_runtime_for_listener);
                    let affinity_processor = Arc::clone(&affinity_processor_for_listener);
                    let seen_node_announces = Arc::clone(&seen_node_announces_for_listener);
                    let active_connections = Arc::clone(&active_connections_for_listener);
                    let local_node = local_node_for_listener.clone();

                    tokio::spawn(async move {

                        if let Err(err) = handle_connector_stream(
                            stream,
                            app,
                            p2p_runtime,
                            affinity_processor,
                            seen_node_announces,
                            local_node,
                            peer_addr.to_string(),
                            connection_id,
                        )
                        .await
                        {
                            log::warn!(
                                "connector stream handling failed for {}: {}",
                                peer_addr,
                                err
                            );
                        }

                        let remaining = active_connections.fetch_sub(1, Ordering::SeqCst) - 1;
                        
                        log::info!(
                            "connector peer disconnected from {} (active_connections={})",
                            peer_addr,
                            remaining
                        );

                    });
                },

                Err(err) => {
                    log::warn!("listener accept failed: {}", err);
                }

            }
            
        }

    });

    if let Some(config) = &affinity_config {
        
        execute_affinity_join_sequence(
            Arc::clone(&affinity_processor),
            Arc::clone(&affinity_storage),
            config,
            &local_node,
            &discovered_peers_for_affinity,
        )
        .await;

        // Spawn replication task to execute sync phases
        let _replication_task = spawn_affinity_replication_task(
            Arc::clone(&affinity_processor),
            Arc::clone(&affinity_storage),
            Arc::clone(&app),
            Arc::clone(&p2p_runtime),
            config.clone(),
            local_node.clone(),
        );

    }

    log::info!("server process is running; press Ctrl+C to shutdown");
    tokio::signal::ctrl_c().await?;
    log::info!("shutdown signal received");

    p2p_heartbeat_task.abort();

    app.lock().await.shutdown()?;
    drop(p2p_runtime);
    
    Ok(())

}

async fn handle_connector_stream(
    mut stream: TcpStream,
    app: Arc<Mutex<ServerApp>>,
    p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>>,
    affinity_processor: Arc<Mutex<Option<AffinityProcessor>>>,
    seen_node_announces: Arc<Mutex<HashSet<String>>>,
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

        if let Some(message) = decode_service_message(&payload) {
            if let ServiceMessage::NodeAnnounce(node) = &message {
                if !is_valid_server_node(node) {
                    log::debug!(
                        "ignoring invalid server node announce id='{}' addrs='{}' from {}",
                        node.id.0,
                        node.addrs.join(","),
                        peer_addr
                    );
                    continue;
                }
            }

            let message_for_fanout = message.clone();
            let mut runtime = p2p_runtime.lock().await;
            if let Err(err) = runtime.handle_event(ServerP2pEvent::MessageReceived {
                from_peer_id: peer_addr.clone(),
                message,
            }) {
                log::debug!("server p2p message handling failed from {}: {}", peer_addr, err);
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
                let (ok, error, schema_definitions) = match build_schema_definitions_for_database(
                    &app_guard,
                    &schema_req.database_id,
                ) {
                    Ok(definitions) => (true, None, definitions),
                    Err(err) => (false, Some(err), Vec::new()),
                };
                let (schema_identifier, schema_hash) = resolve_schema_catalog(&app_guard, &schema_req.database_id)
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

                // TODO: Query actual data snapshot from app
                // For now, return empty snapshot as placeholder
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
                let (ok, error, transactions) = match app_guard
                    .export_wal_records_for_database(
                        &txn_req.database_id,
                        txn_req.from_transaction_id,
                        Some(&stream_cursors),
                    )
                {
                    Ok(records) => {
                        let encoded = records
                            .into_iter()
                            .filter_map(|frame| {
                                match encode_wal_frame(&frame) {
                                    Ok(encoded) => Some(encoded),
                                    Err(err) => {
                                        log::warn!(
                                            "failed encoding WAL frame for response: {}",
                                            err
                                        );
                                        None
                                    }
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

            // Service p2p frame is one-way over short-lived sockets.
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
            maybe_server_peer_discovery_response(&request, &p2p_runtime, &local_node).await
        {
            if let Err(err) = write_response_frame(&mut stream, response).await {
                rollback_active_session_transaction(&app, &session.session_id).await;
                return Err(err);
            }
            continue;
        }

        if !session.authenticated {

            let auth_outcome = match &request.command {
                ConnectorCommand::Query { query } => extract_auth_token(&query.sql)
                    .map(|token| session.authenticate_if_valid_token(token)),
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

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn extract_auth_token(sql: &str) -> Option<&str> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let token = parts.next()?;
    if command.eq_ignore_ascii_case("password_token") {
        return Some(token);
    }
    None
}

async fn write_response_frame(
    stream: &mut TcpStream,
    response: ConnectorResponse,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = bincode::serialize(&response)?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

async fn write_service_message_to_stream(
    stream: &mut TcpStream,
    message: &ServiceMessage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let payload = encode_service_message(message)
        .ok_or("failed to encode service message")?;
    let len = payload.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(&payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_bootstrap_addr_accepts_multiaddr_passthrough() {
        let addr = "/ip4/127.0.0.1/tcp/9400";
        assert_eq!(normalize_bootstrap_addr(addr), Some(addr.to_string()));
    }

    #[test]
    fn normalize_bootstrap_addr_parses_host_port() {
        assert_eq!(
            normalize_bootstrap_addr("127.0.0.1:9400"),
            Some("/ip4/127.0.0.1/tcp/9400".to_string())
        );
        assert_eq!(
            normalize_bootstrap_addr("node.local:9400"),
            Some("/dns/node.local/tcp/9400".to_string())
        );
    }

    #[test]
    fn multiaddr_to_socket_addr_parses_ip4_and_dns() {
        assert_eq!(
            multiaddr_to_socket_addr("/ip4/127.0.0.1/tcp/4001"),
            Some("127.0.0.1:4001".to_string())
        );
        assert_eq!(
            multiaddr_to_socket_addr("/dns/node.local/tcp/4002"),
            Some("node.local:4002".to_string())
        );
        assert_eq!(multiaddr_to_socket_addr("127.0.0.1:4001"), None);
    }

    #[test]
    fn node_announce_wire_encoding_roundtrips() {
        let message = ServiceMessage::NodeAnnounce(NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        });

        let encoded = encode_service_message(&message).expect("message should encode");
        let decoded = decode_service_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn schema_catalog_wire_encoding_roundtrips() {
        let message = ServiceMessage::SchemaCatalogRequest(
            serverlib::p2p::protocol::SchemaCatalogRequest {
                request_id: "req-1".to_string(),
                affinity_id: "aff-1".to_string(),
                database_id: "main".to_string(),
                expected_schema_identifier: 1,
                expected_schema_hash: Some("hash".to_string()),
            },
        );

        let encoded = encode_service_message(&message).expect("message should encode");
        let decoded = decode_service_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }

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

    #[test]
    fn normalize_bootstrap_addr_parses_bare_port() {
        assert_eq!(
            normalize_bootstrap_addr("4001"),
            Some("/ip4/127.0.0.1/tcp/4001".to_string())
        );
        assert_eq!(
            normalize_bootstrap_addr(":4002"),
            Some("/ip4/127.0.0.1/tcp/4002".to_string())
        );
    }

    #[test]
    fn parse_server_list_from_args_dedups_and_normalizes() {
        let args = vec![
            "server".to_string(),
            "servers=127.0.0.1:9400,node.local:9400,127.0.0.1:9400".to_string(),
        ];

        let parsed = parse_server_list_from_args(&args);

        assert_eq!(
            parsed,
            vec![
                "/ip4/127.0.0.1/tcp/9400".to_string(),
                "/dns/node.local/tcp/9400".to_string(),
            ]
        );
    }

    #[test]
    fn advertised_listen_addr_defaults_wildcard_to_localhost() {
        let args = vec!["server".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "127.0.0.1".to_string()
        );
        assert_eq!(
            advertised_listen_addr_from_args(&args, "192.168.1.10"),
            "192.168.1.10".to_string()
        );
    }

    #[test]
    fn advertised_listen_addr_prefers_explicit_override() {
        let args = vec!["server".to_string(), "advertise_addr=10.1.1.5".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "10.1.1.5".to_string()
        );
    }

    #[test]
    fn bootstrap_nodes_use_normalized_addrs() {
        let nodes = bootstrap_nodes_from_server_list(&[
            "/ip4/127.0.0.1/tcp/9400".to_string(),
            "/dns/node.local/tcp/9400".to_string(),
        ]);

        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].addrs, vec!["/ip4/127.0.0.1/tcp/9400".to_string()]);
        assert_eq!(nodes[1].addrs, vec!["/dns/node.local/tcp/9400".to_string()]);
        assert!(nodes.iter().all(|node| !node.id.0.is_empty()));
    }

    #[test]
    fn parse_affinity_startup_config_parses_key_colon_password() {
        let args = vec![
            "server".to_string(),
            "affinity=team-a:secret".to_string(),
        ];

        let cfg = parse_affinity_startup_config(&args).expect("config should parse");
        assert_eq!(cfg.affinity_id, "team-a");
        assert!(!cfg.affinity_key.is_empty());

        let missing_password = vec!["server".to_string(), "affinity=team-a".to_string()];
        assert!(parse_affinity_startup_config(&missing_password).is_none());

        let empty_spec = vec!["server".to_string(), "affinity=:".to_string()];
        assert!(parse_affinity_startup_config(&empty_spec).is_none());
    }

    #[test]
    fn build_affinity_document_snapshot_includes_local_and_discovered_nodes() {
        let cfg = AffinityStartupConfig {
            affinity_id: "team-a".to_string(),
            affinity_key: "k1".to_string(),
        };
        let local_node = NodeDescriptor {
            id: NodeId("sam01".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: true,
        };
        let discovered = vec![NodeDescriptor {
            id: NodeId("sam02".to_string()),
            addrs: vec!["/ip4/127.0.0.1/tcp/4002".to_string()],
            is_local: false,
        }];

        let doc = build_affinity_document_snapshot(&cfg, &local_node, discovered);
        assert_eq!(doc.affinity_id, "team-a");
        assert_eq!(doc.members.len(), 2);
        assert!(doc
            .members
            .iter()
            .any(|member| member.node_id.0 == "sam01" && member.status == AffinityMemberStatus::Online));
        assert!(doc.members.iter().any(|member| member.node_id.0 == "sam02"));
    }
}
