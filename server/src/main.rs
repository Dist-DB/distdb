use server::core::app::ServerApp;
use server::core::config::{ServerRuntimeConfig, DEFAULT_LOCAL_NODE_ID, DEFAULT_LOCAL_SWARM_ID};

use common::helpers::stable_id;
use common::helpers::utils::md5_hash;
use common::helpers::{aes_decrypt, aes_encrypt};
use common::{PeerSession, SessionLog, SessionLogEventType};
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, FieldDef,
    FieldIndex, FieldType, MutationResult, QueryResult, QueryTimings,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::p2p::protocol::ServiceMessage;
use serverlib::p2p::transport::Transport;
use serverlib::{KademliaDiscoveryConfig, KademliaDiscoveryService, ServerP2pEvent, ServerP2pNetwork, ServerP2pRuntime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::HashSet, net::Ipv4Addr};

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";

// we change these later (accessing the database structures...)

const SERVER_TEMP_PASSWORD: &str = "password";
const SERVER_TEMP_USER: &str = "root";
const SERVER_TEMP_TOKEN_SALT: &[u8; 8] = b"distdbv1";

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
            return Err(serverlib::helpers::error::ServerLibError::Network(
                "broadcast failed to reach any peer".to_string(),
            ));
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
        
        let session_id = md5_hash(format!("{}:{}:{}", SERVER_TEMP_USER, peer_addr, challenge_id).as_str());
        
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
    match message {
        ServiceMessage::NodeAnnounce(node) => {
            let addrs = node.addrs.join(",");
            let payload = format!(
                "node_announce|{}|{}|{}",
                node.id.0,
                addrs,
                if node.is_local { "1" } else { "0" }
            );
            Some(payload.into_bytes())
        }
        _ => None,
    }
}

fn decode_service_message(payload: &[u8]) -> Option<ServiceMessage> {
    let text = std::str::from_utf8(payload).ok()?;
    let mut parts = text.splitn(4, '|');
    let kind = parts.next()?;

    if kind != "node_announce" {
        return None;
    }

    let node_id = parts.next()?.trim();
    let addrs_str = parts.next()?.trim();
    let is_local_str = parts.next()?.trim();

    if node_id.is_empty() {
        return None;
    }

    let addrs = if addrs_str.is_empty() {
        Vec::new()
    } else {
        addrs_str
            .split(',')
            .map(|addr| addr.trim().to_string())
            .filter(|addr| !addr.is_empty())
            .collect::<Vec<_>>()
    };

    Some(ServiceMessage::NodeAnnounce(NodeDescriptor {
        id: NodeId(node_id.to_string()),
        addrs,
        is_local: is_local_str == "1",
    }))
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = std::env::args().collect::<Vec<_>>();
    let server_list = parse_server_list_from_args(&args);

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

    let p2p_runtime: Arc<Mutex<ServerP2pRuntime<TcpServerTransport>>> = Arc::new(Mutex::new(runtime));
    let p2p_heartbeat_task = spawn_p2p_heartbeat_task(Arc::clone(&p2p_runtime), local_node.clone());

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
                    let seen_node_announces = Arc::clone(&seen_node_announces_for_listener);
                    let active_connections = Arc::clone(&active_connections_for_listener);
                    let local_node = local_node_for_listener.clone();
                    tokio::spawn(async move {
                        if let Err(err) = handle_connector_stream(
                            stream,
                            app,
                            p2p_runtime,
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

        let request = match bincode::deserialize::<ConnectorRequest>(&payload) {
            Ok(request) => request,
            Err(_) => {
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

                    if let ServiceMessage::NodeAnnounce(node) = message_for_fanout {
                        let dedup_key = node_announce_dedup_key(&node);
                        let should_fanout = {
                            let mut seen = seen_node_announces.lock().await;
                            seen.insert(dedup_key)
                        };

                        if should_fanout {
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
}
