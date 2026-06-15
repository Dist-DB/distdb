use crate::core::{
    ConnectorError, ConnectorRequest, ConnectorResponse, ConnectorResult,
    ConnectorTransport, ResponseStatus,
};

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};

use common::{DEFAULT_SERVER_PORT, PeerSession, epoch_nanos};
use common::helpers::utils::{md5};

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorDiscoveryMode {
    Kademlia,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorP2pConfig {
    pub protocol: String,
    pub bootstrap_peers: Vec<String>,
}

impl ConnectorP2pConfig {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            bootstrap_peers: Vec::new(),
        }
    }

    pub fn with_bootstrap_peers(mut self, peers: Vec<String>) -> Self {
        self.bootstrap_peers = peers;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorPeer {
    pub peer_id: String,
    pub addrs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ConnectorP2pTransport {
    config: ConnectorP2pConfig,
    peers: HashMap<String, ConnectorPeer>,
    active_peer_id: Option<String>,
    queued_responses: HashMap<String, ConnectorResponse>,
    live_connection: Arc<Mutex<Option<LiveConnection>>>,
}

#[derive(Debug)]
struct LiveConnection {
    peer_id: String,
    stream: TcpStream,
    session: PeerSession,
}

impl ConnectorP2pTransport {
    pub fn new(config: ConnectorP2pConfig) -> Self {
        Self {
            config,
            peers: HashMap::new(),
            active_peer_id: None,
            queued_responses: HashMap::new(),
            live_connection: Arc::new(Mutex::new(None)),
        }
    }

    pub fn discovery_mode(&self) -> ConnectorDiscoveryMode {
        ConnectorDiscoveryMode::Kademlia
    }

    pub fn protocol(&self) -> &str {
        &self.config.protocol
    }

    pub fn bootstrap_peers(&self) -> &[String] {
        &self.config.bootstrap_peers
    }

    pub fn upsert_peer(&mut self, peer: ConnectorPeer) {
        let peer_id = peer.peer_id.clone();
        log::debug!(
            "connector transport upsert peer peer_id={} addrs={}",
            peer_id,
            peer.addrs.join(",")
        );

        let stale_peer_ids = self
            .peers
            .iter()
            .filter(|(existing_peer_id, existing_peer)| {
                **existing_peer_id != peer_id
                    && existing_peer
                        .addrs
                        .iter()
                        .any(|existing_addr| peer.addrs.iter().any(|new_addr| new_addr == existing_addr))
            })
            .map(|(existing_peer_id, _)| existing_peer_id.clone())
            .collect::<Vec<_>>();

        let active_was_stale = stale_peer_ids
            .iter()
            .any(|stale_peer_id| self.active_peer_id.as_deref() == Some(stale_peer_id.as_str()));

        for stale_peer_id in stale_peer_ids {
            log::debug!(
                "connector transport replacing stale peer identity old_peer_id={} new_peer_id={}",
                stale_peer_id,
                peer_id
            );
            self.peers.remove(&stale_peer_id);
        }

        self.peers.insert(peer_id.clone(), peer);

        // First discovered peer becomes the sticky session peer.
        if self.active_peer_id.is_none() || active_was_stale {
            self.active_peer_id = Some(peer_id);
        }
    }

    pub fn discovered_peers(&self) -> Vec<ConnectorPeer> {
        self.peers.values().cloned().collect()
    }

    pub fn active_peer_id(&self) -> Option<&str> {
        self.active_peer_id.as_deref()
    }

    pub fn select_peer(&mut self, peer_id: impl AsRef<str>) -> Result<(), ConnectorError> {
        let peer_id = peer_id.as_ref();
        if self.peers.contains_key(peer_id) {
            if self.active_peer_id.as_deref() != Some(peer_id) {
                self.clear_live_connection("peer switch");
            }
            self.active_peer_id = Some(peer_id.to_string());
            log::info!("connector transport active peer set to {}", peer_id);
            return Ok(());
        }

        Err(ConnectorError::Transport(format!(
            "peer '{peer_id}' is not discovered"
        )))
    }

    pub fn active_peer(&self) -> Option<&ConnectorPeer> {
        self.active_peer_id
            .as_ref()
            .and_then(|peer_id| self.peers.get(peer_id))
    }

    /// Queue a response by request id. This is used by tests and by future
    /// network handlers that decode p2p responses and hand them to the client.
    pub fn queue_response(&mut self, response: ConnectorResponse) {
        log::debug!(
            "connector transport queue response request_id={} status={:?}",
            response.request_id,
            response.status
        );
        self.queued_responses
            .insert(response.request_id.clone(), response);
    }

    pub fn queued_response_count(&self) -> usize {
        self.queued_responses.len()
    }

    pub fn has_live_connection(&self) -> bool {
        self.live_connection
            .lock()
            .map(|connection| connection.is_some())
            .unwrap_or(false)
    }

    pub fn connect_active_peer(&self) -> Result<(), ConnectorError> {
        let Some(peer) = self.active_peer() else {
            return Err(ConnectorError::Transport(
                "no connected peer selected for session routing".to_string(),
            ));
        };

        ensure_live_connection(self, peer)
    }

    pub fn disconnect_active_peer(&self) {
        self.clear_live_connection("disconnect directive");
    }

    pub fn set_session_auth_token(&self, token: Option<String>) -> Result<(), ConnectorError> {
        let mut connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_mut() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for auth token update".to_string(),
            ));
        };

        live.session.auth_token = token;
        Ok(())
    }

    pub fn session_auth_token(&self) -> Result<Option<String>, ConnectorError> {
        let connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_ref() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for auth token retrieval".to_string(),
            ));
        };

        Ok(live.session.auth_token.clone())
    }

    pub fn session_id(&self) -> Result<Option<String>, ConnectorError> {
        let connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_ref() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for session id retrieval".to_string(),
            ));
        };

        Ok(live.session.session_id.clone())
    }

    fn clear_live_connection(&self, reason: &str) {
        if let Ok(mut connection) = self.live_connection.lock() {
            if let Some(live) = connection.take() {
                log::info!(
                    "connector transport disconnected peer={} reason={}",
                    live.peer_id,
                    reason
                );
            }
        }
    }
}

impl ConnectorTransport for ConnectorP2pTransport {

    fn request(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError> {

        if self.peers.is_empty() && self.config.bootstrap_peers.is_empty() {
            log::warn!("connector transport request failed: no peers or bootstrap peers configured");
            return Err(ConnectorError::Transport(
                "no Kademlia peers available for routing".to_string(),
            ));
        }

        if self.active_peer_id.is_none() {
            log::warn!("connector transport request failed: no active peer selected");
            return Err(ConnectorError::Transport(
                "no connected peer selected for session routing".to_string(),
            ));
        }

        let has_live_connection = self.has_live_connection();

        if let Some(active_peer) = self.active_peer_id() {
            log::debug!(
                "connector transport routing request_id={} to peer={}",
                request.request_id,
                active_peer
            );
        }

        if has_live_connection {
            if let Some(peer) = self.active_peer() {
            match send_request_over_tcp(self, peer, request) {
                Ok(response) => {
                    log::debug!(
                        "connector transport received network response request_id={} status={:?}",
                        response.request_id,
                        response.status
                    );
                    return Ok(response);
                }
                Err(err) => {
                    log::warn!(
                        "connector transport network request failed for request_id={}: {}",
                        request.request_id,
                        err
                    );
                }
            }
            }
        }

        self.queued_responses
            .get(&request.request_id)
            .cloned()
            .ok_or_else(|| {
                log::warn!(
                    "connector transport has no queued response for request_id={}",
                    request.request_id
                );
                if !has_live_connection {
                    ConnectorError::Transport(
                        "no active peer connection; run `connect <user@peer-id>;` first"
                            .to_string(),
                    )
                } else {
                    ConnectorError::Transport(
                        "no queued response for request_id; p2p request/response loop is not wired yet"
                            .to_string(),
                    )
                }
            })
    
    }

}

fn send_request_over_tcp(
    transport: &ConnectorP2pTransport,
    peer: &ConnectorPeer,
    request: &ConnectorRequest,
) -> Result<ConnectorResponse, ConnectorError> {

    ensure_live_connection(transport, peer)?;

    let mut connection = transport
        .live_connection
        .lock()
        .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

    let response = {
        let Some(live) = connection.as_mut() else {
            return Err(ConnectorError::Transport(
                "active connection missing after connect".to_string(),
            ));
        };
        send_request_frame(&mut live.stream, request)
    };

    if response.is_err() {
        let _ = connection.take();
    }

    response

}

fn ensure_live_connection(
    transport: &ConnectorP2pTransport,
    peer: &ConnectorPeer,
) -> Result<(), ConnectorError> {

    let mut connection = transport
        .live_connection
        .lock()
        .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

    let should_reconnect = connection
        .as_ref()
        .map(|live| live.peer_id != peer.peer_id)
        .unwrap_or(true);

    if !should_reconnect {
        return Ok(());
    }

    let Some(addr) = peer.addrs.first() else {
        return Err(ConnectorError::Transport(
            "active peer has no address for routing".to_string(),
        ));
    };

    let socket_addr = normalize_peer_addr(addr);
    let mut stream = TcpStream::connect(&socket_addr)
        .map_err(|e| ConnectorError::Transport(format!("failed to connect to {socket_addr}: {e}")))?;

    let challenge = read_response_frame(&mut stream)?;
    if challenge.request_id != SERVER_PASSWORD_CHALLENGE_REQUEST_ID {
        return Err(ConnectorError::InvalidResponse(format!(
            "missing server password challenge on connect; received request_id='{}'",
            challenge.request_id
        )));
    }

    match (&challenge.status, &challenge.result) {
        (ResponseStatus::Rejected, ConnectorResult::Error(_message)) => {}
        _ => {
            return Err(ConnectorError::InvalidResponse(
                "server challenge frame had unexpected status/result".to_string(),
            ));
        }
    }

    log::info!(
        "connector transport established persistent stream peer={} addr={}",
        peer.peer_id,
        socket_addr
    );

    let server_session_id = match &challenge.result {
        ConnectorResult::Error(message) => extract_session_id(message),
        _ => None,
    };
    let shared_session_token = generate_shared_session_token(
        &peer.peer_id,
        server_session_id.as_deref(),
    );

    *connection = Some(LiveConnection {
        peer_id: peer.peer_id.clone(),
        stream,
        session: PeerSession::new().with_session_id(shared_session_token),
    });

    Ok(())

}

fn send_request_frame(
    stream: &mut TcpStream,
    request: &ConnectorRequest,
) -> Result<ConnectorResponse, ConnectorError> {

    let payload = bincode::serialize(request).map_err(|e| {
        ConnectorError::Transport(format!("failed to serialize request payload: {e}"))
    })?;

    let len = payload.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|e| ConnectorError::Transport(format!("failed to write request: {e}")))?;

    read_response_frame(stream)

}

fn read_response_frame(stream: &mut TcpStream) -> Result<ConnectorResponse, ConnectorError> {

    let mut response_len_buf = [0u8; 4];
    stream
        .read_exact(&mut response_len_buf)
        .map_err(|e| ConnectorError::Transport(format!("failed to read response length: {e}")))?;

    let response_len = u32::from_le_bytes(response_len_buf) as usize;
    let mut response_buf = vec![0u8; response_len];

    stream
        .read_exact(&mut response_buf)
        .map_err(|e| ConnectorError::Transport(format!("failed to read response payload: {e}")))?;

    bincode::deserialize::<ConnectorResponse>(&response_buf)
        .map_err(|e| ConnectorError::Transport(format!("failed to decode response payload: {e}")))

}

fn normalize_peer_addr(raw: &str) -> String {

    let trimmed = raw.trim();

    if let Some(rest) = trimmed.strip_prefix("/ip4/") {
        if let Some((host, port)) = rest.split_once("/tcp/") {
            if !host.is_empty() && port.parse::<u16>().is_ok() {
                return format!("{host}:{port}");
            }
        }
    }

    if let Some(rest) = trimmed.strip_prefix("/dns/") {
        if let Some((host, port)) = rest.split_once("/tcp/") {
            if !host.is_empty() && port.parse::<u16>().is_ok() {
                return format!("{host}:{port}");
            }
        }
    }

    if trimmed.contains(':') {
        return trimmed.to_string();
    }
    
    format!("{}:{}", trimmed, DEFAULT_SERVER_PORT)

}

fn extract_session_id(message: &str) -> Option<String> {

    for part in message.split_whitespace() {

        if let Some(value) = part.strip_prefix("session_id=") {
            let token = value.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }

        // Backward compatibility for servers that still emit the old label.
        if let Some(value) = part.strip_prefix("shared_authorization=") {
            let token = value.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    
    }
    
    None

}

fn generate_shared_session_token(peer_id: &str, server_token: Option<&str>) -> String {

    let entropy = format!(
        "{}:{}:{}",
        peer_id,
        epoch_nanos!(),
        server_token.unwrap_or("server-token-unavailable")
    );
    
    md5(entropy.as_bytes())

}

#[cfg(test)]
mod tests {
    
    use super::*;
    use crate::core::{
        ConnectorCommand, ConnectorRequest, ConnectorResult, MutationResult,
        ResponseStatus,
    };

    #[test]
    fn transport_uses_kademlia_mode() {
        let transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));
        assert_eq!(transport.discovery_mode(), ConnectorDiscoveryMode::Kademlia);
    }

    #[test]
    fn request_fails_when_no_peers_are_available() {
        let transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));
        let req = ConnectorRequest::new(
            "req-1",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let result = transport.request(&req);
        assert!(matches!(result, Err(ConnectorError::Transport(_))));
    }

    #[test]
    fn queued_response_is_returned_for_matching_request() {
        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap-peer-1".to_string()]),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
        });

        transport.queue_response(ConnectorResponse {
            request_id: "req-9".to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Mutation(MutationResult { affected_rows: 2 }),
        });

        let req = ConnectorRequest::new(
            "req-9",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let response = transport.request(&req).expect("response should be routed");
        assert_eq!(response.request_id, "req-9");
        assert_eq!(response.status, ResponseStatus::Applied);
    }

    #[test]
    fn first_discovered_peer_becomes_active_session_peer() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
        });

        assert_eq!(transport.active_peer_id(), Some("peer-1"));
    }

    #[test]
    fn select_peer_switches_active_session_peer() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
        });
        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-2".to_string(),
            addrs: vec!["/ip4/10.0.0.2/tcp/4001".to_string()],
        });

        transport
            .select_peer("peer-2")
            .expect("peer switch should succeed");

        assert_eq!(transport.active_peer_id(), Some("peer-2"));
    }

    #[test]
    fn upsert_peer_replaces_stale_identity_when_addr_matches() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        });

        transport.upsert_peer(ConnectorPeer {
            peer_id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        });

        let peers = transport.discovered_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer_id, "sam01");
        assert_eq!(transport.active_peer_id(), Some("sam01"));
    }

    #[test]
    fn normalize_peer_addr_parses_supported_multiaddrs() {
        assert_eq!(
            normalize_peer_addr("/ip4/127.0.0.1/tcp/4001"),
            "127.0.0.1:4001"
        );
        assert_eq!(
            normalize_peer_addr("/dns/server-node-01/tcp/9400"),
            "server-node-01:9400"
        );
    }

    #[test]
    fn normalize_peer_addr_keeps_host_port_and_defaults_port() {
        assert_eq!(normalize_peer_addr("127.0.0.1:4001"), "127.0.0.1:4001");
        assert_eq!(
            normalize_peer_addr("localhost"),
            format!("localhost:{}", DEFAULT_SERVER_PORT)
        );
    }
    
}