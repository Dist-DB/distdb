use crate::core::{ConnectorError, ConnectorRequest, ConnectorResponse, ConnectorTransport};

use std::collections::HashMap;

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
}

impl ConnectorP2pTransport {
    pub fn new(config: ConnectorP2pConfig) -> Self {
        Self {
            config,
            peers: HashMap::new(),
            active_peer_id: None,
            queued_responses: HashMap::new(),
        }
    }

    pub fn discovery_mode(&self) -> ConnectorDiscoveryMode {
        ConnectorDiscoveryMode::Kademlia
    }

    pub fn protocol(&self) -> &str {
        &self.config.protocol
    }

    pub fn upsert_peer(&mut self, peer: ConnectorPeer) {
        let peer_id = peer.peer_id.clone();
        self.peers.insert(peer_id.clone(), peer);

        // First discovered peer becomes the sticky session peer.
        if self.active_peer_id.is_none() {
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
            self.active_peer_id = Some(peer_id.to_string());
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
        self.queued_responses
            .insert(response.request_id.clone(), response);
    }
}

impl ConnectorTransport for ConnectorP2pTransport {
    fn request(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError> {
        if self.peers.is_empty() && self.config.bootstrap_peers.is_empty() {
            return Err(ConnectorError::Transport(
                "no Kademlia peers available for routing".to_string(),
            ));
        }

        if self.active_peer_id.is_none() {
            return Err(ConnectorError::Transport(
                "no connected peer selected for session routing".to_string(),
            ));
        }

        self.queued_responses
            .get(&request.request_id)
            .cloned()
            .ok_or_else(|| {
                ConnectorError::Transport(
                    "no queued response for request_id (network loop not wired yet)"
                        .to_string(),
                )
            })
    }
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
}