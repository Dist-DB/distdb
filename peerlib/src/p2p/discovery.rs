use crate::p2p::types::PeerNode;

use std::collections::HashMap;

pub trait DiscoveryService {
    fn discover_peers(&self) -> Vec<PeerNode>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryMode {
    Kademlia,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KademliaDiscoveryConfig {
    pub protocol: String,
    pub bootstrap_nodes: Vec<PeerNode>,
}

impl KademliaDiscoveryConfig {

    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            bootstrap_nodes: Vec::new(),
        }
    }

    pub fn with_bootstrap_nodes(mut self, nodes: Vec<PeerNode>) -> Self {
        self.bootstrap_nodes = nodes;
        self
    }

}

#[derive(Debug, Clone)]
pub struct KademliaDiscoveryService {
    local_node_id: String,
    config: KademliaDiscoveryConfig,
    bootstrap_nodes: Vec<PeerNode>,
    peers: HashMap<String, PeerNode>,
}

impl KademliaDiscoveryService {
    
    pub fn new(local_node_id: impl Into<String>, config: KademliaDiscoveryConfig) -> Self {
        Self {
            local_node_id: local_node_id.into(),
            bootstrap_nodes: config.bootstrap_nodes.clone(),
            config,
            peers: HashMap::new(),
        }
    }

    pub fn mode(&self) -> DiscoveryMode {
        DiscoveryMode::Kademlia
    }

    pub fn protocol(&self) -> &str {
        &self.config.protocol
    }

    pub fn upsert_peer(&mut self, node: PeerNode) {
        
        if node.id != self.local_node_id {
            let mut remote = node;
            remote.is_local = false;
            self.peers.insert(remote.id.clone(), remote);
        }

    }

    pub fn bootstrap_nodes(&self) -> &[PeerNode] {
        &self.bootstrap_nodes
    }

}

impl DiscoveryService for KademliaDiscoveryService {
    fn discover_peers(&self) -> Vec<PeerNode> {
        self.peers.values().cloned().collect()
    }
}


#[cfg(test)]
#[path = "discovery_test.rs"]
mod tests;
