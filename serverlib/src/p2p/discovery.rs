use crate::core::cluster::NodeDescriptor;
use crate::core::identity::NodeId;

use std::collections::HashMap;

pub trait DiscoveryService {
    fn discover_peers(&self) -> Vec<NodeDescriptor>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscoveryMode {
    Kademlia,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KademliaDiscoveryConfig {
    pub protocol: String,
    pub bootstrap_nodes: Vec<NodeDescriptor>,
}

impl KademliaDiscoveryConfig {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            bootstrap_nodes: Vec::new(),
        }
    }

    pub fn with_bootstrap_nodes(mut self, nodes: Vec<NodeDescriptor>) -> Self {
        self.bootstrap_nodes = nodes;
        self
    }
}

#[derive(Debug, Clone)]
pub struct KademliaDiscoveryService {
    local_node_id: NodeId,
    config: KademliaDiscoveryConfig,
    peers: HashMap<NodeId, NodeDescriptor>,
}

impl KademliaDiscoveryService {
    pub fn new(local_node_id: NodeId, config: KademliaDiscoveryConfig) -> Self {
        let mut peers = HashMap::new();
        for node in &config.bootstrap_nodes {
            peers.insert(node.id.clone(), node.clone());
        }

        Self {
            local_node_id,
            config,
            peers,
        }
    }

    pub fn mode(&self) -> DiscoveryMode {
        DiscoveryMode::Kademlia
    }

    pub fn protocol(&self) -> &str {
        &self.config.protocol
    }

    pub fn upsert_peer(&mut self, node: NodeDescriptor) {
        if node.id != self.local_node_id {
            self.peers.insert(node.id.clone(), node);
        }
    }
}

impl DiscoveryService for KademliaDiscoveryService {
    fn discover_peers(&self) -> Vec<NodeDescriptor> {
        self.peers.values().cloned().collect()
    }
}


#[cfg(test)]
#[path = "discovery_test.rs"]
mod tests;
