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
mod tests {
    use super::*;

    fn node(id: &str, addr: &str) -> NodeDescriptor {
        NodeDescriptor {
            id: NodeId(id.to_string()),
            addrs: vec![addr.to_string()],
            is_local: false,
        }
    }

    #[test]
    fn local_node_is_not_added_to_discovered_peers() {
        let local = NodeId("node-local".to_string());
        let mut discovery = KademliaDiscoveryService::new(
            local.clone(),
            KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
        );

        discovery.upsert_peer(NodeDescriptor {
            id: local,
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: true,
        });

        assert!(discovery.discover_peers().is_empty());
    }

    #[test]
    fn bootstrap_nodes_are_available_as_discovered_peers() {
        let config = KademliaDiscoveryConfig::new("/distdb/kad/1.0.0")
            .with_bootstrap_nodes(vec![node("node-a", "/ip4/10.0.0.1/tcp/4001")]);
        let discovery = KademliaDiscoveryService::new(NodeId("node-local".to_string()), config);

        let peers = discovery.discover_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].id.0, "node-a");
    }
}