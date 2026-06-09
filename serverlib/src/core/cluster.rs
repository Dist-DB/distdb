use std::collections::HashMap;

use super::identity::NodeId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeDescriptor {
    pub id: NodeId,
    pub addrs: Vec<String>,
    pub is_local: bool,
}

#[derive(Debug, Default)]
pub struct ClusterState {
    nodes: HashMap<NodeId, NodeDescriptor>,
}

impl ClusterState {
    pub fn upsert_node(&mut self, node: NodeDescriptor) {
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn all_nodes(&self) -> impl Iterator<Item = &NodeDescriptor> {
        self.nodes.values()
    }

    pub fn get(&self, id: &NodeId) -> Option<&NodeDescriptor> {
        self.nodes.get(id)
    }
}