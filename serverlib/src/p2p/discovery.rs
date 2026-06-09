use crate::core::cluster::NodeDescriptor;

pub trait DiscoveryService {
    fn discover_peers(&self) -> Vec<NodeDescriptor>;
}