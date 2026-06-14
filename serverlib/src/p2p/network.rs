use crate::core::cluster::NodeDescriptor;
use crate::helpers::error::Result;
use crate::p2p::discovery::{DiscoveryService, KademliaDiscoveryService};
use crate::p2p::protocol::ServiceMessage;
use crate::p2p::transport::Transport;

#[derive(Debug)]
pub struct ServerP2pNetwork<T: Transport> {
    discovery: KademliaDiscoveryService,
    transport: T,
}

impl<T: Transport> ServerP2pNetwork<T> {
    pub fn new(discovery: KademliaDiscoveryService, transport: T) -> Self {
        Self {
            discovery,
            transport,
        }
    }

    pub fn discover_peers(&self) -> Vec<NodeDescriptor> {
        let peers = self.discovery.discover_peers();
        log::debug!("server p2p discover peers count={}", peers.len());
        peers
    }

    pub fn upsert_discovered_peer(&mut self, node: NodeDescriptor) {
        log::debug!(
            "server p2p upsert discovered peer peer_id={} addrs={}",
            node.id.0,
            node.addrs.join(",")
        );
        self.discovery.upsert_peer(node);
    }

    pub fn broadcast_announce(&mut self, local: NodeDescriptor) -> Result<()> {
        log::info!(
            "server p2p broadcast announce peer_id={} addrs={}",
            local.id.0,
            local.addrs.join(",")
        );
        self.transport.broadcast(ServiceMessage::NodeAnnounce(local))
    }

    pub fn send_message(&mut self, peer_id: &str, message: ServiceMessage) -> Result<()> {
        log::debug!("server p2p send message to peer_id={} message={:?}", peer_id, message);
        self.transport.send(peer_id, message)
    }

    pub fn discovery(&self) -> &KademliaDiscoveryService {
        &self.discovery
    }
}


#[cfg(test)]
#[path = "network_test.rs"]
mod tests;
