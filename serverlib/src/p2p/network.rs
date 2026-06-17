use crate::core::cluster::NodeDescriptor;
use crate::helpers::error::{Result, ServerLibError};
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

    #[expect(clippy::let_and_return, reason="clarity in logging before return")]
    pub fn discover_peers(&self) -> Vec<NodeDescriptor> {
        let peers = self.discovery.discover_peers();
        // log::debug!("server p2p discover peers count={}", peers.len());
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

        // Backward compatibility path: allow direct address sends where caller
        // already provides a transport-routable destination.
        if peer_id.starts_with('/') || peer_id.contains(':') {
            return self.transport.send(peer_id, message);
        }

        let resolved_addrs = self
            .discover_peers()
            .into_iter()
            .find(|peer| peer.id.0 == peer_id)
            .map(|peer| peer.addrs)
            .or_else(|| {
                self.discovery
                    .bootstrap_nodes()
                    .iter()
                    .find(|peer| peer.id.0 == peer_id)
                    .map(|peer| peer.addrs.clone())
            })
            .unwrap_or_default();

        if resolved_addrs.is_empty() {
            return Err(ServerLibError::Network(format!(
                "peer '{}' has no routable addresses in discovery",
                peer_id
            )));
        }

        let mut last_err: Option<ServerLibError> = None;
        for addr in resolved_addrs {
            match self.transport.send(&addr, message.clone()) {
                Ok(()) => return Ok(()),
                Err(err) => {
                    log::debug!(
                        "server p2p send attempt failed peer_id={} addr={} err={}",
                        peer_id,
                        addr,
                        err
                    );
                    last_err = Some(err);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| {
            ServerLibError::Network(format!(
                "failed sending message to peer '{}' via all known addresses",
                peer_id
            ))
        }))
    }

    pub fn discovery(&self) -> &KademliaDiscoveryService {
        &self.discovery
    }

}


#[cfg(test)]
#[path = "network_test.rs"]
mod tests;
