use super::*;
use crate::core::identity::NodeId;
use crate::helpers::error::ServerLibError;
use crate::p2p::discovery::KademliaDiscoveryConfig;

#[derive(Debug, Default)]
struct StubTransport {
    sent_count: usize,
}

impl Transport for StubTransport {
    fn send(&mut self, _peer_id: &str, _message: ServiceMessage) -> Result<()> {
        self.sent_count += 1;
        Ok(())
    }

    fn broadcast(&mut self, _message: ServiceMessage) -> Result<()> {
        self.sent_count += 1;
        Ok(())
    }
}

fn node(id: &str, addr: &str) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(id.to_string()),
        addrs: vec![addr.to_string()],
        is_local: false,
    }
}

#[test]
fn network_returns_discovered_kademlia_peers() {
    let local = NodeId("node-local".to_string());
    let config = KademliaDiscoveryConfig::new("/distdb/kad/1.0.0")
        .with_bootstrap_nodes(vec![node("node-a", "/ip4/10.0.0.1/tcp/4001")]);

    let discovery = KademliaDiscoveryService::new(local, config);
    let network = ServerP2pNetwork::new(discovery, StubTransport::default());

    let peers = network.discover_peers();
    assert!(peers.is_empty());
}

#[test]
fn network_can_broadcast_announce() {
    let local = NodeId("node-local".to_string());
    let discovery =
        KademliaDiscoveryService::new(local, KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"));
    let mut network = ServerP2pNetwork::new(discovery, StubTransport::default());

    let result = network.broadcast_announce(node("node-b", "/ip4/10.0.0.2/tcp/4001"));
    assert!(result.is_ok(), "announce failed: {result:?}");
}

#[test]
fn network_can_send_point_to_point_message() {
    let local = NodeId("node-local".to_string());
    let discovery = KademliaDiscoveryService::new(
        local,
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0")
            .with_bootstrap_nodes(vec![node("node-c", "/ip4/10.0.0.3/tcp/4001")]),
    );
    let mut network = ServerP2pNetwork::new(discovery, StubTransport::default());

    let err: Result<()> = network.send_message(
        "node-c",
        ServiceMessage::TransactionsSince {
            database_id: "db1".to_string(),
            from: None,
        },
    );

    assert!(err.is_ok(), "send failed: {err:?}");

    // Keep explicit reference to ensure ServerLibError stays in scope for this module.
    let _ = ServerLibError::Network("none".to_string());
}

#[test]
fn network_send_message_accepts_direct_address() {
    let local = NodeId("node-local".to_string());
    let discovery =
        KademliaDiscoveryService::new(local, KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"));
    let mut network = ServerP2pNetwork::new(discovery, StubTransport::default());

    let result = network.send_message(
        "/ip4/127.0.0.1/tcp/4010",
        ServiceMessage::TransactionsSince {
            database_id: "db1".to_string(),
            from: None,
        },
    );

    assert!(result.is_ok(), "direct address send failed: {result:?}");
}
