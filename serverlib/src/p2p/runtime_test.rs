use super::*;
use crate::core::identity::NodeId;
use crate::p2p::discovery::{KademliaDiscoveryConfig, KademliaDiscoveryService};

#[derive(Debug, Default)]
struct StubTransport;

impl Transport for StubTransport {
    fn send(&mut self, _peer_id: &str, _message: ServiceMessage) -> Result<()> {
        Ok(())
    }

    fn broadcast(&mut self, _message: ServiceMessage) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct StubSwarmSource {
    events: Vec<ServerP2pEvent>,
}

impl ServerSwarmEventSource for StubSwarmSource {
    fn next_event(&mut self, _idle_wait: Duration) -> Option<ServerP2pEvent> {
        if self.events.is_empty() {
            None
        } else {
            Some(self.events.remove(0))
        }
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
fn runtime_processes_discovery_and_announce_events() {
    let discovery = KademliaDiscoveryService::new(
        NodeId("local".to_string()),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );
    let network = ServerP2pNetwork::new(discovery, StubTransport);
    let mut runtime = ServerP2pRuntime::new(network);

    let (tx, rx) = std::sync::mpsc::channel();
    tx.send(ServerP2pEvent::PeerDiscovered(node(
        "n1",
        "/ip4/10.0.0.1/tcp/4001",
    )))
    .expect("event send should succeed");
    tx.send(ServerP2pEvent::MessageReceived {
        from_peer_id: "n2".to_string(),
        message: ServiceMessage::NodeAnnounce(node("n2", "/ip4/10.0.0.2/tcp/4001")),
    })
    .expect("event send should succeed");
    tx.send(ServerP2pEvent::Shutdown)
        .expect("event send should succeed");

    runtime.run_loop(&rx).expect("runtime loop should succeed");

    let peers = runtime.network().discover_peers();
    assert_eq!(peers.len(), 2);
}

#[test]
fn runtime_can_run_from_swarm_source() {
    let discovery = KademliaDiscoveryService::new(
        NodeId("local".to_string()),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );
    let network = ServerP2pNetwork::new(discovery, StubTransport);
    let mut runtime = ServerP2pRuntime::new(network);

    let mut source = StubSwarmSource {
        events: vec![
            ServerP2pEvent::PeerDiscovered(node("n1", "/ip4/10.0.0.1/tcp/4001")),
            ServerP2pEvent::Shutdown,
        ],
    };

    runtime
        .run_swarm_loop(&mut source)
        .expect("swarm loop should succeed");

    assert_eq!(runtime.network().discover_peers().len(), 1);
}

#[test]
fn runtime_returns_error_for_error_event() {
    let discovery = KademliaDiscoveryService::new(
        NodeId("local".to_string()),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );
    let network = ServerP2pNetwork::new(discovery, StubTransport);
    let mut runtime = ServerP2pRuntime::new(network);

    let result = runtime.handle_event(ServerP2pEvent::ErrorReceived {
        from_peer_id: Some("node-err".to_string()),
        message: "decode failure".to_string(),
    });

    assert!(result.is_err());
}
