use crate::core::cluster::NodeDescriptor;
use crate::helpers::error::{Result, ServerLibError};
use crate::p2p::network::ServerP2pNetwork;
use crate::p2p::protocol::ServiceMessage;
use crate::p2p::transport::Transport;

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerP2pEvent {
    PeerDiscovered(NodeDescriptor),
    MessageReceived {
        from_peer_id: String,
        message: ServiceMessage,
    },
    ErrorReceived {
        from_peer_id: Option<String>,
        message: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerP2pHandleOutcome {
    PeerDiscovered { peer_id: String },
    MessageReceived { from_peer_id: String },
    Shutdown,
}

pub trait ServerSwarmEventSource {
    fn next_event(&mut self, idle_wait: Duration) -> Option<ServerP2pEvent>;
}

#[derive(Debug)]
pub struct ServerP2pRuntime<T: Transport> {
    network: ServerP2pNetwork<T>,
    idle_wait: Duration,
    running: bool,
}

impl<T: Transport> ServerP2pRuntime<T> {
    pub fn new(network: ServerP2pNetwork<T>) -> Self {
        Self {
            network,
            idle_wait: Duration::from_millis(50),
            running: false,
        }
    }

    pub fn with_idle_wait(mut self, idle_wait: Duration) -> Self {
        self.idle_wait = idle_wait;
        self
    }

    pub fn is_running(&self) -> bool {
        self.running
    }

    pub fn network(&self) -> &ServerP2pNetwork<T> {
        &self.network
    }

    pub fn network_mut(&mut self) -> &mut ServerP2pNetwork<T> {
        &mut self.network
    }

    pub fn run_loop(&mut self, events: &Receiver<ServerP2pEvent>) -> Result<()> {
        self.running = true;

        while self.running {
            match events.recv_timeout(self.idle_wait) {
                Ok(event) => {
                    let _ = self.handle_event(event)?;
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }

        self.running = false;
        Ok(())
    }

    pub fn run_swarm_loop<S: ServerSwarmEventSource>(&mut self, source: &mut S) -> Result<()> {
        self.running = true;

        while self.running {
            match source.next_event(self.idle_wait) {
                Some(event) => {
                    let _ = self.handle_event(event)?;
                }
                None => break,
            }
        }

        self.running = false;
        Ok(())
    }

    pub fn handle_event(&mut self, event: ServerP2pEvent) -> Result<ServerP2pHandleOutcome> {
        match event {
            ServerP2pEvent::PeerDiscovered(node) => {
                let peer_id = node.id.0.clone();
                self.network.upsert_discovered_peer(node);
                Ok(ServerP2pHandleOutcome::PeerDiscovered { peer_id })
            }
            ServerP2pEvent::MessageReceived {
                from_peer_id,
                message,
            } => {
                if let ServiceMessage::NodeAnnounce(node) = message {
                    self.network.upsert_discovered_peer(node);
                }
                Ok(ServerP2pHandleOutcome::MessageReceived { from_peer_id })
            }
            ServerP2pEvent::ErrorReceived {
                from_peer_id,
                message,
            } => {
                let source = from_peer_id
                    .map(|peer| format!("peer={peer}"))
                    .unwrap_or_else(|| "peer=unknown".to_string());
                Err(ServerLibError::Network(format!(
                    "p2p event error from {source}: {message}"
                )))
            }
            ServerP2pEvent::Shutdown => {
                self.running = false;
                Ok(ServerP2pHandleOutcome::Shutdown)
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
        tx.send(ServerP2pEvent::PeerDiscovered(node("n1", "/ip4/10.0.0.1/tcp/4001")))
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
}
