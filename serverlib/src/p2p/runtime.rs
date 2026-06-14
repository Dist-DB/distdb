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
                log::info!(
                    "server p2p peer discovered peer_id={} addrs={}",
                    peer_id,
                    node.addrs.join(",")
                );
                self.network.upsert_discovered_peer(node);
                Ok(ServerP2pHandleOutcome::PeerDiscovered { peer_id })
            }
            ServerP2pEvent::MessageReceived {
                from_peer_id,
                message,
            } => {
                log::debug!("server p2p message received from_peer_id={}", from_peer_id);
                if let ServiceMessage::NodeAnnounce(node) = message {
                    log::info!(
                        "server p2p node announce received peer_id={} addrs={}",
                        node.id.0,
                        node.addrs.join(",")
                    );
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
                log::error!("server p2p runtime error from {}: {}", source, message);
                Err(ServerLibError::Network(format!(
                    "p2p event error from {source}: {message}"
                )))
            }
            ServerP2pEvent::Shutdown => {
                log::info!("server p2p runtime shutdown received");
                self.running = false;
                Ok(ServerP2pHandleOutcome::Shutdown)
            }
        }
    }
}


#[cfg(test)]
#[path = "runtime_test.rs"]
mod tests;
