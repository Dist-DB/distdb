use crate::core::cluster::NodeDescriptor;
use crate::helpers::error::{Result, ServerLibError};
use crate::p2p::network::ServerP2pNetwork;
use crate::p2p::protocol::{AffinityJoinRequest, AffinityJoinResponse, ServiceMessage};
use crate::p2p::transport::Transport;

use std::collections::VecDeque;
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
    pending_affinity_join_requests: VecDeque<(String, AffinityJoinRequest)>,
    pending_affinity_join_responses: VecDeque<(String, AffinityJoinResponse)>,
    pending_schema_catalog_requests: VecDeque<(String, super::protocol::SchemaCatalogRequest)>,
    pending_schema_catalog_responses: VecDeque<(String, super::protocol::SchemaCatalogResponse)>,
    pending_data_snapshot_requests: VecDeque<(String, super::protocol::DataSnapshotRequest)>,
    pending_data_snapshot_responses: VecDeque<(String, super::protocol::DataSnapshotResponse)>,
    pending_transactions_since_requests: VecDeque<(String, super::protocol::TransactionsSinceRequest)>,
    pending_transactions_since_responses: VecDeque<(String, super::protocol::TransactionsSinceResponse)>,
}

impl<T: Transport> ServerP2pRuntime<T> {
    pub fn new(network: ServerP2pNetwork<T>) -> Self {
        Self {
            network,
            idle_wait: Duration::from_millis(50),
            running: false,
            pending_affinity_join_requests: VecDeque::new(),
            pending_affinity_join_responses: VecDeque::new(),
            pending_schema_catalog_requests: VecDeque::new(),
            pending_schema_catalog_responses: VecDeque::new(),
            pending_data_snapshot_requests: VecDeque::new(),
            pending_data_snapshot_responses: VecDeque::new(),
            pending_transactions_since_requests: VecDeque::new(),
            pending_transactions_since_responses: VecDeque::new(),
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

    pub fn pending_affinity_join_requests(
        &mut self,
    ) -> Vec<(String, AffinityJoinRequest)> {
        self.pending_affinity_join_requests
            .drain(..)
            .collect()
    }

    pub fn pending_affinity_join_responses(
        &mut self,
    ) -> Vec<(String, AffinityJoinResponse)> {
        self.pending_affinity_join_responses
            .drain(..)
            .collect()
    }

    pub fn pending_schema_catalog_requests(
        &mut self,
    ) -> Vec<(String, super::protocol::SchemaCatalogRequest)> {
        self.pending_schema_catalog_requests
            .drain(..)
            .collect()
    }

    pub fn pending_schema_catalog_responses(
        &mut self,
    ) -> Vec<(String, super::protocol::SchemaCatalogResponse)> {
        self.pending_schema_catalog_responses
            .drain(..)
            .collect()
    }

    pub fn pending_data_snapshot_requests(
        &mut self,
    ) -> Vec<(String, super::protocol::DataSnapshotRequest)> {
        self.pending_data_snapshot_requests
            .drain(..)
            .collect()
    }

    pub fn pending_data_snapshot_responses(
        &mut self,
    ) -> Vec<(String, super::protocol::DataSnapshotResponse)> {
        self.pending_data_snapshot_responses
            .drain(..)
            .collect()
    }

    pub fn pending_transactions_since_requests(
        &mut self,
    ) -> Vec<(String, super::protocol::TransactionsSinceRequest)> {
        self.pending_transactions_since_requests
            .drain(..)
            .collect()
    }

    pub fn pending_transactions_since_responses(
        &mut self,
    ) -> Vec<(String, super::protocol::TransactionsSinceResponse)> {
        self.pending_transactions_since_responses
            .drain(..)
            .collect()
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
                match message {
                    ServiceMessage::NodeAnnounce(node) => {
                        log::info!(
                            "server p2p node announce received peer_id={} addrs={}",
                            node.id.0,
                            node.addrs.join(",")
                        );
                        self.network.upsert_discovered_peer(node);
                    }
                    ServiceMessage::AffinityJoinRequest(req) => {
                        log::info!(
                            "server p2p affinity join request received from_peer_id={} affinity_id={}",
                            from_peer_id,
                            req.affinity_id
                        );
                        self.pending_affinity_join_requests
                            .push_back((from_peer_id.clone(), req));
                    }
                    ServiceMessage::AffinityJoinResponse(resp) => {
                        log::info!(
                            "server p2p affinity join response received from_peer_id={} request_id={} ok={}",
                            from_peer_id,
                            resp.request_id,
                            resp.ok
                        );
                        self.pending_affinity_join_responses
                            .push_back((from_peer_id.clone(), resp));
                    }
                    ServiceMessage::SchemaCatalogRequest(req) => {
                        log::info!(
                            "server p2p schema catalog request received from_peer_id={} database_id={}",
                            from_peer_id,
                            req.database_id
                        );
                        self.pending_schema_catalog_requests
                            .push_back((from_peer_id.clone(), req));
                    }
                    ServiceMessage::SchemaCatalogResponse(resp) => {
                        log::info!(
                            "server p2p schema catalog response received from_peer_id={} request_id={}",
                            from_peer_id,
                            resp.request_id
                        );
                        self.pending_schema_catalog_responses
                            .push_back((from_peer_id.clone(), resp));
                    }
                    ServiceMessage::DataSnapshotRequest(req) => {
                        log::info!(
                            "server p2p data snapshot request received from_peer_id={} database_id={}",
                            from_peer_id,
                            req.database_id
                        );
                        self.pending_data_snapshot_requests
                            .push_back((from_peer_id.clone(), req));
                    }
                    ServiceMessage::DataSnapshotResponse(resp) => {
                        log::info!(
                            "server p2p data snapshot response received from_peer_id={} request_id={}",
                            from_peer_id,
                            resp.request_id
                        );
                        self.pending_data_snapshot_responses
                            .push_back((from_peer_id.clone(), resp));
                    }
                    ServiceMessage::TransactionsSinceRequest(req) => {
                        log::info!(
                            "server p2p transactions since request received from_peer_id={} database_id={} from_tx={:?}",
                            from_peer_id,
                            req.database_id,
                            req.from_transaction_id
                        );
                        self.pending_transactions_since_requests
                            .push_back((from_peer_id.clone(), req));
                    }
                    ServiceMessage::TransactionsSinceResponse(resp) => {
                        log::info!(
                            "server p2p transactions since response received from_peer_id={} request_id={} count={}",
                            from_peer_id,
                            resp.request_id,
                            resp.transactions.len()
                        );
                        self.pending_transactions_since_responses
                            .push_back((from_peer_id.clone(), resp));
                    }
                    _ => {
                        log::debug!(
                            "server p2p message received but not handled: {:?}",
                            message
                        );
                    }
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
