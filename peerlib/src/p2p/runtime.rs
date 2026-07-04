use crate::error::{PeerError, Result};
use crate::p2p::network::ServerP2pNetwork;
use crate::p2p::protocol::{
    AffinityJoinRequest, AffinityJoinResponse, AffinityReplicationAction, ServiceMessage,
};
use crate::p2p::transport::Transport;
use crate::p2p::types::PeerNode;

use std::collections::VecDeque;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerP2pEvent {

    PeerDiscovered(PeerNode),
    
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

macro_rules! drain_pending_queue {
    ($name:ident, $field:ident, $ty:ty) => {
        pub fn $name(&mut self) -> Vec<(String, $ty)> {
            self.$field.drain(..).collect()
        }
    };
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

    drain_pending_queue!(
        pending_affinity_join_requests,
        pending_affinity_join_requests,
        AffinityJoinRequest
    );

    drain_pending_queue!(
        pending_affinity_join_responses,
        pending_affinity_join_responses,
        AffinityJoinResponse
    );
    
    drain_pending_queue!(
        pending_schema_catalog_requests,
        pending_schema_catalog_requests,
        super::protocol::SchemaCatalogRequest
    );
    
    drain_pending_queue!(
        pending_schema_catalog_responses,
        pending_schema_catalog_responses,
        super::protocol::SchemaCatalogResponse
    );
    
    drain_pending_queue!(
        pending_data_snapshot_requests,
        pending_data_snapshot_requests,
        super::protocol::DataSnapshotRequest
    );
    
    drain_pending_queue!(
        pending_data_snapshot_responses,
        pending_data_snapshot_responses,
        super::protocol::DataSnapshotResponse
    );
    
    drain_pending_queue!(
        pending_transactions_since_requests,
        pending_transactions_since_requests,
        super::protocol::TransactionsSinceRequest
    );

    drain_pending_queue!(
        pending_transactions_since_responses,
        pending_transactions_since_responses,
        super::protocol::TransactionsSinceResponse
    );
    
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
                let peer_id = node.id.clone();
                log::debug!(
                    "server p2p peer discovered peer_id={} addrs={}",
                    peer_id,
                    node.addrs.join(",")
                );
                self.network.upsert_discovered_peer(node);
                Ok(ServerP2pHandleOutcome::PeerDiscovered { peer_id })
            },

            ServerP2pEvent::MessageReceived {
                from_peer_id,
                message,
            } => {
                
                log::debug!("server p2p message received from_peer_id={}", from_peer_id);

                if let Some(action) = message.affinity_replication_action() {
                    log::debug!(
                        "server p2p affinity replication action={} from_peer_id={}",
                        action.as_str(),
                        from_peer_id
                    );
                }

                match message {

                    ServiceMessage::NodeAnnounce(node) => {
                        log::debug!(
                            "server p2p node announce received peer_id={} addrs={}",
                            node.id,
                            node.addrs.join(",")
                        );
                        self.network.upsert_discovered_peer(node);
                    },
                    
                    ServiceMessage::AffinityJoinRequest(req) => {
                        log::debug!(
                            "server p2p affinity join request received action={} from_peer_id={} affinity_id={}",
                            AffinityReplicationAction::JoinRequest.as_str(),
                            from_peer_id,
                            req.affinity_id
                        );
                        self.pending_affinity_join_requests
                            .push_back((from_peer_id.clone(), req));
                    },
                    
                    ServiceMessage::AffinityJoinResponse(resp) => {
                        log::debug!(
                            "server p2p affinity join response received action={} from_peer_id={} request_id={} ok={}",
                            AffinityReplicationAction::JoinResponse.as_str(),
                            from_peer_id,
                            resp.request_id,
                            resp.ok
                        );
                        self.pending_affinity_join_responses
                            .push_back((from_peer_id.clone(), resp));
                    },
                    
                    ServiceMessage::SchemaCatalogRequest(req) => {
                        log::debug!(
                            "server p2p schema catalog request received action={} from_peer_id={} database_id={}",
                            AffinityReplicationAction::SchemaCatalogRequest.as_str(),
                            from_peer_id,
                            req.database_id
                        );
                        self.pending_schema_catalog_requests
                            .push_back((from_peer_id.clone(), req));
                    },

                    ServiceMessage::SchemaCatalogResponse(resp) => {
                        log::debug!(
                            "server p2p schema catalog response received action={} from_peer_id={} request_id={}",
                            AffinityReplicationAction::SchemaCatalogResponse.as_str(),
                            from_peer_id,
                            resp.request_id
                        );
                        self.pending_schema_catalog_responses
                            .push_back((from_peer_id.clone(), resp));
                    },

                    ServiceMessage::DataSnapshotRequest(req) => {
                        log::debug!(
                            "server p2p data snapshot request received action={} from_peer_id={} database_id={}",
                            AffinityReplicationAction::DataSnapshotRequest.as_str(),
                            from_peer_id,
                            req.database_id
                        );
                        self.pending_data_snapshot_requests
                            .push_back((from_peer_id.clone(), req));
                    },

                    ServiceMessage::DataSnapshotResponse(resp) => {
                        log::debug!(
                            "server p2p data snapshot response received action={} from_peer_id={} request_id={}",
                            AffinityReplicationAction::DataSnapshotResponse.as_str(),
                            from_peer_id,
                            resp.request_id
                        );
                        self.pending_data_snapshot_responses
                            .push_back((from_peer_id.clone(), resp));
                    },

                    ServiceMessage::TransactionsSinceRequest(req) => {
                        log::debug!(
                            "server p2p transactions since request received action={} from_peer_id={} database_id={} from_tx={:?}",
                            AffinityReplicationAction::TransactionsSinceRequest.as_str(),
                            from_peer_id,
                            req.database_id,
                            req.from_transaction_id
                        );
                        self.pending_transactions_since_requests
                            .push_back((from_peer_id.clone(), req));
                    },

                    ServiceMessage::TransactionsSinceResponse(resp) => {
                        log::debug!(
                            "server p2p transactions since response received action={} from_peer_id={} request_id={} count={}",
                            AffinityReplicationAction::TransactionsSinceResponse.as_str(),
                            from_peer_id,
                            resp.request_id,
                            resp.transactions.len()
                        );
                        self.pending_transactions_since_responses
                            .push_back((from_peer_id.clone(), resp));
                    },

                    _ => {
                        log::debug!(
                            "server p2p message received but not handled: {:?}",
                            message
                        );
                    }

                }

                Ok(ServerP2pHandleOutcome::MessageReceived { from_peer_id })

            },

            ServerP2pEvent::ErrorReceived {
                from_peer_id,
                message,
            } => {

                let source = from_peer_id
                    .map(|peer| format!("peer={peer}"))
                    .unwrap_or_else(|| "peer=unknown".to_string());
                
                log::error!("server p2p runtime error from {}: {}", source, message);
                
                Err(PeerError::Network(format!(
                    "p2p event error from {source}: {message}"
                )))

            },

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
