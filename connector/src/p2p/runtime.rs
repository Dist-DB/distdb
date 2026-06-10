use crate::core::{ConnectorError, ConnectorResponse};
use crate::p2p::{ConnectorP2pTransport, ConnectorPeer};

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorP2pEvent {
    PeerDiscovered(ConnectorPeer),
    ResponseReceived(ConnectorResponse),
    ErrorReceived {
        request_id: Option<String>,
        message: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectorP2pHandleOutcome {
    PeerDiscovered { peer_id: String },
    ResponseReceived(ConnectorResponse),
    Shutdown,
}

pub trait ConnectorSwarmEventSource {
    fn next_event(&mut self, idle_wait: Duration) -> Option<ConnectorP2pEvent>;
}

#[derive(Debug, Clone)]
pub struct ConnectorP2pRuntime {
    transport: ConnectorP2pTransport,
    idle_wait: Duration,
    running: bool,
}

impl ConnectorP2pRuntime {
    
    pub fn new(transport: ConnectorP2pTransport) -> Self {
        Self {
            transport,
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

    pub fn transport(&self) -> &ConnectorP2pTransport {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut ConnectorP2pTransport {
        &mut self.transport
    }

    pub fn into_transport(self) -> ConnectorP2pTransport {
        self.transport
    }

    pub fn run_loop(&mut self, events: &Receiver<ConnectorP2pEvent>) -> Result<(), ConnectorError> {
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

    pub fn run_swarm_loop<S: ConnectorSwarmEventSource>(&mut self, source: &mut S) -> Result<(), ConnectorError> {
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

    pub fn handle_event(&mut self, event: ConnectorP2pEvent) -> Result<ConnectorP2pHandleOutcome, ConnectorError> {
        match event {
            ConnectorP2pEvent::PeerDiscovered(peer) => {
                let peer_id = peer.peer_id.clone();
                log::info!(
                    "connector p2p peer discovered peer_id={} addrs={}",
                    peer_id,
                    peer.addrs.join(",")
                );
                self.transport.upsert_peer(peer);
                Ok(ConnectorP2pHandleOutcome::PeerDiscovered { peer_id })
            }
            ConnectorP2pEvent::ResponseReceived(response) => {
                log::debug!(
                    "connector p2p response received request_id={} status={:?}",
                    response.request_id,
                    response.status
                );
                self.transport.queue_response(response.clone());
                Ok(ConnectorP2pHandleOutcome::ResponseReceived(response))
            }
            ConnectorP2pEvent::ErrorReceived {
                request_id,
                message,
            } => {
                let prefix = request_id
                    .map(|id| format!("request_id={id}: "))
                    .unwrap_or_default();
                log::error!("connector p2p error: {}{}", prefix, message);
                Err(ConnectorError::Transport(format!("{prefix}{message}")))
            }
            ConnectorP2pEvent::Shutdown => {
                log::info!("connector p2p runtime shutdown received");
                self.running = false;
                Ok(ConnectorP2pHandleOutcome::Shutdown)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::{
        ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult,
        MutationResult, ResponseStatus,
    };
    use crate::core::ConnectorTransport;
    use crate::p2p::ConnectorP2pConfig;

    #[derive(Debug)]
    struct StubSwarmSource {
        events: Vec<ConnectorP2pEvent>,
    }

    impl ConnectorSwarmEventSource for StubSwarmSource {
        fn next_event(&mut self, _idle_wait: Duration) -> Option<ConnectorP2pEvent> {
            if self.events.is_empty() {
                None
            } else {
                Some(self.events.remove(0))
            }
        }
    }

    #[test]
    fn runtime_processes_peer_and_response_events() {
        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap".to_string()]),
        );
        let mut runtime = ConnectorP2pRuntime::new(transport);

        let (tx, rx) = std::sync::mpsc::channel();

        tx.send(ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
        }))
        .expect("event send should succeed");

        tx.send(ConnectorP2pEvent::ResponseReceived(ConnectorResponse {
            request_id: "req-7".to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
        }))
        .expect("event send should succeed");

        tx.send(ConnectorP2pEvent::Shutdown)
            .expect("event send should succeed");

        runtime.run_loop(&rx).expect("runtime loop should succeed");

        let request = ConnectorRequest::new(
            "req-7",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let response = runtime
            .transport()
            .request(&request)
            .expect("queued response should be available");

        assert_eq!(response.request_id, "req-7");
        assert_eq!(runtime.transport().discovered_peers().len(), 1);
    }

    #[test]
    fn runtime_can_run_from_swarm_source() {
        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap".to_string()]),
        );
        let mut runtime = ConnectorP2pRuntime::new(transport);

        let mut source = StubSwarmSource {
            events: vec![
                ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
                    peer_id: "peer-2".to_string(),
                    addrs: vec!["/ip4/10.0.0.2/tcp/4001".to_string()],
                }),
                ConnectorP2pEvent::Shutdown,
            ],
        };

        runtime
            .run_swarm_loop(&mut source)
            .expect("swarm loop should succeed");
        assert_eq!(runtime.transport().discovered_peers().len(), 1);
    }

    #[test]
    fn runtime_returns_error_for_error_event() {
        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap".to_string()]),
        );
        let mut runtime = ConnectorP2pRuntime::new(transport);

        let result = runtime.handle_event(ConnectorP2pEvent::ErrorReceived {
            request_id: Some("req-err".to_string()),
            message: "response decode failed".to_string(),
        });

        assert!(matches!(result, Err(ConnectorError::Transport(_))));
    }
}
