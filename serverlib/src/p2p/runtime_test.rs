use super::*;
use crate::core::identity::NodeId;
use crate::p2p::discovery::{KademliaDiscoveryConfig, KademliaDiscoveryService};
use crate::p2p::protocol::{
    DataSnapshotRequest, DataSnapshotResponse, SchemaCatalogRequest, SchemaCatalogResponse,
    TransactionsSinceRequest, TransactionsSinceResponse,
};

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

#[test]
fn runtime_queues_schema_catalog_messages() {
    let discovery = KademliaDiscoveryService::new(
        NodeId("local".to_string()),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );
    let network = ServerP2pNetwork::new(discovery, StubTransport);
    let mut runtime = ServerP2pRuntime::new(network);

    let request = SchemaCatalogRequest {
        request_id: "req-schema-1".to_string(),
        affinity_id: "aff-1".to_string(),
        database_id: "db1".to_string(),
    };
    let response = SchemaCatalogResponse {
        request_id: "req-schema-1".to_string(),
        ok: true,
        error: None,
        schema_definitions: vec!["CREATE TABLE users (id INT);".to_string()],
    };

    runtime
        .handle_event(ServerP2pEvent::MessageReceived {
            from_peer_id: "peer-a".to_string(),
            message: ServiceMessage::SchemaCatalogRequest(request.clone()),
        })
        .expect("schema request event should be handled");
    runtime
        .handle_event(ServerP2pEvent::MessageReceived {
            from_peer_id: "peer-a".to_string(),
            message: ServiceMessage::SchemaCatalogResponse(response.clone()),
        })
        .expect("schema response event should be handled");

    let queued_requests = runtime.pending_schema_catalog_requests();
    let queued_responses = runtime.pending_schema_catalog_responses();

    assert_eq!(queued_requests, vec![("peer-a".to_string(), request)]);
    assert_eq!(queued_responses, vec![("peer-a".to_string(), response)]);
}

#[test]
fn runtime_queues_data_snapshot_messages() {
    let discovery = KademliaDiscoveryService::new(
        NodeId("local".to_string()),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );
    let network = ServerP2pNetwork::new(discovery, StubTransport);
    let mut runtime = ServerP2pRuntime::new(network);

    let request = DataSnapshotRequest {
        request_id: "req-snap-1".to_string(),
        affinity_id: "aff-1".to_string(),
        database_id: "db1".to_string(),
        table_names: vec!["users".to_string()],
    };
    let response = DataSnapshotResponse {
        request_id: "req-snap-1".to_string(),
        ok: true,
        error: None,
        snapshot_data: vec![(
            "users".to_string(),
            vec!["INSERT INTO users VALUES (1);".to_string()],
        )],
    };

    runtime
        .handle_event(ServerP2pEvent::MessageReceived {
            from_peer_id: "peer-b".to_string(),
            message: ServiceMessage::DataSnapshotRequest(request.clone()),
        })
        .expect("data snapshot request event should be handled");
    runtime
        .handle_event(ServerP2pEvent::MessageReceived {
            from_peer_id: "peer-b".to_string(),
            message: ServiceMessage::DataSnapshotResponse(response.clone()),
        })
        .expect("data snapshot response event should be handled");

    let queued_requests = runtime.pending_data_snapshot_requests();
    let queued_responses = runtime.pending_data_snapshot_responses();

    assert_eq!(queued_requests, vec![("peer-b".to_string(), request)]);
    assert_eq!(queued_responses, vec![("peer-b".to_string(), response)]);
}

#[test]
fn runtime_queues_transactions_since_messages() {
    let discovery = KademliaDiscoveryService::new(
        NodeId("local".to_string()),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );
    let network = ServerP2pNetwork::new(discovery, StubTransport);
    let mut runtime = ServerP2pRuntime::new(network);

    let request = TransactionsSinceRequest {
        request_id: "req-wal-1".to_string(),
        affinity_id: "aff-1".to_string(),
        database_id: "db1".to_string(),
        from_transaction_id: None,
            from_stream_transaction_ids: Vec::new(),
    };
    let response = TransactionsSinceResponse {
        request_id: "req-wal-1".to_string(),
        ok: true,
        error: None,
        transactions: vec!["UPDATE users SET active = 1 WHERE id = 1;".to_string()],
    };

    runtime
        .handle_event(ServerP2pEvent::MessageReceived {
            from_peer_id: "peer-c".to_string(),
            message: ServiceMessage::TransactionsSinceRequest(request.clone()),
        })
        .expect("transactions-since request event should be handled");
    runtime
        .handle_event(ServerP2pEvent::MessageReceived {
            from_peer_id: "peer-c".to_string(),
            message: ServiceMessage::TransactionsSinceResponse(response.clone()),
        })
        .expect("transactions-since response event should be handled");

    let queued_requests = runtime.pending_transactions_since_requests();
    let queued_responses = runtime.pending_transactions_since_responses();

    assert_eq!(queued_requests, vec![("peer-c".to_string(), request)]);
    assert_eq!(queued_responses, vec![("peer-c".to_string(), response)]);
}
