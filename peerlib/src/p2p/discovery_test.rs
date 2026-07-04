use super::*;

fn node(id: &str, addr: &str) -> PeerNode {
    PeerNode {
        id: id.to_string(),
        addrs: vec![addr.to_string()],
        is_local: false,
    }
}

#[test]
fn local_node_is_not_added_to_discovered_peers() {
    let local = "node-local".to_string();
    let mut discovery = KademliaDiscoveryService::new(
        local.clone(),
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );

    discovery.upsert_peer(PeerNode {
        id: local,
        addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
        is_local: true,
    });

    assert!(discovery.discover_peers().is_empty());
}

#[test]
fn bootstrap_nodes_are_retained_for_routing_only() {
    let config = KademliaDiscoveryConfig::new("/distdb/kad/1.0.0")
        .with_bootstrap_nodes(vec![node("node-a", "/ip4/10.0.0.1/tcp/4001")]);
    let discovery = KademliaDiscoveryService::new("node-local", config);

    let peers = discovery.discover_peers();
    assert!(peers.is_empty());
    assert_eq!(discovery.bootstrap_nodes().len(), 1);
    assert_eq!(discovery.bootstrap_nodes()[0].id, "node-a");
}

#[test]
fn remote_announced_peer_is_normalized_to_non_local() {
    let mut discovery = KademliaDiscoveryService::new(
        "node-local",
        KademliaDiscoveryConfig::new("/distdb/kad/1.0.0"),
    );

    discovery.upsert_peer(PeerNode {
        id: "node-remote".to_string(),
        addrs: vec!["/ip4/127.0.0.1/tcp/4102".to_string()],
        is_local: true,
    });

    let peers = discovery.discover_peers();
    assert_eq!(peers.len(), 1);
    assert_eq!(peers[0].id, "node-remote");
    assert!(!peers[0].is_local);
}
