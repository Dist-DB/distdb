use std::net::Ipv4Addr;

use common::helpers::stable_id;
use peerlib::{
    PeerNode, ServiceMessage, WireAffinityDocument, WireAffinityMember,
    WireAffinityMemberStatus, WireDatabaseSchemaSummary,
    WireReplicationSecuritySummary, WireTransactionId,
};
use serverlib::core::cluster::NodeDescriptor;
use serverlib::core::identity::NodeId;
use serverlib::{
    AffinityDocument, AffinityMember, AffinityMemberStatus,
    DatabaseSchemaSummary, ReplicationSecuritySummary, TransactionId,
};

const SERVICE_MESSAGE_MAGIC: &[u8; 4] = b"SDSP";

pub fn node_descriptor_to_peer_node(node: &NodeDescriptor) -> PeerNode {
    PeerNode {
        id: node.id.0.clone(),
        addrs: node.addrs.clone(),
        is_local: node.is_local,
    }
}

pub fn peer_node_to_node_descriptor(node: &PeerNode) -> NodeDescriptor {
    NodeDescriptor {
        id: NodeId(node.id.clone()),
        addrs: node.addrs.clone(),
        is_local: node.is_local,
    }
}

pub fn transaction_id_to_wire(tx_id: TransactionId) -> WireTransactionId {
    WireTransactionId(tx_id.0)
}

pub fn wire_transaction_id_to_transaction_id(tx_id: WireTransactionId) -> TransactionId {
    TransactionId(tx_id.0)
}

fn affinity_member_status_to_wire(status: AffinityMemberStatus) -> WireAffinityMemberStatus {
    match status {
        AffinityMemberStatus::Online => WireAffinityMemberStatus::Online,
        AffinityMemberStatus::Offline => WireAffinityMemberStatus::Offline,
        AffinityMemberStatus::Unknown => WireAffinityMemberStatus::Unknown,
    }
}

fn wire_affinity_member_status_to_domain(status: WireAffinityMemberStatus) -> AffinityMemberStatus {
    match status {
        WireAffinityMemberStatus::Online => AffinityMemberStatus::Online,
        WireAffinityMemberStatus::Offline => AffinityMemberStatus::Offline,
        WireAffinityMemberStatus::Unknown => AffinityMemberStatus::Unknown,
    }
}

pub fn affinity_document_to_wire(document: &AffinityDocument) -> WireAffinityDocument {
    WireAffinityDocument {
        affinity_id: document.affinity_id.clone(),
        affinity_revision: document.affinity_revision,
        members: document
            .members
            .iter()
            .map(|member| WireAffinityMember {
                node_id: member.node_id.0.clone(),
                addrs: member.addrs.clone(),
                status: affinity_member_status_to_wire(member.status),
                last_seen_epoch_ms: member.last_seen_epoch_ms,
            })
            .collect(),
        databases: document
            .databases
            .iter()
            .map(|db| WireDatabaseSchemaSummary {
                database_id: db.database_id.clone(),
                database_name: db.database_name.clone(),
                schema_identifier: db.schema_identifier,
                schema_hash: db.schema_hash.clone(),
            })
            .collect(),
        replication_security: WireReplicationSecuritySummary {
            policy_revision: document.replication_security.policy_revision,
            key_id: document.replication_security.key_id.clone(),
            updated_epoch_ms: document.replication_security.updated_epoch_ms,
        },
    }
}

pub fn wire_affinity_document_to_domain(document: &WireAffinityDocument) -> AffinityDocument {
    AffinityDocument {
        affinity_id: document.affinity_id.clone(),
        affinity_revision: document.affinity_revision,
        members: document
            .members
            .iter()
            .map(|member| AffinityMember {
                node_id: NodeId(member.node_id.clone()),
                addrs: member.addrs.clone(),
                status: wire_affinity_member_status_to_domain(member.status),
                last_seen_epoch_ms: member.last_seen_epoch_ms,
            })
            .collect(),
        databases: document
            .databases
            .iter()
            .map(|db| DatabaseSchemaSummary {
                database_id: db.database_id.clone(),
                database_name: db.database_name.clone(),
                schema_identifier: db.schema_identifier,
                schema_hash: db.schema_hash.clone(),
            })
            .collect(),
        replication_security: ReplicationSecuritySummary {
            policy_revision: document.replication_security.policy_revision,
            key_id: document.replication_security.key_id.clone(),
            updated_epoch_ms: document.replication_security.updated_epoch_ms,
        },
    }
}

pub fn advertised_listen_addr_from_args(args: &[String], listen_addr: &str) -> String {

    if let Some(explicit) = args
        .iter()
        .find_map(|arg| arg.strip_prefix("advertise_addr=").map(ToOwned::to_owned))
    {
        let explicit = explicit.trim().to_string();
        if !explicit.is_empty() {
            return explicit;
        }
    }

    if listen_addr == "0.0.0.0" {
        return "127.0.0.1".to_string();
    }

    listen_addr.to_string()
}

pub fn normalize_bootstrap_addr(raw: &str) -> Option<String> {

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.starts_with('/') {
        return Some(trimmed.to_string());
    }

    if let Ok(port) = trimmed.parse::<u16>() {
        return Some(format!("/ip4/127.0.0.1/tcp/{port}"));
    }

    if let Some(port_str) = trimmed.strip_prefix(':') {
        let port = port_str.parse::<u16>().ok()?;
        return Some(format!("/ip4/127.0.0.1/tcp/{port}"));
    }

    let (host, port) = match trimmed.rsplit_once(':') {
        Some((host, port_str)) => {
            let parsed_port = port_str.parse::<u16>().ok()?;
            (host.trim(), parsed_port)
        }
        None => (trimmed, common::DEFAULT_SERVER_PORT),
    };

    if host.is_empty() {
        return None;
    }

    let host_prefix = if host.parse::<Ipv4Addr>().is_ok() {
        "ip4"
    } else {
        "dns"
    };

    Some(format!("/{host_prefix}/{host}/tcp/{port}"))

}

pub fn bootstrap_nodes_from_server_list(server_list: &[String]) -> Vec<PeerNode> {

    server_list
        .iter()
        .map(|addr| PeerNode {
            id: format!("bootstrap-{}", stable_id(&[addr])),
            addrs: vec![addr.clone()],
            is_local: false,
        })
        .collect()

}

pub fn multiaddr_to_socket_addr(addr: &str) -> Option<String> {

    let parts = addr.trim_matches('/').split('/').collect::<Vec<_>>();
    if parts.len() < 4 {
        return None;
    }

    match (parts[0], parts[2]) {
        ("ip4", "tcp") | ("dns", "tcp") => {
            let host = parts[1];
            let port = parts[3].parse::<u16>().ok()?;
            Some(format!("{}:{}", host, port))
        }
        _ => None,
    }
    
}

pub fn encode_service_message(message: &ServiceMessage) -> Option<Vec<u8>> {
    let mut payload = SERVICE_MESSAGE_MAGIC.to_vec();
    let encoded = bincode::serialize(message).ok()?;
    payload.extend_from_slice(&encoded);
    Some(payload)
}

pub fn decode_service_message(payload: &[u8]) -> Option<ServiceMessage> {
    if payload.len() < SERVICE_MESSAGE_MAGIC.len() {
        return None;
    }

    if &payload[..SERVICE_MESSAGE_MAGIC.len()] != SERVICE_MESSAGE_MAGIC {
        return None;
    }

    bincode::deserialize(&payload[SERVICE_MESSAGE_MAGIC.len()..]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_bootstrap_addr_accepts_multiaddr_passthrough() {
        let addr = "/ip4/127.0.0.1/tcp/9400";
        assert_eq!(normalize_bootstrap_addr(addr), Some(addr.to_string()));
    }

    #[test]
    fn normalize_bootstrap_addr_parses_host_port() {
        assert_eq!(
            normalize_bootstrap_addr("127.0.0.1:9400"),
            Some("/ip4/127.0.0.1/tcp/9400".to_string())
        );
        assert_eq!(
            normalize_bootstrap_addr("node.local:9400"),
            Some("/dns/node.local/tcp/9400".to_string())
        );
    }

    #[test]
    fn multiaddr_to_socket_addr_parses_ip4_and_dns() {
        assert_eq!(
            multiaddr_to_socket_addr("/ip4/127.0.0.1/tcp/4001"),
            Some("127.0.0.1:4001".to_string())
        );
        assert_eq!(
            multiaddr_to_socket_addr("/dns/node.local/tcp/4002"),
            Some("node.local:4002".to_string())
        );
        assert_eq!(multiaddr_to_socket_addr("127.0.0.1:4001"), None);
    }

    #[test]
    fn node_announce_wire_encoding_roundtrips() {
        let message = ServiceMessage::NodeAnnounce(peerlib::PeerNode {
            id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        });

        let encoded = encode_service_message(&message).expect("message should encode");
        let decoded = decode_service_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn schema_catalog_wire_encoding_roundtrips() {
        let message = ServiceMessage::SchemaCatalogRequest(
            peerlib::SchemaCatalogRequest {
                request_id: "req-1".to_string(),
                affinity_id: "aff-1".to_string(),
                database_id: "main".to_string(),
                expected_schema_identifier: 1,
                expected_schema_hash: Some("hash".to_string()),
            },
        );

        let encoded = encode_service_message(&message).expect("message should encode");
        let decoded = decode_service_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn normalize_bootstrap_addr_parses_bare_port() {
        assert_eq!(
            normalize_bootstrap_addr("4001"),
            Some("/ip4/127.0.0.1/tcp/4001".to_string())
        );
        assert_eq!(
            normalize_bootstrap_addr(":4002"),
            Some("/ip4/127.0.0.1/tcp/4002".to_string())
        );
    }

    #[test]
    fn advertised_listen_addr_defaults_wildcard_to_localhost() {
        let args = vec!["server".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "127.0.0.1".to_string()
        );
        assert_eq!(
            advertised_listen_addr_from_args(&args, "192.168.1.10"),
            "192.168.1.10".to_string()
        );
    }

    #[test]
    fn advertised_listen_addr_prefers_explicit_override() {
        let args = vec!["server".to_string(), "advertise_addr=10.1.1.5".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "10.1.1.5".to_string()
        );
    }

    #[test]
    fn bootstrap_nodes_use_normalized_addrs() {
        let nodes = bootstrap_nodes_from_server_list(&[
            "/ip4/127.0.0.1/tcp/9400".to_string(),
            "/dns/node.local/tcp/9400".to_string(),
        ]);

        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].addrs, vec!["/ip4/127.0.0.1/tcp/9400".to_string()]);
        assert_eq!(nodes[1].addrs, vec!["/dns/node.local/tcp/9400".to_string()]);
        assert!(nodes.iter().all(|node| !node.id.is_empty()));
    }
}
