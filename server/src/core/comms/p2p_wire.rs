use std::net::{Ipv4Addr, Ipv6Addr};

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

fn is_placeholder_host(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.is_empty() || trimmed == "--" || trimmed == "*" || trimmed == "null"
}

fn is_loopback_host(value: &str) -> bool {
    let trimmed = value.trim().to_ascii_lowercase();
    matches!(trimmed.as_str(), "127.0.0.1" | "localhost" | "localhost.localdomain" | "::1")
}

fn looks_like_public_hostname(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() || is_placeholder_host(trimmed) || is_loopback_host(trimmed) {
        return false;
    }

    if trimmed.parse::<Ipv4Addr>().is_ok() || trimmed.parse::<Ipv6Addr>().is_ok() {
        return true;
    }

    let lower = trimmed.to_ascii_lowercase();
    let mut label_count = 0usize;
    let mut has_private_label = false;
    for label in lower.split('.') {
        if label.is_empty() {
            continue;
        }

        label_count += 1;
        if matches!(label, "fritz" | "box" | "local" | "lan" | "home" | "host" | "desktop" | "mac") {
            has_private_label = true;
        }
    }

    if label_count < 2 {
        return false;
    }

    let local_suffixes = [
        ".local",
        ".localdomain",
        ".lan",
        ".home",
        ".home.arpa",
        ".internal",
        ".corp",
        ".private",
        ".intranet",
        ".test",
        ".invalid",
        ".example",
        ".localhost",
        ".onion",
    ];

    if local_suffixes.iter().any(|suffix| lower.ends_with(suffix)) {
        return false;
    }

    if has_private_label {
        return false;
    }

    !trimmed.starts_with('.') && !trimmed.ends_with('.')

}

fn extract_public_host_from_server_entry(entry: &str) -> Option<String> {

    let trimmed = entry.trim();
    if trimmed.is_empty() || is_placeholder_host(trimmed) {
        return None;
    }

    if trimmed.starts_with('/') {
        let mut parts = trimmed.trim_matches('/').split('/');
        let _proto = parts.next();
        if let Some(host) = parts.next() {
            let host = host.trim();
            if looks_like_public_hostname(host) {
                return Some(host.to_string());
            }
        }
        return None;
    }

    if let Some((host, _)) = trimmed.rsplit_once(':') {
        let host = host.trim();
        if looks_like_public_hostname(host) {
            return Some(host.to_string());
        }
    }

    if looks_like_public_hostname(trimmed) {
        return Some(trimmed.to_string());
    }

    None

}

fn resolve_public_host_from_server_list(args: &[String]) -> Option<String> {
    args.iter()
        .find_map(|arg| arg.strip_prefix("servers=").map(str::trim))
        .and_then(|server_list| server_list.split(',').find_map(extract_public_host_from_server_entry))

}

fn resolve_public_host_from_tls_sans(args: &[String]) -> Option<String> {
    args.iter()
        .find_map(|arg| arg.strip_prefix("tls_san=").map(str::trim))
        .and_then(|san_list| {
            san_list
                .split(',')
                .find_map(|san| {
                    if is_placeholder_host(san) {
                        None
                    } else {
                        extract_public_host_from_server_entry(san)
                    }
                })
        })
}

fn extract_public_hostname_from_hosts_content(content: &str) -> Option<String> {

    content
        .lines()
        .find_map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                return None;
            }

            let mut parts = trimmed.split_whitespace();
            let first = parts.next()?;
            let mut candidate = None;

            for part in parts {
                let cleaned = part.trim();
                if cleaned.is_empty() || cleaned.starts_with('#') {
                    continue;
                }

                if looks_like_public_hostname(cleaned) {
                    candidate = Some(cleaned.to_string());
                    break;
                }
            }

            if let Some(host) = candidate
                && (first.parse::<Ipv4Addr>().is_ok() || first.parse::<Ipv6Addr>().is_ok()) {
                    return Some(host);
                }

            None
        })

}

fn resolve_public_hostname_from_hosts_paths(paths: &[&str]) -> Option<String> {
    
    for path in paths {
        let Ok(content) = std::fs::read_to_string(path) else {
            continue;
        };
        if let Some(host) = extract_public_hostname_from_hosts_content(&content) {
            return Some(host);
        }
    }
    
    None

}

pub(crate) fn resolve_hostname_hint() -> Option<String> {

    std::env::var("DISTDB_ADVERTISE_HOST")
        .ok()
        .filter(|value| looks_like_public_hostname(value))
        .or_else(|| resolve_public_hostname_from_hosts_paths(&["/etc/hosts", "/etc/hosts.deny"]))
        .or_else(|| {
            std::env::var("HOSTNAME")
                .ok()
                .filter(|value| looks_like_public_hostname(value))
        })
        .or_else(|| {
            std::env::var("COMPUTERNAME")
                .ok()
                .filter(|value| looks_like_public_hostname(value))
        })
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|output| {
                    let host = String::from_utf8_lossy(&output.stdout).trim().to_string();
                    if looks_like_public_hostname(&host) {
                        Some(host)
                    } else {
                        None
                    }
                })
        })

}

pub fn resolve_advertise_host(args: &[String], listen_addr: &str, hostname_hint: Option<&str>) -> String {

    if let Some(explicit) = args
        .iter()
        .skip(1)
        .find_map(|arg| arg.strip_prefix("advertise_addr=").map(ToOwned::to_owned))
    {
        let explicit = explicit.trim().to_string();
        if !is_placeholder_host(&explicit) {
            return explicit;
        }
    }

    if let Some(positional_host) = args.iter().skip(1).find_map(|arg| {
        let trimmed = arg.trim();
        if trimmed.is_empty()
            || trimmed.starts_with("listen_addr=")
            || trimmed.starts_with("advertise_addr=")
            || trimmed.starts_with("port=")
            || trimmed.starts_with("node_id=")
            || trimmed.starts_with("datadir=")
            || trimmed.starts_with("swarm_id=")
            || trimmed.starts_with("servers=")
            || trimmed.starts_with("affinity=")
            || trimmed.starts_with("tls_san=")
            || trimmed.starts_with("ca_root")
            || trimmed.starts_with("service=")
            || trimmed.starts_with("wss")
        {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
        && !is_placeholder_host(&positional_host) {
            return positional_host;
        }

    if let Some(explicit_host) = std::env::var("DISTDB_ADVERTISE_HOST")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        let explicit_host = explicit_host.trim().to_string();
        if !is_placeholder_host(&explicit_host) {
            return explicit_host;
        }
    }

    if let Some(tls_san_host) = resolve_public_host_from_tls_sans(args) {
        return tls_san_host;
    }

    if let Some(hostname_hint) = hostname_hint
        && looks_like_public_hostname(hostname_hint)
    {
        return hostname_hint.to_string();
    }

    if let Some(server_host) = resolve_public_host_from_server_list(args) {
        return server_host;
    }

    if listen_addr == "0.0.0.0" || listen_addr == "::" || listen_addr.is_empty() {
        return "127.0.0.1".to_string();
    }

    listen_addr.to_string()

}

pub fn advertised_listen_addr_from_args(args: &[String], listen_addr: &str) -> String {
    resolve_advertise_host(args, listen_addr, resolve_hostname_hint().as_deref())
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

fn extract_port_from_multiaddr(addr: &str) -> Option<u16> {

    let trimmed = addr.trim();
    let port_token = trimmed.rsplit_once("/tcp/").and_then(|(_, port)| port.parse::<u16>().ok());
    
    if port_token.is_some() {
        return port_token;
    }

    trimmed
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())

}

pub fn prefer_public_hostname_in_addrs(addrs: &[String], hostname_hint: Option<&str>) -> Vec<String> {

    let Some(hostname_hint) = hostname_hint.filter(|value| looks_like_public_hostname(value)) else {
        return addrs.to_vec();
    };

    addrs
        .iter()
        .map(|addr| {
            if let Some(port) = extract_port_from_multiaddr(addr) {
                normalize_advertise_addr(hostname_hint, port)
            } else {
                addr.clone()
            }
        })
        .collect()

}

pub fn normalize_advertise_addr(addr: &str, port: u16) -> String {

    let trimmed = addr.trim();
    if trimmed.is_empty() || is_placeholder_host(trimmed) {
        return format!("/ip4/127.0.0.1/tcp/{port}");
    }

    if trimmed.starts_with('/') {
        return trimmed.to_string();
    }

    if let Ok(ip) = trimmed.parse::<Ipv4Addr>() {
        return format!("/ip4/{ip}/tcp/{port}");
    }

    if let Some((host, maybe_port)) = trimmed.rsplit_once(':') {
        let maybe_port = maybe_port.trim();
        if maybe_port.parse::<u16>().is_ok() {
            let host = host.trim();
            if host.is_empty() {
                return format!("/dns/127.0.0.1/tcp/{port}");
            }
            if host.parse::<Ipv4Addr>().is_ok() {
                return format!("/ip4/{host}/tcp/{port}");
            }
            return format!("/dns/{host}/tcp/{port}");
        }
    }

    if trimmed.contains("/") {
        return trimmed.to_string();
    }

    format!("/dns/{trimmed}/tcp/{port}")

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

    let mut parts = addr.trim_matches('/').split('/');
    let p0 = parts.next()?;
    let host = parts.next()?;
    let p2 = parts.next()?;
    let p3 = parts.next()?;

    match (p0, p2) {
        ("ip4", "tcp") | ("dns", "tcp") => {
            let port = p3.parse::<u16>().ok()?;
            Some(format!("{host}:{port}"))
        }
        _ => None,
    }
    
}

pub fn encode_service_message(message: &ServiceMessage) -> Option<Vec<u8>> {
    let encoded = bincode::serialize(message).ok()?;
    let mut payload = Vec::with_capacity(SERVICE_MESSAGE_MAGIC.len() + encoded.len());
    payload.extend_from_slice(SERVICE_MESSAGE_MAGIC);
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
#[path = "p2p_wire_test.rs"]
mod tests;
