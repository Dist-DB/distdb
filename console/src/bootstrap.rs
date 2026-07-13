use peerlib::ConnectorTlsConfig;
use common::DEFAULT_SERVER_PORT;
use std::{collections::HashSet, net::Ipv4Addr};

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
        None => (trimmed, DEFAULT_SERVER_PORT),
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

pub fn normalize_bootstrap_peers<I>(peers: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    let mut seen = HashSet::new();
    let mut normalized = Vec::new();

    for peer in peers {

        let Some(peer) = normalize_bootstrap_addr(&peer) else {
            continue;
        };

        if seen.insert(peer.clone()) {
            normalized.push(peer);
        }

    }

    normalized
}

pub fn bootstrap_peers_from_cli_args(args: &[String]) -> Vec<String> {

    let listed = args
        .iter()
        .find_map(|arg| arg.strip_prefix("servers=").map(ToOwned::to_owned))
        .map(|list| {
            list.split(',')
                .map(|addr| addr.trim().to_string())
                .collect::<Vec<String>>()
        })
        .unwrap_or_default();

    let mut candidates = Vec::new();

    if let Some(primary_server) = args.iter().find(|arg| !arg.contains('=')) {
        let primary_server = primary_server.trim().to_string();
        if !primary_server.is_empty() {
            candidates.push(primary_server);
        }
    }

    candidates.extend(listed);

    normalize_bootstrap_peers(candidates)

}

pub fn connector_tls_config_from_cli_args(
    args: &[String],
) -> Result<ConnectorTlsConfig, String> {

    let mode = match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
        Some(raw) => common::TlsMode::parse(raw).ok_or_else(|| {
            format!("invalid tls mode '{}'; expected off|optional|required", raw)
        })?,
        None => common::TlsMode::Required,
    };

    let ca_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_ca="))
        .map(std::path::PathBuf::from);

    Ok(ConnectorTlsConfig { mode, ca_path })

}

#[cfg(test)]
#[path = "bootstrap_test.rs"]
mod tests;
