use crate::{ClientError, ClientOptions, TlsMode};
use common::DEFAULT_SERVER_PORT;
use std::net::Ipv4Addr;
use std::path::PathBuf;

pub(crate) const DEFAULT_DATABASE: &str = "main";

impl ClientOptions {

    pub fn from_cli_args(args: &[String]) -> Result<Self, ClientError> {

        let servers = bootstrap_peers_from_cli_args(args);
        
        if servers.is_empty() {
            return Err(ClientError::Config(
                "at least one server address is required".to_string(),
            ));
        }

        let tls_mode = match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
            Some(raw) => TlsMode::parse(raw).ok_or_else(|| {
                ClientError::Config(format!(
                    "invalid tls mode '{}'; expected off|optional|required",
                    raw
                ))
            })?,
            None => TlsMode::Optional,
        };

        let tls_ca_path = args
            .iter()
            .find_map(|arg| arg.strip_prefix("tls_ca="))
            .map(PathBuf::from);

        let user = args
            .iter()
            .find_map(|arg| arg.strip_prefix("user="))
            .map(ToOwned::to_owned);

        let password = args
            .iter()
            .find_map(|arg| arg.strip_prefix("password="))
            .map(ToOwned::to_owned);

        let database = args
            .iter()
            .find_map(|arg| arg.strip_prefix("database="))
            .map(ToOwned::to_owned);

        let peer_id = args
            .iter()
            .find_map(|arg| arg.strip_prefix("peer="))
            .map(ToOwned::to_owned);

        Ok(Self {
            servers,
            tls_mode,
            tls_ca_path,
            user,
            password,
            database,
            peer_id,
        })
    
    }

}

pub(crate) fn resolve_database_for_sql(
    current_database: Option<&str>,
    sql: &str,
) -> Result<String, ClientError> {

    if let Some(database) = current_database {
        return Ok(database.to_string());
    }

    if is_global_sql_without_database(sql) {
        return Ok(DEFAULT_DATABASE.to_string());
    }

    Err(ClientError::Config(
        "no active database selected; set database in options or call set_database()"
            .to_string(),
    ))
    
}

pub(crate) fn normalize_bootstrap_peers(peers: Vec<String>) -> Vec<String> {

    let mut normalized = Vec::new();

    for peer in peers {
        let Some(addr) = normalize_bootstrap_addr(&peer) else {
            continue;
        };

        if !normalized.contains(&addr) {
            normalized.push(addr);
        }
    }

    normalized

}

fn bootstrap_peers_from_cli_args(args: &[String]) -> Vec<String> {

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

fn normalize_bootstrap_addr(raw: &str) -> Option<String> {

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

fn is_global_sql_without_database(sql: &str) -> bool {

    let tokens = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if tokens.len() < 2 {
        return false;
    }

    matches!(
        (tokens[0].as_str(), tokens[1].as_str()),
        ("show", "databases")
            | ("show", "entities")
            | ("show", "server")
            | ("create", "database")
            | ("drop", "database")
    ) || (tokens[0] == "password" && tokens.len() == 2)
    
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
