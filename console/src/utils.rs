use crate::ConsoleCommand;
use connector::{ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, ResponseStatus};
use peerlib::ConnectorTlsConfig;

pub(crate) const AUTH_FALLBACK_DATABASE: &str = "main";
const SHOW_PEERS_REQUEST_TIMEOUT_SECS_DEFAULT: u64 = 1;
const SHOW_PEERS_REQUEST_TIMEOUT_SECS_ENV: &str = "DISTDB_CONSOLE_SHOW_PEERS_TIMEOUT_SECS";

pub(crate) fn show_peers_request_timeout_secs() -> u64 {
    std::env::var(SHOW_PEERS_REQUEST_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.clamp(1, 30))
        .unwrap_or(SHOW_PEERS_REQUEST_TIMEOUT_SECS_DEFAULT)
}

pub(crate) fn validate_use_database_probe_response(
    database: &str,
    response: &ConnectorResponse,
) -> Result<(), String> {
    if response.status != ResponseStatus::Rejected {
        return Ok(());
    }

    match &response.result {
        ConnectorResult::Error(message) => Err(format!(
            "database switch to '{}' rejected: {}",
            database,
            message,
        )),

        _ => Err(format!(
            "database switch to '{}' rejected",
            database,
        )),
    }
}

pub(crate) trait ConsoleRequestExt {
    fn query_database_id(&self) -> &str;
}

impl ConsoleRequestExt for ConnectorRequest {
    fn query_database_id(&self) -> &str {
        match &self.command {
            ConnectorCommand::Query { query } => &query.database_id,
            _ => "<n/a>",
        }
    }
}

pub fn normalize_bootstrap_addr(raw: &str) -> Option<String> {
    crate::bootstrap::normalize_bootstrap_addr(raw)
}

pub fn normalize_bootstrap_peers<I>(peers: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    crate::bootstrap::normalize_bootstrap_peers(peers)
}

pub fn bootstrap_peers_from_cli_args(args: &[String]) -> Vec<String> {
    crate::bootstrap::bootstrap_peers_from_cli_args(args)
}

pub fn connector_tls_config_from_cli_args(
    args: &[String],
) -> Result<ConnectorTlsConfig, String> {
    crate::bootstrap::connector_tls_config_from_cli_args(args)
}

pub fn extract_password_token_input(sql: &str) -> Option<&str> {
    crate::commands::extract_password_token_input(sql)
}

pub fn auth_password_input(sql: &str) -> Option<&str> {
    crate::commands::auth_password_input(sql)
}

pub fn resolve_database_for_sql(
    current_database: Option<&str>,
    is_auth_request: bool,
    sql: &str,
) -> Result<String, &'static str> {
    crate::commands::resolve_database_for_sql(
        current_database,
        is_auth_request,
        sql,
        AUTH_FALLBACK_DATABASE,
    )
}

pub fn parse_console_command(input: &str) -> Result<Option<ConsoleCommand>, String> {
    crate::commands::parse_console_command(input, crate::TEMP_CONNECT_USER)
}

pub fn parse_console_command_with_delimiter(
    input: &str,
    delimiter: &str,
) -> Result<Option<ConsoleCommand>, String> {
    crate::commands::parse_console_command_with_delimiter(input, crate::TEMP_CONNECT_USER, delimiter)
}

pub fn print_help() {
    crate::commands::print_help();
}

pub fn parse_connect_target(target: &str) -> Result<(String, String), String> {
    crate::commands::parse_connect_target(target, crate::TEMP_CONNECT_USER)
}

#[cfg(test)]
#[path = "utils_test.rs"]
mod tests;