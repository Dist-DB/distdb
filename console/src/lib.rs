
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer, ConnectorRequest,
    ConnectorResponse, ConnectorResult, DataQuery, ResponseStatus,
};
use common::DEFAULT_SERVER_PORT;
use common::helpers::utils::md5_hash;
use std::{collections::HashSet, net::Ipv4Addr};

pub const TEMP_CONNECT_USER: &str = "root";
const AUTH_FALLBACK_DATABASE: &str = "main";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";

pub enum ConsoleCommand {
    Help,
    Exit,
    ShowP2p,
    ShowLog,
    ShowPeers,
    ConnectPeer { user: String, peer_id: String },
    Disconnect,
    UseDatabase(String),
    Sql(String),
}

struct ConsoleLogEntry {
    seqno: u64,
    message: String,
}

pub struct ConsoleSession {
    pub runtime: ConnectorP2pRuntime,
    pub current_database: Option<String>,
    request_seq: u64,
    log_seq: u64,
    log_entries: Vec<ConsoleLogEntry>,
}

impl ConsoleSession {

    pub fn new(server_list: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let bootstrap_peers = normalize_bootstrap_peers(server_list);

        if bootstrap_peers.is_empty() {
            return Err("at least one server address is required".into());
        }

        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(bootstrap_peers.clone()),
        );

        let mut runtime = ConnectorP2pRuntime::new(transport);

        for (idx, server_address) in bootstrap_peers.into_iter().enumerate() {
            runtime.handle_event(ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
                peer_id: format!("server-node-{:02}", idx + 1),
                addrs: vec![server_address],
            }))?;
        }

        Ok(Self {
            runtime,
            current_database: None,
            request_seq: 0,
            log_seq: 0,
            log_entries: Vec::new(),
        })

    }

    pub fn next_request_id(&mut self) -> String {
        self.request_seq += 1;
        format!("console-req-{}", self.request_seq)
    }

    pub fn execute(&mut self, command: ConsoleCommand) -> Result<bool, Box<dyn std::error::Error>> {

        match command {

            ConsoleCommand::Help => {
                print_help();
                self.push_log("help displayed".to_string());
                Ok(true)
            },

            ConsoleCommand::Exit => {
                self.runtime.transport().disconnect_active_peer();
                self.push_log("session exit requested".to_string());
                Ok(false)
            },

            ConsoleCommand::ShowP2p => {
                self.print_p2p_status();
                self.push_log("p2p status displayed".to_string());
                Ok(true)
            },

            ConsoleCommand::ShowLog => {
                self.print_log();
                Ok(true)
            },

            ConsoleCommand::ShowPeers => {
                self.refresh_discovered_peers_from_server()?;
                let peers = self.runtime.transport().discovered_peers();
                let active_peer_id = self.runtime.transport().active_peer_id();
                if peers.is_empty() {
                    println!("no peers discovered");
                } else {
                    for peer in peers {
                        let marker = if Some(peer.peer_id.as_str()) == active_peer_id {
                            "*"
                        } else {
                            " "
                        };
                        println!(
                            "{} peer={} addrs={}",
                            marker,
                            peer.peer_id,
                            peer.addrs.join(", ")
                        );
                    }
                }
                self.push_log("peer list displayed".to_string());
                Ok(true)
            },

            ConsoleCommand::ConnectPeer { user, peer_id } => {
                self.runtime.transport_mut().select_peer(&peer_id)?;
                self.runtime.transport().connect_active_peer()?;
                println!(
                    "notification: connection to {} is successful (session {}@{})",
                    peer_id, user, peer_id
                );
                match self.runtime.transport().session_id() {
                    Ok(Some(token)) => println!("session_id={}", token),
                    Ok(None) => println!("session_id=<none>"),
                    Err(_) => println!("session_id=<unavailable>"),
                }
                self.push_log(format!("connected peer={} as user={}", peer_id, user));
                Ok(true)
            },

            ConsoleCommand::Disconnect => {
                self.runtime.transport().disconnect_active_peer();
                println!("disconnected active peer session");
                self.push_log("active peer disconnected".to_string());
                Ok(true)
            },

            ConsoleCommand::UseDatabase(database) => {

                let probe_request = ConnectorRequest::new(
                    self.next_request_id(),
                    ConnectorCommand::Query {
                        query: DataQuery {
                            database_id: database.clone(),
                            sql: "show tables".to_string(),
                        },
                    },
                );

                let client = ConnectorClient::new(self.runtime.transport().clone());
                client.execute(&probe_request)?;

                self.current_database = Some(database);

                println!(
                    "database switched to {}",
                    self.current_database.as_deref().unwrap_or("<none>")
                );
                self.push_log(format!("database switched to {}", self.current_database.as_deref().unwrap_or("<none>")));
                
                Ok(true)
            },

            ConsoleCommand::Sql(sql) => self.execute_sql(sql),

        }

    }

    fn execute_sql(&mut self, sql: String) -> Result<bool, Box<dyn std::error::Error>> {

        let auth_token_for_session = extract_password_token_input(&sql).map(md5_hash);
        let is_auth_request = auth_token_for_session.is_some();

        let wire_sql = auth_token_for_session
            .as_ref()
            .map(|token| format!("password_token {token}"))
            .unwrap_or_else(|| sql.clone());

        let request_id = self.next_request_id();
        let database_id = resolve_database_for_sql(
            self.current_database.as_deref(),
            is_auth_request,
            &sql,
        )?;

        let command = ConnectorCommand::Query {
            query: DataQuery {
                database_id,
                sql: wire_sql,
            },
        };

        let request = ConnectorRequest::new(request_id.clone(), command);

        let client = ConnectorClient::new(self.runtime.transport().clone());
        let request_start = std::time::Instant::now();
        let mut response = client.execute(&request)?;
        let round_trip_ms = request_start.elapsed().as_millis() as u64;

        if let ConnectorResult::Query(result) = &mut response.result {
            result.timings.network_round_trip_ms = Some(round_trip_ms);
        }

        if let Some(token) = auth_token_for_session {
            if response.status == ResponseStatus::Rejected {
                let _ = self.runtime.transport().set_session_auth_token(None);
            } else {
                self.runtime
                    .transport()
                    .set_session_auth_token(Some(token))?;
            }
        }

        print_response(&response);

        self.push_log(format!(
            "sql request_id={} db={} outcome={}",
            request_id,
            request.query_database_id(),
            summarize_response(&response)
        ));

        Ok(true)

    }

    fn refresh_discovered_peers_from_server(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.runtime.transport().has_live_connection() {
            if let Err(err) = self.runtime.transport().connect_active_peer() {
                log::debug!("server peer refresh skipped: unable to connect active peer: {}", err);
                return Ok(());
            }
        }

        let database_id = self
            .current_database
            .clone()
            .unwrap_or_else(|| AUTH_FALLBACK_DATABASE.to_string());

        let request = ConnectorRequest::new(
            self.next_request_id(),
            ConnectorCommand::Query {
                query: DataQuery {
                    database_id,
                    sql: SERVER_PEER_DISCOVERY_SQL.to_string(),
                },
            },
        );

        let client = ConnectorClient::new(self.runtime.transport().clone());
        let response = match client.execute(&request) {
            Ok(response) => response,
            Err(err) => {
                log::debug!("server peer refresh request failed: {}", err);
                return Ok(());
            }
        };

        let ConnectorResult::Query(result) = response.result else {
            return Ok(());
        };

        for row in result.rows {
            if row.len() < 2 {
                continue;
            }

            let peer_id = String::from_utf8_lossy(&row[0]).trim().to_string();
            if peer_id.is_empty() {
                continue;
            }

            let addrs = String::from_utf8_lossy(&row[1])
                .split(',')
                .map(|addr| addr.trim().to_string())
                .filter(|addr| !addr.is_empty())
                .collect::<Vec<_>>();

            if addrs.is_empty() {
                continue;
            }

            self.runtime.transport_mut().upsert_peer(ConnectorPeer { peer_id, addrs });
        }

        Ok(())
    }

    fn print_p2p_status(&self) {

        let transport = self.runtime.transport();
        let mode = match transport.discovery_mode() {
            connector::ConnectorDiscoveryMode::Kademlia => "kademlia",
        };

        println!("connector p2p:");
        println!("  mode={mode}");
        println!("  protocol={}", transport.protocol());

        if transport.bootstrap_peers().is_empty() {
            println!("  bootstrap_peers=<none>");
        } else {
            println!(
                "  bootstrap_peers={}",
                transport.bootstrap_peers().join(", ")
            );
        }

        println!("  discovered_peer_count={}", transport.discovered_peers().len());
        println!(
            "  active_peer={}",
            transport.active_peer_id().unwrap_or("<none>")
        );
        println!("  active_connection={}", transport.has_live_connection());
        println!("  queued_response_count={}", transport.queued_response_count());
        println!("server p2p:");
        println!(
            "  visibility=not exposed by connector API yet (request/response path is active)"
        );

        match transport.session_auth_token() {
            Ok(Some(_)) => println!("  auth_token=<set>"),
            Ok(None) => println!("  auth_token=<none>"),
            Err(_) => println!("  auth_token=<unavailable>"),
        }

        match transport.session_id() {
            Ok(Some(_)) => println!("  session_id=<set>"),
            Ok(None) => println!("  session_id=<none>"),
            Err(_) => println!("  session_id=<unavailable>"),
        }

    }

    fn push_log(&mut self, message: String) {
        self.log_seq += 1;
        self.log_entries.push(ConsoleLogEntry {
            seqno: self.log_seq,
            message,
        });
    }

    fn print_log(&self) {
        if self.log_entries.is_empty() {
            println!("no console log entries");
            return;
        }

        for entry in &self.log_entries {
            println!("[{}] {}", entry.seqno, entry.message);
        }
    }

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

trait ConsoleRequestExt {
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

fn summarize_response(response: &ConnectorResponse) -> String {
    match &response.result {
        ConnectorResult::Query(result) => format!("query rows={}", result.rows.len()),
        ConnectorResult::Mutation(result) => {
            format!("mutation affected_rows={}", result.affected_rows)
        }
        ConnectorResult::Schema(result) => {
            format!("schema table={} revision={}", result.table_id, result.schema_revision)
        }
        ConnectorResult::Error(message) => format!("error {}", message),
    }
}

pub fn extract_password_token_input(sql: &str) -> Option<&str> {

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let mut parts = trimmed.split_whitespace();
    let command = parts.next()?;
    let password = parts.next()?;
    
    if command.eq_ignore_ascii_case("password") {
        return Some(password);
    }
    
    None

}

fn resolve_database_for_sql(
    current_database: Option<&str>,
    is_auth_request: bool,
    sql: &str,
) -> Result<String, &'static str> {
    if let Some(database) = current_database {
        return Ok(database.to_string());
    }

    if is_auth_request || is_global_sql_without_database(sql) {
        return Ok(AUTH_FALLBACK_DATABASE.to_string());
    }

    Err("no active database selected; run `use <database>;` first")
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
        ("show", "databases") | ("create", "database") | ("drop", "database")
    )
    
}

pub fn parse_console_command(input: &str) -> Result<Option<ConsoleCommand>, String> {

    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Ok(None);
    }

    if !trimmed.ends_with(';') {
        return Ok(None);
    }

    let command_text = trimmed.trim_end_matches(';').trim();
    if command_text.contains('\n') {
        let lines: Vec<&str> = command_text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect();

        if lines.len() > 1
            && lines[..lines.len() - 1]
                .iter()
                .any(|line| is_console_command_fragment(line))
        {
            return Err(
                "previous console command is missing ';' before starting a new command"
                    .to_string(),
            );
        }
    }

    let lowered = command_text.to_lowercase();

    if lowered == "help" || lowered == ".help" {
        return Ok(Some(ConsoleCommand::Help));
    }

    if lowered == "exit" || lowered == "quit" || lowered == "\\q" {
        return Ok(Some(ConsoleCommand::Exit));
    }

    if lowered == "show p2p" {
        return Ok(Some(ConsoleCommand::ShowP2p));
    }

    if lowered == "show log" {
        return Ok(Some(ConsoleCommand::ShowLog));
    }

    if lowered == "show peers" {
        return Ok(Some(ConsoleCommand::ShowPeers));
    }

    if lowered == "disconnect" {
        return Ok(Some(ConsoleCommand::Disconnect));
    }

    if let Some(database_name) = command_text.strip_prefix("use ") {
        let database_name = database_name.trim();
        if database_name.is_empty() {
            return Err("use requires a database name".to_string());
        }
        return Ok(Some(ConsoleCommand::UseDatabase(database_name.to_string())));
    }

    if let Some(target) = command_text.strip_prefix("connect ") {
        let target = target.trim();
        if target.is_empty() {
            return Err("connect requires a peer id".to_string());
        }
        let (user, peer_id) = parse_connect_target(target)?;
        return Ok(Some(ConsoleCommand::ConnectPeer { user, peer_id }));
    }

    let sql = command_text.trim();
    if sql.is_empty() {
        return Ok(None);
    }

    Ok(Some(ConsoleCommand::Sql(sql.to_string())))

}

pub fn print_response(response: &ConnectorResponse) {

    match &response.result {

        ConnectorResult::Query(result) => {

            print_query_table(result);

            println!("{} row(s)", result.rows.len());
            println!(
                "timing: server_total={}ms parse={}ms execute={}ms network_rtt={}ms",
                result.timings.server_total_ms,
                result.timings.server_parse_ms,
                result.timings.server_execute_ms,
                result.timings.network_round_trip_ms.unwrap_or(0)
            );

            if let Some(cache) = &result.timings.cache {
                println!("cache: {:?}", cache);
            }

        },

        ConnectorResult::Mutation(result) => {
            println!("ok: {} row(s) affected", result.affected_rows);
        },

        ConnectorResult::Schema(result) => {
            println!(
                "schema updated: table={} revision={}",
                result.table_id, result.schema_revision
            );
        },

        ConnectorResult::Error(message) => {
            println!("error: {}", message);
        },

    }

}

fn print_query_table(result: &connector::QueryResult) {

    if result.columns.is_empty() {
        return;
    }

    let headers = result
        .columns
        .iter()
        .map(|field| field.field_name.clone())
        .collect::<Vec<_>>();

    let rows = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|col| String::from_utf8_lossy(col).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    let mut widths = headers.iter().map(|h| h.chars().count()).collect::<Vec<_>>();
    for row in &rows {
        for (i, col) in row.iter().enumerate() {
            if i < widths.len() {
                widths[i] = widths[i].max(col.chars().count());
            }
        }
    }

    println!("{}", format_table_separator(&widths));
    println!("{}", format_table_row(&headers, &widths));
    println!("{}", format_table_separator(&widths));
    for row in &rows {
        println!("{}", format_table_row(row, &widths));
    }
    println!("{}", format_table_separator(&widths));

}

fn format_table_separator(widths: &[usize]) -> String {
    let mut sep = String::new();
    sep.push('+');
    for width in widths {
        sep.push_str(&"-".repeat(*width + 2));
        sep.push('+');
    }
    sep
}

fn format_table_row(cells: &[String], widths: &[usize]) -> String {
    let mut line = String::new();
    line.push('|');
    for (i, width) in widths.iter().enumerate() {
        let cell = cells.get(i).map(|s| s.as_str()).unwrap_or("");
        let padding = width.saturating_sub(cell.chars().count());
        line.push(' ');
        line.push_str(cell);
        line.push_str(&" ".repeat(padding + 1));
        line.push('|');
    }
    line
}

pub fn print_help() {
    println!("distdb console commands:");
    println!("  help | .help              show this message");
    println!("  exit | quit | \\q          leave console");
    println!("  use <database>;           switch active database");
    println!("  show p2p;                 display connector/server p2p stack status");
    println!("  show log;                 display in-console command/response log");
    println!("  show peers;               list discovered p2p peers (* = active)");
    println!("  connect <user@peer-id>;   switch session to a discovered peer");
    println!("  disconnect;               close the active peer session connection");
    println!("  <sql>;                    run SQL statements (multi-line supported)");
    println!();
    println!("Note: all commands must end with ';' to execute");
}

pub fn parse_connect_target(target: &str) -> Result<(String, String), String> {

    let Some((user, peer_id)) = target.split_once('@') else {
        return Err("connect requires format user@peer-id".to_string());
    };

    let user = user.trim();
    let peer_id = peer_id.trim();

    if user.is_empty() || peer_id.is_empty() {
        return Err("connect requires format user@peer-id".to_string());
    }

    if user != TEMP_CONNECT_USER {
        return Err("invalid user".to_string());
    }

    Ok((user.to_string(), peer_id.to_string()))

}

fn is_console_command_fragment(line: &str) -> bool {

    let lowered = line.to_lowercase();

    matches!(
        lowered.as_str(),
        "help" | ".help" | "exit" | "quit" | "\\q" | "show p2p" | "show log" | "show peers" | "disconnect"
    ) || lowered.starts_with("use ")
        || lowered.starts_with("connect ")

}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_command_requires_semicolon() {
        assert!(matches!(parse_console_command("show peers"), Ok(None)));
        assert!(matches!(parse_console_command("show peers;"), Ok(Some(ConsoleCommand::ShowPeers))));
    }

    #[test]
    fn parse_command_recognises_keywords() {
        assert!(matches!(parse_console_command("help;"), Ok(Some(ConsoleCommand::Help))));
        assert!(matches!(parse_console_command("exit;"), Ok(Some(ConsoleCommand::Exit))));
        assert!(matches!(parse_console_command("disconnect;"), Ok(Some(ConsoleCommand::Disconnect))));
        assert!(matches!(parse_console_command("show p2p;"), Ok(Some(ConsoleCommand::ShowP2p))));
        assert!(matches!(parse_console_command("show log;"), Ok(Some(ConsoleCommand::ShowLog))));
    }

    #[test]
    fn parse_connect_requires_user_at_peer() {
        assert!(parse_console_command("connect server-node-01;").is_err());
        assert!(parse_console_command("connect @server-node-01;").is_err());
        assert!(parse_console_command("connect other@server-node-01;").is_err());
        assert!(matches!(
            parse_console_command("connect root@server-node-01;"),
            Ok(Some(ConsoleCommand::ConnectPeer { .. }))
        ));
    }

    #[test]
    fn parse_use_database_extracts_name() {
        match parse_console_command("use mydb;") {
            Ok(Some(ConsoleCommand::UseDatabase(name))) => assert_eq!(name, "mydb"),
            other => panic!("unexpected: {:?}", other.is_ok()),
        }
    }

    #[test]
    fn parse_sql_falls_through() {
        assert!(matches!(
            parse_console_command("select 1;"),
            Ok(Some(ConsoleCommand::Sql(_)))
        ));
    }

    #[test]
    fn parse_rejects_new_command_when_previous_missing_semicolon() {
        let result = parse_console_command("show peers\nconnect root@server-node-01;");
        assert!(matches!(result, Err(message) if message.contains("missing ';'")));
    }

    #[test]
    fn ctrl_d_on_empty_does_not_abort() {
        // parse_console_command only handles parsing; empty input returns None
        assert!(matches!(parse_console_command(""), Ok(None)));
    }

    #[test]
    fn extract_password_token_detects_password_command() {
        assert_eq!(extract_password_token_input("password secret;"), Some("secret"));
        assert_eq!(extract_password_token_input("PASSWORD secret"), Some("secret"));
        assert_eq!(extract_password_token_input("select 1"), None);
    }

    #[test]
    fn resolve_database_for_auth_without_selection_uses_fallback() {
        let database = resolve_database_for_sql(None, true, "password secret;")
            .expect("auth should allow fallback");
        assert_eq!(database, "main");
    }

    #[test]
    fn resolve_database_without_selection_rejects_non_auth() {
        let result = resolve_database_for_sql(None, false, "select 1;");
        assert!(matches!(
            result,
            Err("no active database selected; run `use <database>;` first")
        ));
    }

    #[test]
    fn resolve_database_without_selection_allows_show_databases() {
        let database = resolve_database_for_sql(None, false, "show databases;")
            .expect("show databases should not require explicit selection");
        assert_eq!(database, "main");
    }

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
    fn bootstrap_peers_from_cli_args_dedups_and_preserves_order() {
        let args = vec![
            "127.0.0.1:9400".to_string(),
            "servers=node.local:9400,127.0.0.1:9400".to_string(),
        ];

        let peers = bootstrap_peers_from_cli_args(&args);

        assert_eq!(
            peers,
            vec![
                "/ip4/127.0.0.1/tcp/9400".to_string(),
                "/dns/node.local/tcp/9400".to_string(),
            ]
        );
    }

}
