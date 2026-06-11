
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer, ConnectorRequest,
    ConnectorResponse, ConnectorResult, DataQuery, ResponseStatus,
};
use common::helpers::utils::md5_hash;

pub const TEMP_CONNECT_USER: &str = "root";
const AUTH_FALLBACK_DATABASE: &str = "main";

pub enum ConsoleCommand {
    Help,
    Exit,
    ShowP2p,
    ShowPeers,
    ConnectPeer { user: String, peer_id: String },
    Disconnect,
    UseDatabase(String),
    Sql(String),
}

pub struct ConsoleSession {
    pub runtime: ConnectorP2pRuntime,
    pub current_database: Option<String>,
    request_seq: u64,
}

impl ConsoleSession {

    pub fn new(server_address: String) -> Result<Self, Box<dyn std::error::Error>> {

        let transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec![server_address.clone()]),
        );

        let mut runtime = ConnectorP2pRuntime::new(transport);

        runtime.handle_event(ConnectorP2pEvent::PeerDiscovered(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec![server_address],
        }))?;

        Ok(Self {
            runtime,
            current_database: None,
            request_seq: 0,
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
                Ok(true)
            },

            ConsoleCommand::Exit => {
                self.runtime.transport().disconnect_active_peer();
                Ok(false)
            },

            ConsoleCommand::ShowP2p => {
                self.print_p2p_status();
                Ok(true)
            },

            ConsoleCommand::ShowPeers => {
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
                Ok(true)
            },

            ConsoleCommand::ConnectPeer { user, peer_id } => {
                self.runtime.transport_mut().select_peer(&peer_id)?;
                self.runtime.transport().connect_active_peer()?;
                println!(
                    "notification: connection to {} is successful (session {}@{})",
                    peer_id, user, peer_id
                );
                match self.runtime.transport().session_shared_authorization() {
                    Ok(Some(token)) => println!("shared_authorization={}", token),
                    Ok(None) => println!("shared_authorization=<none>"),
                    Err(_) => println!("shared_authorization=<unavailable>"),
                }
                Ok(true)
            },

            ConsoleCommand::Disconnect => {
                self.runtime.transport().disconnect_active_peer();
                println!("disconnected active peer session");
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

        Ok(true)

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

        match transport.session_shared_authorization() {
            Ok(Some(_)) => println!("  shared_authorization=<set>"),
            Ok(None) => println!("  shared_authorization=<none>"),
            Err(_) => println!("  shared_authorization=<unavailable>"),
        }

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

            if !result.columns.is_empty() {
                let header = result
                    .columns
                    .iter()
                    .map(|field| field.field_name.as_str())
                    .collect::<Vec<_>>()
                    .join(" | ");
                println!("{}", header);
                println!("{}", "-".repeat(header.len()));
            }

            for row in &result.rows {
                let rendered = row
                    .iter()
                    .map(|col| String::from_utf8_lossy(col).to_string())
                    .collect::<Vec<_>>()
                    .join(" | ");
                println!("{}", rendered);
            }

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

pub fn print_help() {
    println!("distdb console commands:");
    println!("  help | .help              show this message");
    println!("  exit | quit | \\q          leave console");
    println!("  use <database>;           switch active database");
    println!("  show p2p;                 display connector/server p2p stack status");
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
        "help" | ".help" | "exit" | "quit" | "\\q" | "show p2p" | "show peers" | "disconnect"
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

}
