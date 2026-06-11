
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer, ConnectorRequest,
    ConnectorResponse, ConnectorResult, DataQuery,
};

use std::env;
use std::io::{self, Write};

const TEMP_CONNECT_USER: &str = "root";

enum ConsoleCommand {
    Help,
    Exit,
    ShowP2p,
    ShowPeers,
    ConnectPeer { user: String, peer_id: String },
    Disconnect,
    UseDatabase(String),
    Sql(String),
}

struct ConsoleSession {
    runtime: ConnectorP2pRuntime,
    current_database: String,
    request_seq: u64,
}

impl ConsoleSession {
    fn new(server_address: String) -> Result<Self, Box<dyn std::error::Error>> {
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
            current_database: "main".to_string(),
            request_seq: 0,
        })
    }

    fn next_request_id(&mut self) -> String {
        self.request_seq += 1;
        format!("console-req-{}", self.request_seq)
    }

    fn execute(&mut self, command: ConsoleCommand) -> Result<bool, Box<dyn std::error::Error>> {
        match command {
            ConsoleCommand::Help => {
                print_help();
                Ok(true)
            }
            ConsoleCommand::Exit => {
                self.runtime.transport().disconnect_active_peer();
                Ok(false)
            }
            ConsoleCommand::ShowP2p => {
                self.print_p2p_status();
                Ok(true)
            }
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
            }
            ConsoleCommand::ConnectPeer { user, peer_id } => {
                self.runtime.transport_mut().select_peer(&peer_id)?;
                self.runtime.transport().connect_active_peer()?;
                println!("connected session to {}@{}", user, peer_id);
                Ok(true)
            }
            ConsoleCommand::Disconnect => {
                self.runtime.transport().disconnect_active_peer();
                println!("disconnected active peer session");
                Ok(true)
            }
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

                self.current_database = database;
                println!("database switched to {}", self.current_database);
                Ok(true)
            }
            ConsoleCommand::Sql(sql) => self.execute_sql(sql),
        }
    }

    fn execute_sql(&mut self, sql: String) -> Result<bool, Box<dyn std::error::Error>> {
        let request_id = self.next_request_id();
        let command = ConnectorCommand::Query {
            query: DataQuery {
                database_id: self.current_database.clone(),
                sql,
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
    }
}

fn parse_console_command(input: &str) -> Result<Option<ConsoleCommand>, String> {
    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Ok(None);
    }

    // Require commands to end with semicolon
    if !trimmed.ends_with(';') {
        return Ok(None);
    }

    let command_text = trimmed.trim_end_matches(';').trim();
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

fn print_response(response: &ConnectorResponse) {
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
        }
        ConnectorResult::Mutation(result) => {
            println!("ok: {} row(s) affected", result.affected_rows);
        }
        ConnectorResult::Schema(result) => {
            println!(
                "schema updated: table={} revision={}",
                result.table_id, result.schema_revision
            );
        }
        ConnectorResult::Error(message) => {
            println!("error: {}", message);
        }
    }
}

fn print_help() {
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

fn parse_connect_target(target: &str) -> Result<(String, String), String> {
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let server_address = env::args().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: console <server-address>",
        )
    })?;

    let mut session = ConsoleSession::new(server_address)?;

    println!("distdb console");
    println!("type help for commands, or \\q to quit");
    println!("all commands must end with ';' to execute");

    let mut accumulated_command = String::new();
    loop {
        print!("distdb:{}> ", session.current_database);
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes_read = io::stdin().read_line(&mut line)?;
        if bytes_read == 0 {
            if !accumulated_command.trim().is_empty() {
                accumulated_command.clear();
                println!("aborted pending command");
                continue;
            }
            println!();
            break;
        }

        accumulated_command.push_str(&line);

        match parse_console_command(&accumulated_command) {
            Ok(Some(command)) => {
                accumulated_command.clear();
                match session.execute(command) {
                    Ok(should_continue) => {
                        if !should_continue {
                            break;
                        }
                    }
                    Err(error) => {
                        eprintln!("error: {error}");
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                accumulated_command.clear();
                println!("error: {error}");
            }
        }
    }

    Ok(())
}
