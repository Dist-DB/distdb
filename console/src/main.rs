
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig, ConnectorP2pEvent,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer, ConnectorRequest,
    ConnectorResponse, ConnectorResult, DataQuery, FieldDef, FieldType,
    MutationResult, QueryResult, QueryTimings, ResponseStatus,
};

use std::env;
use std::io::{self, Write};

enum ConsoleCommand {
    Help,
    Exit,
    ShowPeers,
    ConnectPeer(String),
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
            ConsoleCommand::Exit => Ok(false),
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
            ConsoleCommand::ConnectPeer(peer_id) => {
                self.runtime.transport_mut().select_peer(&peer_id)?;
                println!("connected session to peer={}", peer_id);
                Ok(true)
            }
            ConsoleCommand::UseDatabase(database) => {
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

        let request = ConnectorRequest::new(request_id.clone(), command.clone());
        let simulated_response = simulate_server_response(&request_id, &command);

        self.runtime
            .handle_event(ConnectorP2pEvent::ResponseReceived(simulated_response))?;

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
}

fn simulate_server_response(request_id: &str, command: &ConnectorCommand) -> ConnectorResponse {
    match command {
        ConnectorCommand::Mutation { .. } => ConnectorResponse {
            request_id: request_id.to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
        },
        ConnectorCommand::Query { .. } => ConnectorResponse {
            request_id: request_id.to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Query(QueryResult {
                columns: vec![FieldDef {
                    seqno: 1,
                    field_name: "result".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: false,
                    default_value: None,
                }],
                rows: vec![vec![format!("simulated connector boundary response: {:?}", command).into_bytes()]],
                timings: QueryTimings {
                    server_parse_ms: 0,
                    server_execute_ms: 0,
                    server_total_ms: 0,
                    network_round_trip_ms: None,
                    cache: None,
                },
            }),
        },
        _ => ConnectorResponse {
            request_id: request_id.to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
        },
    }
}

fn parse_console_command(input: &str) -> Result<Option<ConsoleCommand>, String> {
    let trimmed = input.trim();

    if trimmed.is_empty() {
        return Ok(None);
    }

    let lowered = trimmed.to_lowercase();
    if lowered == "help" || lowered == ".help" {
        return Ok(Some(ConsoleCommand::Help));
    }
    if lowered == "exit" || lowered == "quit" || lowered == "\\q" {
        return Ok(Some(ConsoleCommand::Exit));
    }
    if lowered == "show peers" {
        return Ok(Some(ConsoleCommand::ShowPeers));
    }
    if let Some(database_name) = trimmed.strip_prefix("use ") {
        let database_name = database_name.trim();
        if database_name.is_empty() {
            return Err("use requires a database name".to_string());
        }
        return Ok(Some(ConsoleCommand::UseDatabase(database_name.to_string())));
    }
    if let Some(peer_id) = trimmed.strip_prefix("connect ") {
        let peer_id = peer_id.trim();
        if peer_id.is_empty() {
            return Err("connect requires a peer id".to_string());
        }
        return Ok(Some(ConsoleCommand::ConnectPeer(peer_id.to_string())));
    }

    let sql = trimmed.trim_end_matches(';').trim();
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
    println!("  use <database>            switch active database");
    println!("  show peers                list discovered p2p peers (* = active)");
    println!("  connect <peer-id>         switch session to a discovered peer");
    println!("  <sql>                     run one or more SQL statements");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server_address = env::args().nth(1).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: console <server-address>",
        )
    })?;

    let mut session = ConsoleSession::new(server_address)?;

    println!("distdb console");
    println!("type help for commands, or \\q to quit");

    loop {
        print!("distdb:{}> ", session.current_database);
        io::stdout().flush()?;

        let mut line = String::new();
        let bytes_read = io::stdin().read_line(&mut line)?;
        if bytes_read == 0 {
            println!();
            break;
        }

        match parse_console_command(&line) {
            Ok(Some(command)) => {
                if !session.execute(command)? {
                    break;
                }
            }
            Ok(None) => {}
            Err(error) => println!("error: {error}"),
        }
    }

    Ok(())
}
