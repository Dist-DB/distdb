
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorP2pConfig,
    ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer, ConnectorRequest,
    ConnectorResponse, ConnectorResult, ConnectorTlsConfig, ConnectorError,
    DataQuery, ResponseStatus,
};
use common::DEFAULT_SERVER_PORT;
use common::helpers::utils::md5_hash;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::{collections::HashSet, net::Ipv4Addr};
use std::time::Duration;

pub const TEMP_CONNECT_USER: &str = "root";
const AUTH_FALLBACK_DATABASE: &str = "main";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";
const IMPORT_TRANSPORT_RETRY_LIMIT: usize = 3;
const IMPORT_TRANSACTION_BATCH_SIZE: usize = 500;
const IMPORT_TRANSACTION_BATCH_MAX_AGE_MS: u128 = 500;
const IMPORT_BEGIN_STATEMENT: &str = "begin /*distdb_import*/";
const SHOW_PEERS_REQUEST_TIMEOUT_SECS_DEFAULT: u64 = 1;
const SHOW_PEERS_REQUEST_TIMEOUT_SECS_ENV: &str = "DISTDB_CONSOLE_SHOW_PEERS_TIMEOUT_SECS";
const DEFAULT_CONNECTOR_IO_TIMEOUT_SECS: u64 = 120;
const IMPORT_LARGE_STATEMENT_BYTES: usize = 256_000;
const IMPORT_INSERT_CHUNK_TARGET_BYTES: usize = 256_000;
const IMPORT_INSERT_CHUNK_MAX_TUPLES: usize = 512;

fn show_peers_request_timeout_secs() -> u64 {
    std::env::var(SHOW_PEERS_REQUEST_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.clamp(1, 30))
        .unwrap_or(SHOW_PEERS_REQUEST_TIMEOUT_SECS_DEFAULT)
}

pub enum ConsoleCommand {
    Help,
    Exit,
    ShowP2p,
    ShowLog,
    ShowPeers,
    ConnectPeer { user: String, peer_id: String },
    Disconnect,
    UseDatabase(String),
    ImportFile(String),
    Sql(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ImportTransactionState {
    enabled: bool,
    active: bool,
    dml_statements_in_batch: usize,
    committed_batches: usize,
    batch_started_at: Option<std::time::Instant>,
    statement_calls: usize,
    execute_statement_ms: u128,
    begin_statement_ms: u128,
    commit_statement_ms: u128,
    query_statement_ms: u128,
    max_statement_ms: u128,
    max_statement_kind: Option<ImportStatementKind>,
    max_statement_bytes: usize,
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

    pub fn new(
        server_list: Vec<String>,
        tls_config: ConnectorTlsConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {

        let bootstrap_peers = normalize_bootstrap_peers(server_list);

        if bootstrap_peers.is_empty() {
            return Err("at least one server address is required".into());
        }

        let mut p2p_config = ConnectorP2pConfig::new("/distdb/kad/1.0.0")
            .with_bootstrap_peers(bootstrap_peers.clone())
            .with_tls_mode(tls_config.mode);

        if let Some(ca_path) = tls_config.ca_path {
            p2p_config = p2p_config.with_tls_ca_path(ca_path);
        }

        let transport = ConnectorP2pTransport::new(p2p_config);

        let runtime = ConnectorP2pRuntime::new(transport);

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

    pub fn startup_connect_user(
        &mut self,
        user: &str,
        requested_peer_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {

        self.refresh_discovered_peers_from_server()?;

        let discovered_peers = self.runtime.transport().discovered_peers();
        let resolved_peer_id = if discovered_peers
            .iter()
            .any(|peer| peer.peer_id == requested_peer_id)
        {
            requested_peer_id.to_string()
        } else if discovered_peers.len() == 1 {
            discovered_peers[0].peer_id.clone()
        } else {

            let discovered_ids = discovered_peers
                .iter()
                .map(|peer| peer.peer_id.clone())
                .collect::<Vec<_>>();

            let hint = if discovered_ids.is_empty() {
                "none".to_string()
            } else {
                discovered_ids.join(", ")
            };

            return Err(format!(
                "peer '{}' is not discovered (discovered peers: {})",
                requested_peer_id, hint
            )
            .into());

        };

        self.execute(ConsoleCommand::ConnectPeer {
            user: user.to_string(),
            peer_id: resolved_peer_id.clone(),
        })?;

        Ok(resolved_peer_id)
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
                    log::info!("no peers discovered");
                } else {
                    for peer in peers {
                        let marker = if Some(peer.peer_id.as_str()) == active_peer_id {
                            "*"
                        } else {
                            " "
                        };
                        log::info!(
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
                self.runtime.transport_mut().connect_active_peer()?;
                log::info!(
                    "notification: connection to {} is successful (session {}@{})",
                    peer_id, user, peer_id
                );
                match self.runtime.transport().session_id() {
                    Ok(Some(token)) => log::info!("session_id={}", token),
                    Ok(None) => log::info!("session_id=<none>"),
                    Err(_) => log::warn!("session_id=<unavailable>"),
                }
                self.push_log(format!("connected peer={} as user={}", peer_id, user));
                Ok(true)
            },

            ConsoleCommand::Disconnect => {
                self.runtime.transport().disconnect_active_peer();
                log::info!("disconnected active peer session");
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

                log::info!(
                    "database switched to {}",
                    self.current_database.as_deref().unwrap_or("<none>")
                );
                self.push_log(format!("database switched to {}", self.current_database.as_deref().unwrap_or("<none>")));
                
                Ok(true)
            },

            ConsoleCommand::ImportFile(file_name) => {
                self.execute_import_file(&file_name)?;
                Ok(true)
            }

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

    fn execute_import_file(&mut self, file_name: &str) -> Result<(), Box<dyn std::error::Error>> {

        let Some(database_id) = self.current_database.clone() else {
            return Err("no active database selected; run `use <database>;` first".into());
        };

        let path = Path::new(file_name);
        let file = std::fs::File::open(path)
            .map_err(|err| format!("failed to open import file '{}': {}", path.display(), err))?;

        log::info!(
            "import started: file={} target_database={}",
            path.display(),
            database_id
        );

        let mut transaction_state = ImportTransactionState {
            enabled: true,
            active: false,
            dml_statements_in_batch: 0,
            committed_batches: 0,
            batch_started_at: None,
            statement_calls: 0,
            execute_statement_ms: 0,
            begin_statement_ms: 0,
            commit_statement_ms: 0,
            query_statement_ms: 0,
            max_statement_ms: 0,
            max_statement_kind: None,
            max_statement_bytes: 0,
        };

        execute_import_from_reader(
            BufReader::new(file),
            &database_id,
            &mut transaction_state,
            |database_id, statement, transaction_state| {
                self.execute_import_with_batching(database_id, statement, transaction_state)
            },
        )
        .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;

        self
            .finalize_import_batching(&database_id, &mut transaction_state)
            .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;

        log::info!(
            "import completed: committed_batches={} exec_ms={} begin_ms={} commit_ms={} query_ms={} stmt_calls={} max_stmt_ms={} max_stmt_kind={} max_stmt_bytes={}",
            transaction_state.committed_batches,
            transaction_state.execute_statement_ms,
            transaction_state.begin_statement_ms,
            transaction_state.commit_statement_ms,
            transaction_state.query_statement_ms,
            transaction_state.statement_calls,
            transaction_state.max_statement_ms,
            transaction_state.max_statement_kind.map(|kind| kind.as_str()).unwrap_or("<none>"),
            transaction_state.max_statement_bytes,
        );

        self.push_log(format!(
            "import file={} db={} committed_batches={} exec_ms={} begin_ms={} commit_ms={} query_ms={} stmt_calls={} max_stmt_ms={} max_stmt_kind={} max_stmt_bytes={}",
            path.display(),
            database_id,
            transaction_state.committed_batches,
            transaction_state.execute_statement_ms,
            transaction_state.begin_statement_ms,
            transaction_state.commit_statement_ms,
            transaction_state.query_statement_ms,
            transaction_state.statement_calls,
            transaction_state.max_statement_ms,
            transaction_state.max_statement_kind.map(|kind| kind.as_str()).unwrap_or("<none>"),
            transaction_state.max_statement_bytes,
        ));

        Ok(())

    }

    fn execute_import_statement(
        &mut self,
        database_id: &str,
        statement: &str,
        transaction_state: &mut ImportTransactionState,
    ) -> Result<(), String> {

        let statement_kind = classify_import_statement(statement);

        for attempt in 0..=IMPORT_TRANSPORT_RETRY_LIMIT {
            let request_id = self.next_request_id();

            let request = ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: DataQuery {
                        database_id: database_id.to_string(),
                        sql: statement.to_string(),
                    },
                },
            );

            let client = ConnectorClient::new(self.runtime.transport().clone());
            let execute_started_at = std::time::Instant::now();
            match client.execute(&request) {
                Ok(response) => {
                    let elapsed_ms = execute_started_at.elapsed().as_millis();
                    record_import_statement_timing(transaction_state, statement_kind, statement.len(), elapsed_ms);
                    return match response.result {
                        ConnectorResult::Error(message) => Err(message),
                        _ => Ok(()),
                    };
                }

                Err(err) => {
                    let elapsed_ms = execute_started_at.elapsed().as_millis();
                    record_import_statement_timing(transaction_state, statement_kind, statement.len(), elapsed_ms);
                    let message = err.to_string();
                    let is_retryable = import_transport_error_is_retryable(&message);

                    if !is_retryable || attempt >= IMPORT_TRANSPORT_RETRY_LIMIT {
                        return Err(message);
                    }

                    self.recover_import_transport()?
                }
            }
        }

        Err("import transport retry loop exhausted".to_string())

    }

    fn execute_import_with_batching(
        &mut self,
        database_id: &str,
        statement: &str,
        transaction_state: &mut ImportTransactionState,
    ) -> Result<(), String> {

        let is_dml = statement_is_import_batchable_dml(statement);

        if transaction_state.enabled && is_dml {
            if !transaction_state.active {
                match self.execute_import_statement(database_id, IMPORT_BEGIN_STATEMENT, transaction_state) {
                    Ok(()) => {
                        transaction_state.active = true;
                        transaction_state.batch_started_at = Some(std::time::Instant::now());
                    }
                    Err(err) => {
                        transaction_state.enabled = false;
                        log::warn!(
                            "import transactional batching disabled: failed to begin transaction: {}",
                            err
                        );
                    }
                }
            }

            self.execute_import_statement(database_id, statement, transaction_state)?;

            if transaction_state.active {
                transaction_state.dml_statements_in_batch += 1;

                let should_commit_by_size =
                    transaction_state.dml_statements_in_batch >= import_transaction_batch_size();
                let should_commit_by_age = transaction_state
                    .batch_started_at
                    .map(|started_at| {
                        started_at.elapsed().as_millis() >= import_transaction_batch_max_age_ms()
                    })
                    .unwrap_or(false);

                if should_commit_by_size || should_commit_by_age {
                    match self.execute_import_statement(database_id, "commit", transaction_state) {
                        Ok(()) => {
                            transaction_state.committed_batches += 1;
                            transaction_state.active = false;
                            transaction_state.dml_statements_in_batch = 0;
                            transaction_state.batch_started_at = None;
                        }
                        Err(err) => {
                            if import_duplicate_key_error_is_skippable(&err) {
                                let _ = self.execute_import_statement(database_id, "rollback", transaction_state);
                                transaction_state.active = false;
                                transaction_state.dml_statements_in_batch = 0;
                                transaction_state.batch_started_at = None;
                                return Err(err);
                            }

                            return Err(err);
                        }
                    }
                }
            }

            return Ok(());
        }

        if transaction_state.active {
            self.execute_import_statement(database_id, "commit", transaction_state)?;
            transaction_state.committed_batches += 1;
            transaction_state.active = false;
            transaction_state.dml_statements_in_batch = 0;
            transaction_state.batch_started_at = None;
        }

        self.execute_import_statement(database_id, statement, transaction_state)
    }

    fn finalize_import_batching(
        &mut self,
        database_id: &str,
        transaction_state: &mut ImportTransactionState,
    ) -> Result<(), String> {
        if !transaction_state.active {
            return Ok(());
        }

        match self.execute_import_statement(database_id, "commit", transaction_state) {
            Ok(()) => {
                transaction_state.committed_batches += 1;
                transaction_state.active = false;
                transaction_state.dml_statements_in_batch = 0;
                transaction_state.batch_started_at = None;
                Ok(())
            }
            Err(err) => {
                if import_duplicate_key_error_is_skippable(&err) {
                    let _ = self.execute_import_statement(database_id, "rollback", transaction_state);
                    transaction_state.active = false;
                    transaction_state.dml_statements_in_batch = 0;
                    transaction_state.batch_started_at = None;
                    log::warn!(
                        "import finalize skipped duplicate-key batch after rollback: {}",
                        err
                    );
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    fn recover_import_transport(&mut self) -> Result<(), String> {
        self.runtime.transport().disconnect_active_peer();

        self.runtime
            .transport_mut()
            .connect_active_peer()
            .map_err(|err| format!("transport reconnect failed: {err}"))?;

        std::thread::sleep(Duration::from_millis(25));
        Ok(())
    }

    fn refresh_discovered_peers_from_server(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let show_peers_timeout_secs = show_peers_request_timeout_secs();

        let mut known_peers = self.runtime.transport().known_peers();
        if known_peers.is_empty() {
            known_peers = self
                .runtime
                .transport()
                .bootstrap_peers()
                .iter()
                .map(|addr| ConnectorPeer {
                    peer_id: addr.clone(),
                    addrs: vec![addr.clone()],
                    is_discovered: false,
                })
                .collect();
        }

        let original_active_peer = self.runtime.transport().active_peer_id().map(ToOwned::to_owned);
        if let Some(active_peer_id) = original_active_peer.as_deref()
            && let Some(position) = known_peers.iter().position(|peer| peer.peer_id == active_peer_id) {
                let active_peer = known_peers.remove(position);
                known_peers.insert(0, active_peer);
            }
        let database_id = self
            .current_database
            .clone()
            .unwrap_or_else(|| AUTH_FALLBACK_DATABASE.to_string());

        let mut refreshed_from_server = false;

        for peer in known_peers {

            if !self
                .runtime
                .transport()
                .known_peers()
                .iter()
                .any(|known| known.peer_id == peer.peer_id)
            {
                self.runtime.transport_mut().upsert_peer(ConnectorPeer {
                    peer_id: peer.peer_id.clone(),
                    addrs: peer.addrs.clone(),
                    is_discovered: false,
                });
            }

            if let Err(err) = self.runtime.transport_mut().select_peer(&peer.peer_id) {
                log::debug!(
                    "server peer refresh skipped for peer_id={}: {}",
                    peer.peer_id,
                    err
                );
                continue;
            }

            if let Err(err) = self.runtime.transport_mut().connect_active_peer() {
                if let ConnectorError::Rejected(message) = &err
                    && message.to_ascii_lowercase().contains("bootstrapp") {
                        return Err(format!(
                            "server is bootstrapping; retry shortly (peer_id={}): {}",
                            peer.peer_id,
                            message
                        )
                        .into());
                    }
                log::debug!(
                    "server peer refresh skipped for peer_id={}: {}",
                    peer.peer_id,
                    err
                );
                continue;
            }

            let request = ConnectorRequest::new(
                self.next_request_id(),
                ConnectorCommand::Query {
                    query: DataQuery {
                        database_id: database_id.clone(),
                        sql: SERVER_PEER_DISCOVERY_SQL.to_string(),
                    },
                },
            );

            let client = ConnectorClient::new(self.runtime.transport().clone());
            let _ = self.runtime.transport().set_active_connection_timeouts(
                Some(Duration::from_secs(show_peers_timeout_secs)),
                Some(Duration::from_secs(show_peers_timeout_secs)),
            );
            let response = match client.execute(&request) {
                Ok(response) => response,
                Err(err) => {
                    let _ = self.runtime.transport().set_active_connection_timeouts(
                        Some(Duration::from_secs(DEFAULT_CONNECTOR_IO_TIMEOUT_SECS)),
                        Some(Duration::from_secs(DEFAULT_CONNECTOR_IO_TIMEOUT_SECS)),
                    );
                    log::debug!(
                        "server peer refresh request failed for peer_id={}: {}",
                        peer.peer_id,
                        err
                    );
                    continue;
                }
            };

            let _ = self.runtime.transport().set_active_connection_timeouts(
                Some(Duration::from_secs(DEFAULT_CONNECTOR_IO_TIMEOUT_SECS)),
                Some(Duration::from_secs(DEFAULT_CONNECTOR_IO_TIMEOUT_SECS)),
            );

            let ConnectorResult::Query(result) = response.result else {
                continue;
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

                self.runtime.transport_mut().upsert_peer(ConnectorPeer {
                    peer_id,
                    addrs,
                    is_discovered: true,
                });
            }

            refreshed_from_server = true;
            break;

        }

        if let Some(active_peer_id) = original_active_peer {
            let _ = self.runtime.transport_mut().select_peer(&active_peer_id);
        }

        if !refreshed_from_server {
            log::debug!(
                "server peer refresh completed without a successful discovery response"
            );
        }

        Ok(())

    }

    fn print_p2p_status(&self) {

        let transport = self.runtime.transport();
        let mode = match transport.discovery_mode() {
            connector::ConnectorDiscoveryMode::Kademlia => "kademlia",
        };

        log::info!("connector p2p:");
        log::info!("  mode={mode}");
        log::info!("  protocol={}", transport.protocol());
        
        let tls_mode = transport.tls_mode().as_str();

        log::info!("  tls_mode={tls_mode}");
        if let Some(ca_path) = transport.tls_ca_path() {
            log::info!("  tls_ca={}", ca_path.display());
        } else {
            log::info!("  tls_ca=<none>");
        }

        if transport.bootstrap_peers().is_empty() {
            log::info!("  bootstrap_peers=<none>");
        } else {
            log::info!(
                "  bootstrap_peers={}",
                transport.bootstrap_peers().join(", ")
            );
        }

        log::info!("  discovered_peer_count={}", transport.discovered_peers().len());
        log::info!(
            "  active_peer={}",
            transport.active_peer_id().unwrap_or("<none>")
        );
        log::info!("  active_connection={}", transport.has_live_connection());
        log::info!("  queued_response_count={}", transport.queued_response_count());
        log::info!("server p2p:");
        log::info!(
            "  visibility=not exposed by connector API yet (request/response path is active)"
        );

        match transport.session_auth_token() {
            Ok(Some(_)) => log::info!("  auth_token=<set>"),
            Ok(None) => log::info!("  auth_token=<none>"),
            Err(_) => log::warn!("  auth_token=<unavailable>"),
        }

        match transport.session_id() {
            Ok(Some(_)) => log::info!("  session_id=<set>"),
            Ok(None) => log::info!("  session_id=<none>"),
            Err(_) => log::warn!("  session_id=<unavailable>"),
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
            log::info!("no console log entries");
            return;
        }

        for entry in &self.log_entries {
            log::info!("[{}] {}", entry.seqno, entry.message);
        }

    }

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportStatementKind {
    Begin,
    Commit,
    Query,
}

impl ImportStatementKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
            Self::Commit => "commit",
            Self::Query => "query",
        }
    }
}

fn classify_import_statement(statement: &str) -> ImportStatementKind {
    let trimmed = statement.trim_start();

    if starts_with_ascii_case_insensitive(trimmed, "begin") {
        return ImportStatementKind::Begin;
    }

    if starts_with_ascii_case_insensitive(trimmed, "commit") {
        return ImportStatementKind::Commit;
    }

    ImportStatementKind::Query
}

fn record_import_statement_timing(
    transaction_state: &mut ImportTransactionState,
    kind: ImportStatementKind,
    statement_bytes: usize,
    elapsed_ms: u128,
) {
    transaction_state.statement_calls += 1;
    transaction_state.execute_statement_ms += elapsed_ms;

    if elapsed_ms > transaction_state.max_statement_ms {
        transaction_state.max_statement_ms = elapsed_ms;
        transaction_state.max_statement_kind = Some(kind);
        transaction_state.max_statement_bytes = statement_bytes;

        log::debug!(
            "import new max statement: kind={} bytes={} elapsed_ms={}",
            kind.as_str(),
            statement_bytes,
            elapsed_ms,
        );
    }

    match kind {
        ImportStatementKind::Begin => transaction_state.begin_statement_ms += elapsed_ms,
        ImportStatementKind::Commit => transaction_state.commit_statement_ms += elapsed_ms,
        ImportStatementKind::Query => transaction_state.query_statement_ms += elapsed_ms,
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

pub fn connector_tls_config_from_cli_args(
    args: &[String],
) -> Result<ConnectorTlsConfig, String> {

    let mode = match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
        Some(raw) => common::TlsMode::parse(raw).ok_or_else(|| {
            format!("invalid tls mode '{}'; expected off|optional|required", raw)
        })?,
        None => common::TlsMode::Optional,
    };

    let ca_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_ca="))
        .map(std::path::PathBuf::from);

    Ok(ConnectorTlsConfig { mode, ca_path })

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
        },
        ConnectorResult::Schema(result) => {
            format!("schema table={} revision={}", result.table_id, result.schema_revision)
        },
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

    if tokens[0] == "show" && tokens[1] == "bootstrap" {
        return tokens.get(2).is_some_and(|token| token == "status");
    }

    if tokens[0] == "show" && tokens[1] == "catalog" {
        return tokens.get(2).is_some_and(|token| token == "workers");
    }

    matches!(
        (tokens[0].as_str(), tokens[1].as_str()),
        ("show", "databases")
            | ("show", "entities")
            | ("show", "server")
            | ("create", "database")
            | ("drop", "database")
    )
    
}

fn execute_import_from_reader<R, F>(
    mut reader: R,
    database_id: &str,
    transaction_state: &mut ImportTransactionState,
    mut execute_statement: F,
) -> Result<(), String>
where
    R: BufRead,
    F: FnMut(&str, &str, &mut ImportTransactionState) -> Result<(), String>,
{

    let mut parser = SqlStatementParser::default();
    let mut pending_bytes = Vec::<u8>::new();

    loop {
        let chunk_len = {
            let buffer = reader.fill_buf().map_err(|err| err.to_string())?;

            if buffer.is_empty() {
                break;
            }

            pending_bytes.extend_from_slice(buffer);
            buffer.len()
        };

        if chunk_len == 0 {
            break;
        }

        reader.consume(chunk_len);

        loop {
            if pending_bytes.is_empty() {
                break;
            }

            let chunk_len = match std::str::from_utf8(&pending_bytes) {

                Ok(valid) => {
                    parser.push_chunk(valid, &mut |statement| {
                        if statement_starts_with_use(statement) {
                            return Ok(());
                        }

                        let normalized_statement = normalize_import_statement(statement);

                        if statement_is_import_dump_directive(&normalized_statement) {
                            return Ok(());
                        }

                        if normalized_statement.len() >= IMPORT_LARGE_STATEMENT_BYTES {
                            log::debug!(
                                "import executing large statement: bytes={} head='{}'",
                                normalized_statement.len(),
                                statement_head_token(&normalized_statement)
                            );
                        }

                        stream_import_insert_values_statements(
                            &normalized_statement,
                            import_insert_chunk_target_bytes(),
                            import_insert_chunk_max_tuples(),
                            |import_statement| {
                                if let Err(err) = execute_statement(database_id, &import_statement, transaction_state) {
                                    if should_skip_import_error(&import_statement, &err) {
                                        return Ok(());
                                    }

                                    return Err(err);
                                }

                                Ok(())

                            },
                        )?;

                        Ok(())

                    })?;

                    pending_bytes.clear();
                    
                    break;

                }

                Err(err) if err.error_len().is_none() => err.valid_up_to(),

                Err(err) => return Err(err.to_string()),
            };

            if chunk_len == 0 {
                break;
            }

            let valid_chunk = std::str::from_utf8(&pending_bytes[..chunk_len])
                .map_err(|err| err.to_string())?;

            parser.push_chunk(valid_chunk, &mut |statement| {
                if statement_starts_with_use(statement) {
                    return Ok(());
                }

                let normalized_statement = normalize_import_statement(statement);
                if statement_is_import_dump_directive(&normalized_statement) {
                    return Ok(());
                }

                if normalized_statement.len() >= IMPORT_LARGE_STATEMENT_BYTES {
                    log::debug!(
                        "import executing large statement: bytes={} head='{}'",
                        normalized_statement.len(),
                        statement_head_token(&normalized_statement)
                    );
                }

                stream_import_insert_values_statements(
                    &normalized_statement,
                    import_insert_chunk_target_bytes(),
                    import_insert_chunk_max_tuples(),
                    |import_statement| {
                        if let Err(err) = execute_statement(database_id, &import_statement, transaction_state) {
                            if should_skip_import_error(&import_statement, &err) {
                                return Ok(());
                            }

                            return Err(err);
                        }

                        Ok(())
                    },
                )?;

                Ok(())
        })?;

            pending_bytes.drain(..chunk_len);
        }
    }

    parser.flush(&mut |statement| {
        if statement_starts_with_use(statement) {
            return Ok(());
        }

        let normalized_statement = normalize_import_statement(statement);
        if statement_is_import_dump_directive(&normalized_statement) {
            return Ok(());
        }

        if normalized_statement.len() >= IMPORT_LARGE_STATEMENT_BYTES {
            log::debug!(
                "import executing large statement: bytes={} head='{}'",
                normalized_statement.len(),
                statement_head_token(&normalized_statement)
            );
        }

        stream_import_insert_values_statements(
            &normalized_statement,
            import_insert_chunk_target_bytes(),
            import_insert_chunk_max_tuples(),
            |import_statement| {
            if let Err(err) = execute_statement(database_id, &import_statement, transaction_state) {
                if should_skip_import_error(&import_statement, &err) {
                    return Ok(());
                }

                return Err(err);
            }

            Ok(())
        },
        )?;

        Ok(())
    })?;

    Ok(())

}

fn statement_starts_with_use(statement: &str) -> bool {
    starts_with_ascii_case_insensitive(statement.trim_start(), "use ")
}

fn should_skip_import_error(statement: &str, error: &str) -> bool {
    let normalized_statement = statement.trim_start();
    let normalized_error = error.to_ascii_lowercase();

    if starts_with_ascii_case_insensitive(normalized_statement, "drop table")
        && normalized_error.contains("not found") {
        return true;
    }

    if starts_with_ascii_case_insensitive(normalized_statement, "insert ")
        && import_duplicate_key_error_is_skippable(error)
    {
        return true;
    }

    false
}

fn import_duplicate_key_error_is_skippable(error: &str) -> bool {
    let normalized_error = error.to_ascii_lowercase();
    normalized_error.contains("duplicate primary key") || normalized_error.contains("duplicate key")
}

fn statement_is_import_dump_directive(statement: &str) -> bool {
    let normalized = statement.trim_start();

    starts_with_ascii_case_insensitive(normalized, "lock tables ")
        || starts_with_ascii_case_insensitive(normalized, "unlock tables")
    || starts_with_ascii_case_insensitive(normalized, "drop table ")
        || starts_with_ascii_case_insensitive(normalized, "delimiter ")
        || starts_with_ascii_case_insensitive(normalized, "set ")
        || normalized.starts_with("/*!")
}

    fn statement_is_import_batchable_dml(statement: &str) -> bool {
        let normalized = statement.trim_start();

        starts_with_ascii_case_insensitive(normalized, "insert ")
        || starts_with_ascii_case_insensitive(normalized, "update ")
        || starts_with_ascii_case_insensitive(normalized, "delete ")
        || starts_with_ascii_case_insensitive(normalized, "replace ")
    }

fn normalize_import_statement(statement: &str) -> String {
    let mut normalized = statement.to_string();

    // MySQL dumps commonly include index USING clauses that are currently unsupported.
    // Removing them keeps structural intent while allowing first-pass CREATE parsing.
    normalized = remove_case_insensitive_all(&normalized, " USING BTREE");
    normalized = remove_case_insensitive_all(&normalized, " USING HASH");

    normalized
}

fn statement_head_token(statement: &str) -> String {
    statement
        .split_whitespace()
        .next()
        .unwrap_or("<empty>")
        .to_ascii_uppercase()
}

#[cfg(test)]
fn split_import_insert_values_statement(
    statement: &str,
    max_bytes: usize,
    max_tuples_per_chunk: usize,
) -> Vec<String> {
    let mut chunks = Vec::<String>::new();
    stream_import_insert_values_statements(
        statement,
        max_bytes,
        max_tuples_per_chunk,
        |chunk| {
            chunks.push(chunk.to_string());
            Ok(())
        },
    )
    .expect("import chunk splitting should not fail when collecting chunks");

    chunks
}

fn stream_import_insert_values_statements<F>(
    statement: &str,
    max_bytes: usize,
    max_tuples_per_chunk: usize,
    mut on_statement: F,
) -> Result<(), String>
where
    F: FnMut(&str) -> Result<(), String>,
{
    let normalized = statement.trim_start();
    if !starts_with_ascii_case_insensitive(normalized, "insert ") {
        return on_statement(statement);
    }

    let Some(values_index) = find_ascii_case_insensitive(statement, " values ") else {
        return on_statement(statement);
    };

    let prefix_end = values_index + " values ".len();
    let prefix = &statement[..prefix_end];
    let values_tail = &statement[prefix_end..];

    let tuples = extract_insert_value_tuples(values_tail);
    if tuples.len() <= 1 {
        return on_statement(statement);
    }

    let mut current = prefix.to_string();
    let mut tuples_in_chunk = 0usize;

    for tuple in tuples {
        let tuple = tuple.trim();
        let additional = if current.len() > prefix.len() {
            tuple.len() + 1
        } else {
            tuple.len()
        };

        if current.len() > prefix.len()
            && (current.len() + additional > max_bytes
                || tuples_in_chunk >= max_tuples_per_chunk)
        {
            on_statement(&current)?;
            current = prefix.to_string();
            tuples_in_chunk = 0;
        }

        if current.len() > prefix.len() {
            current.push(',');
        }
        current.push_str(tuple);
        tuples_in_chunk += 1;
    }

    if current.len() > prefix.len() {
        on_statement(&current)?;
    }

    Ok(())
}

fn import_insert_chunk_target_bytes() -> usize {
    std::env::var("IMPORT_INSERT_CHUNK_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 8_192)
        .unwrap_or(IMPORT_INSERT_CHUNK_TARGET_BYTES)
}

fn import_insert_chunk_max_tuples() -> usize {
    std::env::var("IMPORT_INSERT_CHUNK_MAX_TUPLES")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(IMPORT_INSERT_CHUNK_MAX_TUPLES)
}

fn import_transaction_batch_size() -> usize {
    std::env::var("IMPORT_TX_BATCH_SIZE")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(IMPORT_TRANSACTION_BATCH_SIZE)
}

fn import_transaction_batch_max_age_ms() -> u128 {
    std::env::var("IMPORT_TX_BATCH_MAX_AGE_MS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u128>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(IMPORT_TRANSACTION_BATCH_MAX_AGE_MS)
}

fn find_ascii_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    if needle.is_empty() {
        return Some(0);
    }

    let haystack_bytes = haystack.as_bytes();
    let needle_bytes = needle.as_bytes();

    if needle_bytes.len() > haystack_bytes.len() {
        return None;
    }

    (0..=haystack_bytes.len() - needle_bytes.len()).find(|index| {
        haystack_bytes[*index..*index + needle_bytes.len()].eq_ignore_ascii_case(needle_bytes)
    })
}

fn starts_with_ascii_case_insensitive(input: &str, prefix: &str) -> bool {
    input
        .get(..prefix.len())
        .is_some_and(|candidate| candidate.eq_ignore_ascii_case(prefix))
}

fn extract_insert_value_tuples(values_tail: &str) -> Vec<&str> {
    let mut tuples = Vec::<&str>::new();

    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick_quote = false;
    let mut escape_next = false;
    let mut paren_depth = 0usize;
    let mut tuple_start: Option<usize> = None;

    let bytes = values_tail.as_bytes();
    let mut index = 0usize;

    while index < bytes.len() {
        let ch = bytes[index] as char;

        if escape_next {
            escape_next = false;
            index += 1;
            continue;
        }

        if (in_single_quote || in_double_quote) && ch == '\\' {
            escape_next = true;
            index += 1;
            continue;
        }

        if ch == '\'' && !in_double_quote && !in_backtick_quote {
            in_single_quote = !in_single_quote;
            index += 1;
            continue;
        }

        if ch == '"' && !in_single_quote && !in_backtick_quote {
            in_double_quote = !in_double_quote;
            index += 1;
            continue;
        }

        if ch == '`' && !in_single_quote && !in_double_quote {
            in_backtick_quote = !in_backtick_quote;
            index += 1;
            continue;
        }

        if in_single_quote || in_double_quote || in_backtick_quote {
            index += 1;
            continue;
        }

        if ch == '(' {
            if paren_depth == 0 {
                tuple_start = Some(index);
            }
            paren_depth += 1;
        } else if ch == ')' && paren_depth > 0 {
            paren_depth -= 1;
            if paren_depth == 0
                && let Some(start) = tuple_start.take() {
                    tuples.push(&values_tail[start..=index]);
                }
        }

        index += 1;
    }

    tuples
}

fn import_transport_error_is_retryable(message: &str) -> bool {
    let normalized = message.to_ascii_lowercase();

    normalized.contains("no queued response")
        || normalized.contains("resource temporarily unavailable")
        || normalized.contains("failed to read response length")
        || normalized.contains("no active peer connection")
        || normalized.contains("connection reset")
        || normalized.contains("broken pipe")
}

fn remove_case_insensitive_all(input: &str, needle: &str) -> String {
    let mut output = String::with_capacity(input.len());

    let mut index = 0;
    while let Some(relative) = find_ascii_case_insensitive(&input[index..], needle) {
        let found = index + relative;
        output.push_str(&input[index..found]);
        index = found + needle.len();
    }

    output.push_str(&input[index..]);
    output
}

#[derive(Default)]
struct SqlStatementParser {
    buffer: String,
    in_single_quote: bool,
    in_double_quote: bool,
    in_backtick_quote: bool,
    in_block_comment: bool,
    in_line_comment: bool,
    pending_dash: bool,
    pending_slash: bool,
    pending_block_comment_star: bool,
}

impl SqlStatementParser {

    fn push_chunk<F>(
        &mut self,
        chunk: &str,
        on_statement: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(&str) -> Result<(), String>,
    {

        for ch in chunk.chars() {

            if self.in_line_comment {
                if ch == '\n' {
                    self.in_line_comment = false;
                    if !self.buffer.is_empty()
                        && !self.in_single_quote
                        && !self.in_double_quote
                        && !self.in_backtick_quote
                    {
                        self.buffer.push('\n');
                    }
                }
                continue;
            }

            if self.in_block_comment {
                if self.pending_block_comment_star && ch == '/' {
                    self.in_block_comment = false;
                    self.pending_block_comment_star = false;
                } else {
                    self.pending_block_comment_star = ch == '*';
                }
                continue;
            }

            if self.pending_dash {
                self.pending_dash = false;

                if !self.in_single_quote && !self.in_double_quote && !self.in_backtick_quote && ch == '-' {
                    self.in_line_comment = true;
                    continue;
                }

                self.buffer.push('-');
            }

            if self.pending_slash {
                self.pending_slash = false;

                if !self.in_single_quote && !self.in_double_quote && !self.in_backtick_quote && ch == '*' {
                    self.in_block_comment = true;
                    self.pending_block_comment_star = false;
                    continue;
                }

                self.buffer.push('/');
            }

            if !self.in_single_quote && !self.in_double_quote && !self.in_backtick_quote {
                if ch == '-' {
                    self.pending_dash = true;
                    continue;
                }

                if ch == '#' {
                    self.in_line_comment = true;
                    continue;
                }

                if ch == '/' {
                    self.pending_slash = true;
                    continue;
                }
            }

            if ch == '\'' && !self.in_double_quote && !self.in_backtick_quote {
                let escaped = self.buffer.ends_with('\\');
                if !escaped {
                    self.in_single_quote = !self.in_single_quote;
                }
                self.buffer.push(ch);
                continue;
            }

            if ch == '"' && !self.in_single_quote && !self.in_backtick_quote {
                let escaped = self.buffer.ends_with('\\');
                if !escaped {
                    self.in_double_quote = !self.in_double_quote;
                }
                self.buffer.push(ch);
                continue;
            }

            if ch == '`' && !self.in_single_quote && !self.in_double_quote {
                self.in_backtick_quote = !self.in_backtick_quote;
                self.buffer.push(ch);
                continue;
            }

            if ch == ';' && !self.in_single_quote && !self.in_double_quote && !self.in_backtick_quote {
                let statement = self.buffer.trim();
                if !statement.is_empty() {
                    on_statement(statement)?;
                }
                self.buffer.clear();
                continue;
            }

            self.buffer.push(ch);
        }

        Ok(())

    }

    fn flush<F>(&mut self, on_statement: &mut F) -> Result<(), String>
    where
        F: FnMut(&str) -> Result<(), String>,
    {
        if self.pending_dash {
            self.buffer.push('-');
            self.pending_dash = false;
        }

        if self.pending_slash {
            self.buffer.push('/');
            self.pending_slash = false;
        }

        let statement = self.buffer.trim();
        if !statement.is_empty() {
            on_statement(statement)?;
        }

        self.buffer.clear();
        self.in_line_comment = false;
        self.pending_block_comment_star = false;
        Ok(())
    }

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

    let import_prefix = "import";
    if lowered.starts_with(import_prefix)
        && command_text
            .chars()
            .nth(import_prefix.len())
            .map(|ch| ch.is_whitespace())
            .unwrap_or(true)
    {
        let file_name = command_text[import_prefix.len()..].trim();
        if file_name.is_empty() {
            return Err("import requires a file name".to_string());
        }

        let file_name = file_name
            .trim_matches('"')
            .trim_matches('\'')
            .to_string();

        if file_name.is_empty() {
            return Err("import requires a file name".to_string());
        }

        return Ok(Some(ConsoleCommand::ImportFile(file_name)));
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
    println!("  import <file.sql>;        stream SQL file into active database");
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
        || lowered.starts_with("import ")
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
    fn parse_import_extracts_file_name() {
        match parse_console_command("import data/locations.sql;") {
            Ok(Some(ConsoleCommand::ImportFile(file_name))) => {
                assert_eq!(file_name, "data/locations.sql")
            }
            other => panic!("unexpected: {:?}", other.is_ok()),
        }
    }

    #[test]
    fn parse_import_requires_file_name() {
        assert!(parse_console_command("import ;").is_err());
    }

    #[test]
    fn parse_sql_falls_through() {
        assert!(matches!(
            parse_console_command("select 1;"),
            Ok(Some(ConsoleCommand::Sql(_)))
        ));
    }

    #[test]
    fn import_reader_splits_and_executes_statements() {
        let input = "\
            -- file header\n\
            use sample;\n\
            create table people (id int, name text);\n\
            insert into people values (1, 'alice;demo');\n\
            # footer\n\
        ";

        let mut executed = Vec::<String>::new();
        let mut transaction_state = ImportTransactionState {
            enabled: false,
            active: false,
            dml_statements_in_batch: 0,
            committed_batches: 0,
            batch_started_at: None,
            statement_calls: 0,
            execute_statement_ms: 0,
            begin_statement_ms: 0,
            commit_statement_ms: 0,
            query_statement_ms: 0,
            max_statement_ms: 0,
            max_statement_kind: None,
            max_statement_bytes: 0,
        };

        execute_import_from_reader(
            BufReader::new(input.as_bytes()),
            "main",
            &mut transaction_state,
            |db, statement, _transaction_state| {
                executed.push(format!("{}:{}", db, statement.trim()));
                Ok(())
            },
        )
        .expect("import reader should succeed");

        assert_eq!(transaction_state.committed_batches, 0);
        assert_eq!(executed.len(), 2);
        assert!(executed[0].contains("create table people"));
        assert!(executed[1].contains("insert into people"));
    }

    #[test]
    fn import_reader_populates_mock_table_structures() {
        let input = "\
            create table users (id int, name text);\n\
            insert into users values (1, 'alice');\n\
            insert into users values (2, 'bob');\n\
            create table regions (id int);\n\
            insert into regions values (10);\n\
        ";

        let mut row_counts = std::collections::HashMap::<String, usize>::new();
        let mut transaction_state = ImportTransactionState {
            enabled: false,
            active: false,
            dml_statements_in_batch: 0,
            committed_batches: 0,
            batch_started_at: None,
            statement_calls: 0,
            execute_statement_ms: 0,
            begin_statement_ms: 0,
            commit_statement_ms: 0,
            query_statement_ms: 0,
            max_statement_ms: 0,
            max_statement_kind: None,
            max_statement_bytes: 0,
        };

        execute_import_from_reader(
            BufReader::new(input.as_bytes()),
            "main",
            &mut transaction_state,
            |_db, statement, _transaction_state| {
                let normalized = statement.trim().to_ascii_lowercase();

                if let Some(rest) = normalized.strip_prefix("create table ") {
                    let table_name = rest.split_whitespace().next().unwrap_or("");
                    if !table_name.is_empty() {
                        row_counts.entry(table_name.to_string()).or_insert(0);
                    }
                    return Ok(());
                }

                if let Some(rest) = normalized.strip_prefix("insert into ") {
                    let table_name = rest.split_whitespace().next().unwrap_or("");
                    if table_name.is_empty() {
                        return Err("insert statement did not include table name".to_string());
                    }

                    let entry = row_counts.entry(table_name.to_string()).or_insert(0);
                    *entry += 1;
                    return Ok(());
                }

                Err(format!("unexpected statement in import: {}", statement))
            },
        )
        .expect("import reader should succeed");

        assert_eq!(transaction_state.committed_batches, 0);
        assert_eq!(row_counts.get("users"), Some(&2));
        assert_eq!(row_counts.get("regions"), Some(&1));
    }

    #[test]
    fn import_reader_skips_drop_table_not_found_errors() {
        let input = "\
            drop table ip_lookup;\n\
            create table ip_lookup (id int);\n\
            insert into ip_lookup values (1);\n\
        ";

        let mut executed = Vec::<String>::new();
        let mut transaction_state = ImportTransactionState {
            enabled: false,
            active: false,
            dml_statements_in_batch: 0,
            committed_batches: 0,
            batch_started_at: None,
            statement_calls: 0,
            execute_statement_ms: 0,
            begin_statement_ms: 0,
            commit_statement_ms: 0,
            query_statement_ms: 0,
            max_statement_ms: 0,
            max_statement_kind: None,
            max_statement_bytes: 0,
        };

        execute_import_from_reader(
            BufReader::new(input.as_bytes()),
            "main",
            &mut transaction_state,
            |_db, statement, _transaction_state| {
                let normalized = statement.trim().to_ascii_lowercase();
                if normalized.starts_with("drop table") {
                    return Err("drop table failed: 'ip_lookup' not found".to_string());
                }

                executed.push(statement.trim().to_string());
                Ok(())
            },
        )
        .expect("import reader should continue past non-fatal drop errors");

        assert_eq!(transaction_state.committed_batches, 0);
        assert_eq!(executed.len(), 2);
    }

    #[test]
    fn normalize_import_statement_removes_mysql_using_clauses() {
        let statement = "create table t (id int, primary key (id) USING BTREE, key idx (id) USING HASH)";
        let normalized = normalize_import_statement(statement);

        assert!(!normalized.to_ascii_lowercase().contains("using btree"));
        assert!(!normalized.to_ascii_lowercase().contains("using hash"));
        assert!(normalized.to_ascii_lowercase().contains("primary key (id)"));
        assert!(normalized.to_ascii_lowercase().contains("key idx (id)"));
    }

    #[test]
    fn import_reader_normalizes_mysql_using_clauses_before_execute() {
        let input = "create table t (id int, primary key (id) USING BTREE);";
        let mut transaction_state = ImportTransactionState {
            enabled: false,
            active: false,
            dml_statements_in_batch: 0,
            committed_batches: 0,
            batch_started_at: None,
            statement_calls: 0,
            execute_statement_ms: 0,
            begin_statement_ms: 0,
            commit_statement_ms: 0,
            query_statement_ms: 0,
            max_statement_ms: 0,
            max_statement_kind: None,
            max_statement_bytes: 0,
        };

        let mut executed_count = 0usize;
        execute_import_from_reader(
            BufReader::new(input.as_bytes()),
            "main",
            &mut transaction_state,
            |_db, statement, _transaction_state| {
                if statement.to_ascii_lowercase().contains("using btree") {
                    return Err("statement still contains unsupported USING BTREE".to_string());
                }

                executed_count += 1;

                Ok(())
            },
        )
        .expect("import reader should normalize unsupported USING clauses");

        assert_eq!(executed_count, 1);
        assert_eq!(transaction_state.committed_batches, 0);
    }

    #[test]
    fn import_reader_skips_mysql_dump_directives() {
        let input = "\
            set @old_foreign_key_checks=@@foreign_key_checks;\n\
            lock tables `ip_lookup` write;\n\
            insert into ip_lookup values (1);\n\
            unlock tables;\n\
        ";

        let mut executed = Vec::<String>::new();
        let mut transaction_state = ImportTransactionState {
            enabled: false,
            active: false,
            dml_statements_in_batch: 0,
            committed_batches: 0,
            batch_started_at: None,
            statement_calls: 0,
            execute_statement_ms: 0,
            begin_statement_ms: 0,
            commit_statement_ms: 0,
            query_statement_ms: 0,
            max_statement_ms: 0,
            max_statement_kind: None,
            max_statement_bytes: 0,
        };

        execute_import_from_reader(
            BufReader::new(input.as_bytes()),
            "main",
            &mut transaction_state,
            |_db, statement, _transaction_state| {
                executed.push(statement.trim().to_string());
                Ok(())
            },
        )
        .expect("import reader should skip dump directives");

        assert_eq!(transaction_state.committed_batches, 0);
        assert_eq!(executed, vec!["insert into ip_lookup values (1)"]);
    }

    #[test]
    fn import_transport_error_retry_classifier_matches_expected_errors() {
        assert!(import_transport_error_is_retryable(
            "transport error: failed to read response length: Resource temporarily unavailable (os error 35)"
        ));
        assert!(import_transport_error_is_retryable(
            "transport error: no queued response for request_id"
        ));
        assert!(!import_transport_error_is_retryable(
            "command rejected: sql parse failed"
        ));
    }

    #[test]
    fn import_batchable_dml_classifier_matches_expected_statements() {
        assert!(statement_is_import_batchable_dml("insert into x values (1)"));
        assert!(statement_is_import_batchable_dml(" update users set a=1"));
        assert!(statement_is_import_batchable_dml("delete from users"));
        assert!(statement_is_import_batchable_dml("replace into users values (1)"));
        assert!(!statement_is_import_batchable_dml("create table users (id int)"));
        assert!(!statement_is_import_batchable_dml("alter table users add key (id)"));
    }

    #[test]
    fn split_import_insert_values_statement_splits_large_insert_values() {
        let statement = "insert into users values (1,'alice'),(2,'bob'),(3,'charlie')";
        let chunks = split_import_insert_values_statement(statement, 48, 16);

        assert!(chunks.len() >= 2);
        assert!(chunks.iter().all(|chunk| chunk.to_ascii_lowercase().starts_with("insert into users values ")));
        assert!(chunks.iter().all(|chunk| chunk.contains("(")));
    }

    #[test]
    fn split_import_insert_values_statement_keeps_non_insert_statement() {
        let statement = "create table users (id int, name text)";
        let chunks = split_import_insert_values_statement(statement, 32, 16);

        assert_eq!(chunks, vec![statement.to_string()]);
    }

    #[test]
    fn split_import_insert_values_statement_respects_tuple_cap() {
        let statement = "insert into users values (1,'alice'),(2,'bob'),(3,'charlie'),(4,'dana')";
        let chunks = split_import_insert_values_statement(statement, 4_096, 2);

        assert_eq!(chunks.len(), 2);
        assert!(chunks[0].contains("(1,'alice')"));
        assert!(chunks[0].contains("(2,'bob')"));
        assert!(chunks[1].contains("(3,'charlie')"));
        assert!(chunks[1].contains("(4,'dana')"));
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
    fn resolve_database_without_selection_allows_status_commands() {
        let entities_db = resolve_database_for_sql(None, false, "show entities;")
            .expect("show entities should not require explicit selection");
        assert_eq!(entities_db, "main");

        let bootstrap_db = resolve_database_for_sql(None, false, "show bootstrap status;")
            .expect("show bootstrap status should not require explicit selection");
        assert_eq!(bootstrap_db, "main");

        let peers_db = resolve_database_for_sql(None, false, "show server peers;")
            .expect("show server peers should not require explicit selection");
        assert_eq!(peers_db, "main");

        let workers_db = resolve_database_for_sql(None, false, "show catalog workers;")
            .expect("show catalog workers should not require explicit selection");
        assert_eq!(workers_db, "main");
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
