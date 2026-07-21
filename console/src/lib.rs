
use connector::{
    ConnectorClient, ConnectorCommand, ConnectorRequest,
    ConnectorResult, ConnectorError,
    DataQuery, ResponseStatus,
};
use peerlib::{
    ConnectorP2pConfig, ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer,
    ConnectorTlsConfig,
};
use common::helpers::utils::md5_hash;
use std::io::BufReader;
use std::path::Path;
use std::time::Duration;

mod bootstrap;
mod commands;
mod import;
mod output;

pub const TEMP_CONNECT_USER: &str = "root";
const AUTH_FALLBACK_DATABASE: &str = "main";
const SERVER_PEER_DISCOVERY_SQL: &str = "__distdb_show_server_peers__";
const IMPORT_TRANSPORT_RETRY_LIMIT: usize = 3;
const SQL_TRANSPORT_RETRY_LIMIT: usize = 3;
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
    SetDelimiter(String),
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
    max_statement_kind: Option<import::ImportStatementKind>,
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

            ConsoleCommand::SetDelimiter(delimiter) => {
                log::info!("delimiter set to {}", delimiter);
                self.push_log(format!("delimiter set to {}", delimiter));
                Ok(true)
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
            },

            ConsoleCommand::Sql(sql) => self.execute_sql(sql),

        }

    }

    fn execute_sql(&mut self, sql: String) -> Result<bool, Box<dyn std::error::Error>> {

        let auth_password_for_session = auth_password_input(&sql);
        let auth_token_for_session = extract_password_token_input(&sql).map(md5_hash);
        let is_auth_request = auth_password_for_session.is_some();

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

        let mut response = None;

        for attempt in 0..=SQL_TRANSPORT_RETRY_LIMIT {
            let client = ConnectorClient::new(self.runtime.transport().clone());
            let request_start = std::time::Instant::now();

            match client.execute(&request) {

                Ok(mut current_response) => {
                    let round_trip_ms = request_start.elapsed().as_millis() as u64;
                    if let ConnectorResult::Query(result) = &mut current_response.result {
                        result.timings.network_round_trip_ms = Some(round_trip_ms);
                    }

                    response = Some(current_response);
                    break;
                },

                Err(err) => {
                    let message = err.to_string();
                    let is_retryable = import::import_transport_error_is_retryable(&message);

                    if !is_retryable || attempt >= SQL_TRANSPORT_RETRY_LIMIT {
                        return Err(err.into());
                    }

                    log::warn!(
                        "sql transport retry {}/{} after request_id={}: {}",
                        attempt + 1,
                        SQL_TRANSPORT_RETRY_LIMIT,
                        request_id,
                        message
                    );

                    self
                        .recover_import_transport()
                        .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
                },

            }

        }

        let response = response.ok_or_else(|| {
            std::io::Error::other("sql transport retry loop exhausted")
        })?;

        if let Some(token) = auth_token_for_session {
            if response.status == ResponseStatus::Rejected {
                let _ = self.runtime.transport().set_session_auth_token(None);
            } else {
                self.runtime
                    .transport()
                    .set_session_auth_token(Some(token))?;
            }
        }

        output::print_response(&response);

        self.push_log(format!(
            "sql request_id={} db={} outcome={}",
            request_id,
            request.query_database_id(),
            output::summarize_response(&response)
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

        import::execute_import_from_reader(
            BufReader::new(file),
            &database_id,
            &mut transaction_state,
            |database_id, statement, transaction_state| {
                self.execute_import_with_batching(database_id, statement, transaction_state)
            },
        )
        .map_err(|err| {
            log::warn!(
                "import failed: file={} target_database={} error={}",
                path.display(),
                database_id,
                err,
            );
            let boxed: Box<dyn std::error::Error> = err.into();
            boxed
        })?;

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

        let statement_kind = import::classify_import_statement(statement);

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
                    
                    import::record_import_statement_timing(
                        transaction_state,
                        statement_kind,
                        statement.len(),
                        elapsed_ms,
                    );
                    
                    return match response.result {
                        ConnectorResult::Error(message) => {
                            log::warn!(
                                "import execution failed: db={} kind={} statement_bytes={} preview='{}' error={}",
                                database_id,
                                statement_kind.as_str(),
                                statement.len(),
                                import::statement_preview(statement),
                                message,
                            );
                            Err(message)
                        },
                        _ => Ok(()),
                    };

                },

                Err(err) => {

                    let elapsed_ms = execute_started_at.elapsed().as_millis();
                    
                    import::record_import_statement_timing(
                        transaction_state,
                        statement_kind,
                        statement.len(),
                        elapsed_ms,
                    );
                    
                    let message = err.to_string();
                    let is_retryable = import::import_transport_error_is_retryable(&message);

                    if !is_retryable || attempt >= IMPORT_TRANSPORT_RETRY_LIMIT {
                        log::warn!(
                            "import transport failed: db={} kind={} statement_bytes={} preview='{}' error={}",
                            database_id,
                            statement_kind.as_str(),
                            statement.len(),
                            import::statement_preview(statement),
                            message,
                        );
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

        let is_dml = import::statement_is_import_batchable_dml(statement);

        if transaction_state.enabled && is_dml {

            if !transaction_state.active {

                match self.execute_import_statement(database_id, IMPORT_BEGIN_STATEMENT, transaction_state) {

                    Ok(()) => {
                        transaction_state.active = true;
                        transaction_state.batch_started_at = Some(std::time::Instant::now());
                    },

                    Err(err) => {
                        transaction_state.enabled = false;
                        log::warn!(
                            "import transactional batching disabled: failed to begin transaction: {}",
                            err
                        );
                    }

                }

            }

            match self.execute_import_statement(database_id, statement, transaction_state) {

                Ok(()) => {},

                Err(err) => {
                    if transaction_state.active && import::import_duplicate_key_error_is_skippable(&err) {
                        let _ = self.execute_import_statement(database_id, "rollback", transaction_state);
                        transaction_state.active = false;
                        transaction_state.dml_statements_in_batch = 0;
                        transaction_state.batch_started_at = None;
                    }

                    log::warn!(
                        "import batch failed: db={} statement_bytes={} preview='{}' error={}",
                        database_id,
                        statement.len(),
                        import::statement_preview(statement),
                        err,
                    );

                    return Err(err);
                }

            }

            if transaction_state.active {

                transaction_state.dml_statements_in_batch += 1;

                let should_commit_by_size =
                    transaction_state.dml_statements_in_batch >= import::import_transaction_batch_size();

                let should_commit_by_age = transaction_state
                    .batch_started_at
                    .map(|started_at| {
                        started_at.elapsed().as_millis() >= import::import_transaction_batch_max_age_ms()
                    })
                    .unwrap_or(false);

                if should_commit_by_size || should_commit_by_age {

                    match self.execute_import_statement(database_id, "commit", transaction_state) {

                        Ok(()) => {
                            transaction_state.committed_batches += 1;
                            transaction_state.active = false;
                            transaction_state.dml_statements_in_batch = 0;
                            transaction_state.batch_started_at = None;
                        },

                        Err(err) => {
                            log::warn!(
                                "import batch commit failed: db={} queued_dml={} error={}",
                                database_id,
                                transaction_state.dml_statements_in_batch,
                                err,
                            );

                            if import::import_duplicate_key_error_is_skippable(&err) {
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

        match self.execute_import_statement(database_id, statement, transaction_state) {
            Ok(()) => Ok(()),
            Err(err) => {
                log::warn!(
                    "import statement failed outside batching: db={} statement_bytes={} preview='{}' error={}",
                    database_id,
                    statement.len(),
                    import::statement_preview(statement),
                    err,
                );
                Err(err)
            }
        }

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
            },

            Err(err) => {
                if import::import_duplicate_key_error_is_skippable(&err) {
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
                    log::warn!(
                        "import finalize failed: db={} queued_dml={} error={}",
                        database_id,
                        transaction_state.dml_statements_in_batch,
                        err,
                    );
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
            peerlib::ConnectorDiscoveryMode::Kademlia => "kademlia",
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

pub fn normalize_bootstrap_addr(raw: &str) -> Option<String> {
    bootstrap::normalize_bootstrap_addr(raw)

}

pub fn normalize_bootstrap_peers<I>(peers: I) -> Vec<String>
where
    I: IntoIterator<Item = String>,
{
    bootstrap::normalize_bootstrap_peers(peers)
}

pub fn bootstrap_peers_from_cli_args(args: &[String]) -> Vec<String> {
    bootstrap::bootstrap_peers_from_cli_args(args)

}

pub fn connector_tls_config_from_cli_args(
    args: &[String],
) -> Result<ConnectorTlsConfig, String> {
    bootstrap::connector_tls_config_from_cli_args(args)

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

pub fn extract_password_token_input(sql: &str) -> Option<&str> {
    commands::extract_password_token_input(sql)

}

pub fn auth_password_input(sql: &str) -> Option<&str> {
    commands::auth_password_input(sql)
}

fn resolve_database_for_sql(
    current_database: Option<&str>,
    is_auth_request: bool,
    sql: &str,
) -> Result<String, &'static str> {
    commands::resolve_database_for_sql(
        current_database,
        is_auth_request,
        sql,
        AUTH_FALLBACK_DATABASE,
    )

}

pub fn parse_console_command(input: &str) -> Result<Option<ConsoleCommand>, String> {
    commands::parse_console_command(input, TEMP_CONNECT_USER)

}

pub fn parse_console_command_with_delimiter(
    input: &str,
    delimiter: &str,
) -> Result<Option<ConsoleCommand>, String> {
    commands::parse_console_command_with_delimiter(input, TEMP_CONNECT_USER, delimiter)

}

pub fn print_help() {
    commands::print_help();
}

pub fn parse_connect_target(target: &str) -> Result<(String, String), String> {
    commands::parse_connect_target(target, TEMP_CONNECT_USER)

}
