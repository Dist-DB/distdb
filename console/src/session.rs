use common::helpers::utils::md5_hash;
use connector::{
    ConnectorCommand, ConnectorError, ConnectorRequest, ConnectorResult, ConnectorTransport,
    DataQuery, ResponseStatus,
};
use peerlib::{
    ConnectorP2pConfig, ConnectorP2pRuntime, ConnectorP2pTransport, ConnectorPeer,
    ConnectorTlsConfig,
};
use std::collections::HashSet;
use std::time::Duration;

use crate::utils::{
    auth_password_input, extract_password_token_input, resolve_database_for_sql,
    show_peers_request_timeout_secs, sql_request_timeout_secs,
    validate_use_database_probe_response, ConsoleRequestExt,
    AUTH_FALLBACK_DATABASE,
};
use crate::{
    import, output, ConsoleCommand, SERVER_PEER_DISCOVERY_SQL, SQL_TRANSPORT_RETRY_LIMIT,
};

#[path = "session_import.rs"]
mod session_import;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ImportTransactionState {
    pub(crate) enabled: bool,
    pub(crate) active: bool,
    pub(crate) dml_statements_in_batch: usize,
    pub(crate) committed_batches: usize,
    pub(crate) batch_started_at: Option<std::time::Instant>,
    pub(crate) statement_calls: usize,
    pub(crate) execute_statement_ms: u128,
    pub(crate) begin_statement_ms: u128,
    pub(crate) commit_statement_ms: u128,
    pub(crate) query_statement_ms: u128,
    pub(crate) max_statement_ms: u128,
    pub(crate) max_statement_kind: Option<import::ImportStatementKind>,
    pub(crate) max_statement_bytes: usize,
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

fn host_from_socket_addr(addr: &str) -> Option<String> {
    let trimmed = addr.trim();

    if trimmed.starts_with("/") {
        let parts = trimmed.split('/').filter(|value| !value.is_empty()).collect::<Vec<_>>();
        if parts.len() >= 2 && matches!(parts[0], "dns" | "dns4" | "dns6" | "ip4" | "ip6") {
            let host = parts[1].trim().to_ascii_lowercase();
            if !host.is_empty() {
                return Some(host);
            }
        }
    }

    let host = addr
        .trim()
        .trim_matches('[')
        .trim_matches(']')
        .rsplit_once(':')
        .map(|(value, _)| value)
        .unwrap_or(addr)
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase();

    if host.is_empty() {
        None
    } else {
        Some(host)
    }
}

fn is_cloud_gateway_addr(addr: &str) -> bool {
    host_from_socket_addr(addr)
        .is_some_and(|host| host == "app.distdb.com" || host.ends_with(".cloud.distdb.com"))
}

fn normalize_discovered_addrs_for_gateway(
    source_peer_addrs: &[String],
    discovered_addrs: Vec<String>,
) -> Vec<String> {
    let source_gateway_addrs = source_peer_addrs
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty() && is_cloud_gateway_addr(value))
        .collect::<Vec<_>>();

    if source_gateway_addrs.is_empty() {
        return discovered_addrs;
    }

    if discovered_addrs.iter().any(|value| is_cloud_gateway_addr(value)) {
        return discovered_addrs;
    }

    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for addr in source_gateway_addrs {
        if seen.insert(addr.clone()) {
            deduped.push(addr);
        }
    }

    deduped
}

impl ConsoleSession {
    pub fn new(
        server_list: Vec<String>,
        tls_config: ConnectorTlsConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let bootstrap_peers = crate::normalize_bootstrap_peers(server_list);

        if bootstrap_peers.is_empty() {
            return Err("at least one server address is required".into());
        }

        let mut p2p_config = ConnectorP2pConfig::new("/distdb/kad/1.0.0")
            .with_bootstrap_peers(bootstrap_peers)
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
        let resolved_peer_id = self.resolve_startup_peer_id(requested_peer_id)?;

        self.execute(ConsoleCommand::ConnectPeer {
            user: user.to_string(),
            peer_id: resolved_peer_id.clone(),
        })?;

        Ok(resolved_peer_id)
    }

    fn resolve_startup_peer_id(
        &self,
        requested_peer_id: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let known_peers = self.runtime.transport().known_peers();
        if known_peers
            .iter()
            .any(|peer| peer.peer_id == requested_peer_id)
        {
            return Ok(requested_peer_id.to_string());
        }

        if known_peers.len() == 1 {
            return Ok(known_peers[0].peer_id.clone());
        }

        let bootstrap_peers = self.runtime.transport().bootstrap_peers();
        if bootstrap_peers.len() == 1 {
            return Ok(bootstrap_peers[0].clone());
        }

        let known_ids = known_peers
            .iter()
            .map(|peer| peer.peer_id.clone())
            .collect::<Vec<_>>();
        let known_hint = if known_ids.is_empty() {
            "none".to_string()
        } else {
            known_ids.join(", ")
        };

        let bootstrap_hint = if bootstrap_peers.is_empty() {
            "none".to_string()
        } else {
            bootstrap_peers.join(", ")
        };

        Err(format!(
            "peer '{}' is not available before authentication (known peers: {}; bootstrap peers: {})",
            requested_peer_id, known_hint, bootstrap_hint
        )
        .into())
    }

    pub fn execute(&mut self, command: ConsoleCommand) -> Result<bool, Box<dyn std::error::Error>> {
        match command {
            ConsoleCommand::Help => {
                crate::print_help();
                self.push_log("help displayed".to_string());
                Ok(true)
            }

            ConsoleCommand::Exit => {
                self.runtime.transport().disconnect_active_peer();
                self.push_log("session exit requested".to_string());
                Ok(false)
            }

            ConsoleCommand::SetDelimiter(delimiter) => {
                log::info!("delimiter set to {}", delimiter);
                self.push_log(format!("delimiter set to {}", delimiter));
                Ok(true)
            }

            ConsoleCommand::ShowP2p => {
                self.print_p2p_status();
                self.push_log("p2p status displayed".to_string());
                Ok(true)
            }

            ConsoleCommand::ShowLog => {
                self.print_log();
                Ok(true)
            }

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
            }

            ConsoleCommand::ConnectPeer { user, peer_id } => {
                self.runtime.transport_mut().select_peer(&peer_id)?;
                self.runtime.transport_mut().connect_active_peer()?;
                log::info!(
                    "notification: connection to {} is successful (session {}@{})",
                    peer_id,
                    user,
                    peer_id
                );
                match self.runtime.transport().session_id() {
                    Ok(Some(token)) => log::info!("session_id={}", token),
                    Ok(None) => log::info!("session_id=<none>"),
                    Err(_) => log::warn!("session_id=<unavailable>"),
                }
                self.push_log(format!("connected peer={} as user={}", peer_id, user));
                Ok(true)
            }

            ConsoleCommand::Disconnect => {
                self.runtime.transport().disconnect_active_peer();
                log::info!("disconnected active peer session");
                self.push_log("active peer disconnected".to_string());
                Ok(true)
            }

            ConsoleCommand::UseDatabase(database) => {
                let sql_timeout_secs = sql_request_timeout_secs();
                let _ = self.runtime.transport().set_active_connection_timeouts(
                    Some(Duration::from_secs(sql_timeout_secs)),
                    Some(Duration::from_secs(sql_timeout_secs)),
                );

                let probe_request = ConnectorRequest::new(
                    self.next_request_id(),
                    ConnectorCommand::Query {
                        query: DataQuery {
                            database_id: database.clone(),
                            sql: "show tables".to_string(),
                        },
                    },
                );

                let probe_response = self.runtime.transport().request(&probe_request)?;

                validate_use_database_probe_response(&database, &probe_response)
                    .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;

                self.current_database = Some(database);

                log::info!(
                    "database switched to {}",
                    self.current_database.as_deref().unwrap_or("<none>")
                );
                self.push_log(format!(
                    "database switched to {}",
                    self.current_database.as_deref().unwrap_or("<none>")
                ));

                Ok(true)
            }

            ConsoleCommand::ImportFile(file_name) => {
                self.execute_import_file(&file_name)?;
                Ok(true)
            }

            ConsoleCommand::Sql(sql) => self.execute_sql(sql),
        }
    }

    fn execute_sql(&mut self, sql: String) -> Result<bool, Box<dyn std::error::Error>> {
        let sql_timeout_secs = sql_request_timeout_secs();

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
            let request_start = std::time::Instant::now();

            let _ = self.runtime.transport().set_active_connection_timeouts(
                Some(Duration::from_secs(sql_timeout_secs)),
                Some(Duration::from_secs(sql_timeout_secs)),
            );

            match self.runtime.transport().request(&request) {
                Ok(mut current_response) => {
                    let round_trip_ms = request_start.elapsed().as_millis() as u64;
                    if let ConnectorResult::Query(result) = &mut current_response.result {
                        result.timings.network_round_trip_ms = Some(round_trip_ms);
                    }

                    response = Some(current_response);
                    break;
                }

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

                    self.recover_import_transport()
                        .map_err(|err| -> Box<dyn std::error::Error> { err.into() })?;
                }
            }
        }

        let response =
            response.ok_or_else(|| std::io::Error::other("sql transport retry loop exhausted"))?;

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
        session_import::execute_import_file(self, file_name)
    }

    fn recover_import_transport(&mut self) -> Result<(), String> {
        session_import::recover_import_transport(self)
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
        let mut preferred_peer_id = None;

        if let Some(active_peer_id) = original_active_peer.as_deref()
            && let Some(position) = known_peers
                .iter()
                .position(|peer| peer.peer_id == active_peer_id)
        {
            let active_peer = known_peers.remove(position);
            known_peers.insert(0, active_peer);
            preferred_peer_id = Some(active_peer_id.to_string());
        }

        let database_id = self
            .current_database
            .clone()
            .unwrap_or_else(|| AUTH_FALLBACK_DATABASE.to_string());

        let mut refreshed_from_server = false;
        let mut last_refresh_error: Option<String> = None;

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
                    && message.to_ascii_lowercase().contains("bootstrapp")
                {
                    return Err(format!(
                        "server is bootstrapping; retry shortly (peer_id={}): {}",
                        peer.peer_id,
                        message
                    )
                    .into());
                }
                last_refresh_error = Some(err.to_string());
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

            let _ = self.runtime.transport().set_active_connection_timeouts(
                Some(Duration::from_secs(show_peers_timeout_secs)),
                Some(Duration::from_secs(show_peers_timeout_secs)),
            );

            let response = match self.runtime.transport().request(&request) {
                Ok(response) => response,

                Err(err) => {
                    let _ = self.runtime.transport().set_active_connection_timeouts(
                        Some(Duration::from_secs(sql_request_timeout_secs())),
                        Some(Duration::from_secs(sql_request_timeout_secs())),
                    );

                    last_refresh_error = Some(err.to_string());

                    log::debug!(
                        "server peer refresh request failed for peer_id={}: {}",
                        peer.peer_id,
                        err
                    );

                    continue;
                }
            };

            let _ = self.runtime.transport().set_active_connection_timeouts(
                Some(Duration::from_secs(sql_request_timeout_secs())),
                Some(Duration::from_secs(sql_request_timeout_secs())),
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

                let addrs = normalize_discovered_addrs_for_gateway(&peer.addrs, addrs);

                if addrs.is_empty() {
                    continue;
                }

                self.runtime.transport_mut().upsert_peer(ConnectorPeer {
                    peer_id: peer_id.clone(),
                    addrs: addrs.clone(),
                    is_discovered: true,
                });

                if preferred_peer_id.as_deref() == Some(peer_id.as_str()) {
                    let _ = self.runtime.transport_mut().select_peer(&peer_id);
                }
            }

            refreshed_from_server = true;
            break;
        }

        if let Some(active_peer_id) = original_active_peer.as_deref() {
            let _ = self.runtime.transport_mut().select_peer(active_peer_id);
        }

        if !refreshed_from_server {
            log::debug!("server peer refresh completed without a successful discovery response");

            if let Some(err) = last_refresh_error {
                return Err(format!(
                    "server peer discovery is unreachable: {}. Verify the server connector endpoint is reachable from this client (host/port, firewall, and load balancer).",
                    err
                )
                .into());
            }
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
            log::info!("  bootstrap_peers={}", transport.bootstrap_peers().join(", "));
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

#[cfg(test)]
#[path = "session_test.rs"]
mod tests;