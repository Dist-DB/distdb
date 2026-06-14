use std::collections::HashMap;
use std::path::PathBuf;

use common::helpers::format::FileKind;
use common::helpers::{create_dir, list_files};
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, DataMutation,
    MutationResult, SchemaCommand,
};
#[cfg(test)]
use serverlib::decode_row_payload;
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore, TransactionId, TransactionKind,
    TransactionRecord, UserId,
};

use crate::core::config::ServerRuntimeConfig;
use crate::core::mappings::query::{
    abort_external_write_group, commit_external_write_group, handle_query_command,
    handle_query_command_in_write_group,
};
use crate::core::transaction_coordinator::{QueryRoutingDecision, TransactionCoordinator};
use crate::engine::wal_probe::{WalProbeResult, run_wal_probe};
use crate::helpers::ServerAppError;

#[derive(Debug)]
pub struct ServerApp {
    config: ServerRuntimeConfig,
    node_data_dir: PathBuf,
    wal: ConcurrentWalManager,
    catalogs: HashMap<String, DatabaseCatalog>,
    runtime_indexes: RuntimeIndexStore,
    transaction_coordinator: TransactionCoordinator,
}

impl ServerApp {
    pub fn new(config: ServerRuntimeConfig) -> Result<Self, ServerAppError> {
        let node_config = config.to_node_config();
        node_config
            .validate()
            .map_err(|msg| ServerAppError::InvalidConfig(msg.to_string()))?;

        let node_data_dir = config.data_dir.join(&config.node_id);

        create_dir(&node_data_dir).map_err(|e| {
            ServerAppError::InvalidConfig(format!(
                "cannot create node data directory '{}': {}",
                node_data_dir.display(),
                e
            ))
        })?;

        log::info!("node data directory: {}", node_data_dir.display());

        let wal = ConcurrentWalManager::with_data_dir(node_data_dir.clone());
        log::info!("server app created for node_id={}", config.node_id);

        Ok(Self {
            config,
            node_data_dir,
            wal,
            catalogs: HashMap::new(),
            runtime_indexes: RuntimeIndexStore::new(),
            transaction_coordinator: TransactionCoordinator::new(),
        })
    }

    pub fn bootstrap(&mut self) -> Result<(), ServerAppError> {
        self.load_catalogs_from_disk()?;
        self.replay_catalog_state_from_wal()?;
        self.runtime_indexes
            .bootstrap_from_catalogs(&self.catalogs, &self.wal);
        log::info!(
            "server bootstrap complete for node_id={} data_dir={}",
            self.config.node_id,
            self.node_data_dir.display()
        );
        Ok(())
    }

    pub fn node_data_dir(&self) -> &PathBuf {
        &self.node_data_dir
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn catalogs(&self) -> &HashMap<String, DatabaseCatalog> {
        &self.catalogs
    }

    pub fn run_wal_smoke_test(&self) -> Result<WalProbeResult, ServerAppError> {
        // Keep startup probe isolated so repeated process boots do not mutate
        // persisted WAL streams and trigger out-of-order validation errors.
        let probe_wal = ConcurrentWalManager::new();
        run_wal_probe(&probe_wal).map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }

    pub fn handle_connector_request(&mut self, request: &ConnectorRequest) -> ConnectorResponse {
        self.handle_connector_request_for_session(request, &request.request_id)
    }

    pub fn handle_connector_request_for_session(
        &mut self,
        request: &ConnectorRequest,
        session_id: &str,
    ) -> ConnectorResponse {
        let command_info = command_info(&request.command);
        let command_path = command_info.path;
        log::info!(
            "connector request dispatch request_id={} path={}",
            request.request_id,
            command_path
        );

        let response = match command_info.kind {

            CommandKind::CreateDatabase => {
                let ConnectorCommand::CreateDatabase { database_name } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                match DatabaseCatalog::create_new_database(database_name, &self.node_data_dir) {
                    Ok(catalog) => {
                        self.catalogs.insert(catalog.database_id.0.clone(), catalog);

                        ConnectorResponse::applied(
                            request.request_id.clone(),
                            ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
                        )
                    }

                    Err(err) => ConnectorResponse::rejected(
                        request.request_id.clone(),
                        format!("create database failed: {err}"),
                    ),
                }
            },

            CommandKind::Query => {
                let ConnectorCommand::Query { query } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                if let Some(response) =
                    self.handle_transaction_control_query(&request.request_id, session_id, query)
                {
                    response
                } else {
                    match self.transaction_coordinator.route_query(
                        session_id,
                        query.clone(),
                        is_staged_dml_query(query),
                    ) {
                        Ok(QueryRoutingDecision::ExecuteImmediately) => handle_query_command(
                            &request.request_id,
                            query,
                            &mut self.catalogs,
                            &self.wal,
                            &self.node_data_dir,
                            &mut self.runtime_indexes,
                        ),
                        Ok(QueryRoutingDecision::Staged) => ConnectorResponse::applied(
                            request.request_id.clone(),
                            ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
                        ),
                        Ok(QueryRoutingDecision::Rejected(message)) => {
                            ConnectorResponse::rejected(request.request_id.clone(), message)
                        }
                        Err(err) => ConnectorResponse::rejected(request.request_id.clone(), err),
                    }
                }
            },

            CommandKind::Schema => ConnectorResponse::rejected(
                request.request_id.clone(),
                "schema command execution is not wired yet",
            ),

            CommandKind::Mutation => ConnectorResponse::rejected(
                request.request_id.clone(),
                "mutation command execution is not wired yet",
            ),

        };

        match &response.result {

            ConnectorResult::Error(message) => {
                log::warn!(
                    "connector request completed request_id={} path={} status={:?} error={}",
                    request.request_id,
                    command_path,
                    response.status,
                    message
                );
            },

            _ => {
                log::info!(
                    "connector request completed request_id={} path={} status={:?}",
                    request.request_id,
                    command_path,
                    response.status
                );
            }

        }

        response
        
    }

    fn handle_transaction_control_query(
        &mut self,
        request_id: &str,
        session_id: &str,
        query: &connector::DataQuery,
    ) -> Option<ConnectorResponse> {
        let normalized = query
            .sql
            .trim()
            .trim_end_matches(';')
            .trim()
            .to_ascii_lowercase();

        if normalized.starts_with("begin") || normalized.starts_with("start transaction") {
            if let Err(err) = self.transaction_coordinator.begin(session_id) {
                return Some(ConnectorResponse::rejected(request_id.to_string(), err));
            }

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                request_id,
                SessionTxMarkerType::Begin,
                0,
            ) {
                log::warn!("failed to append transaction begin marker: {}", err);
            }

            return Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            ));
        }

        if normalized.starts_with("commit") {
            let staged_queries = match self.transaction_coordinator.take_for_commit(session_id) {
                Ok(staged) => staged,
                Err(err) => {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                }
            };

            let staged_count = staged_queries.len();

            let total_affected_rows = match self.commit_staged_queries(request_id, &staged_queries)
            {
                Ok(total) => total,
                Err(err) => {
                    if let Err(restore_err) = self
                        .transaction_coordinator
                        .restore_after_failed_commit(session_id, staged_queries)
                    {
                        log::error!(
                            "failed to restore staged transaction after commit error: {}",
                            restore_err
                        );
                    }

                    if let Err(marker_err) = self.append_session_tx_marker(
                        session_id,
                        request_id,
                        SessionTxMarkerType::CommitFailed,
                        staged_count,
                    ) {
                        log::warn!("failed to append transaction failure marker: {}", marker_err);
                    }

                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                }
            };

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                request_id,
                SessionTxMarkerType::Commit,
                staged_count,
            ) {
                log::warn!("failed to append transaction commit marker: {}", err);
            }

            return Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult {
                    affected_rows: total_affected_rows,
                }),
            ));
        }

        if normalized.starts_with("rollback") {
            let rolled_back = match self.transaction_coordinator.rollback(session_id) {
                Ok(result) => result,
                Err(err) => {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), err));
                }
            };

            if !rolled_back {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    "no active transaction for this session",
                ));
            }

            if let Err(err) = self.append_session_tx_marker(
                session_id,
                request_id,
                SessionTxMarkerType::Rollback,
                0,
            ) {
                log::warn!("failed to append transaction rollback marker: {}", err);
            }

            return Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            ));
        }

        None
    }

    fn commit_staged_queries(
        &mut self,
        request_id: &str,
        staged_queries: &[connector::DataQuery],
    ) -> Result<u64, String> {
        self.validate_staged_queries(request_id, staged_queries)?;

        let mut total_affected_rows = 0u64;
        let write_group_id = TransactionId(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(common::epochabs!() as u64),
        );
        let mut touched_tables = std::collections::HashSet::new();

        for (idx, staged_query) in staged_queries.iter().enumerate() {
            let apply_request_id = format!("{}::apply{}", request_id, idx + 1);
            let response = handle_query_command_in_write_group(
                &apply_request_id,
                staged_query,
                &mut self.catalogs,
                &self.wal,
                &self.node_data_dir,
                &mut self.runtime_indexes,
                write_group_id,
                &mut touched_tables,
            );

            if matches!(response.status, connector::ResponseStatus::Rejected) {
                abort_external_write_group(
                    &self.wal,
                    &self.catalogs,
                    &mut self.runtime_indexes,
                    &touched_tables,
                    write_group_id,
                );

                let error = match response.result {
                    ConnectorResult::Error(message) => message,
                    _ => "staged query apply failed".to_string(),
                };

                return Err(format!(
                    "transaction apply failed at staged statement {} after successful dry-run validation: {}",
                    idx + 1,
                    error
                ));
            }

            if let ConnectorResult::Mutation(mutation) = response.result {
                total_affected_rows = total_affected_rows.saturating_add(mutation.affected_rows);
            }
        }

        if let Err(err) = commit_external_write_group(&self.wal, &touched_tables, write_group_id) {
            abort_external_write_group(
                &self.wal,
                &self.catalogs,
                &mut self.runtime_indexes,
                &touched_tables,
                write_group_id,
            );
            return Err(format!("transaction commit marker append failed: {err}"));
        }

        Ok(total_affected_rows)
    }

    fn validate_staged_queries(
        &self,
        request_id: &str,
        staged_queries: &[connector::DataQuery],
    ) -> Result<(), String> {
        let mut sandbox_catalogs = self.catalogs.clone();
        let mut sandbox_indexes = self.runtime_indexes.clone();
        let sandbox_wal = ConcurrentWalManager::new();

        self.seed_sandbox_wal(&sandbox_wal)?;

        for (idx, staged_query) in staged_queries.iter().enumerate() {
            let dry_run_request_id = format!("{}::dryrun{}", request_id, idx + 1);
            let response = handle_query_command(
                &dry_run_request_id,
                staged_query,
                &mut sandbox_catalogs,
                &sandbox_wal,
                &self.node_data_dir,
                &mut sandbox_indexes,
            );

            if matches!(response.status, connector::ResponseStatus::Rejected) {
                let error = match response.result {
                    ConnectorResult::Error(message) => message,
                    _ => "staged query dry-run failed".to_string(),
                };

                return Err(format!(
                    "transaction validation failed at staged statement {}: {}",
                    idx + 1,
                    error
                ));
            }
        }

        Ok(())
    }

    fn seed_sandbox_wal(&self, sandbox_wal: &ConcurrentWalManager) -> Result<(), String> {
        for catalog in self.catalogs.values() {
            let database_wal_id = catalog.database_id.0.clone();
            self.copy_wal_stream(&database_wal_id, sandbox_wal)?;

            for table_id in catalog.table_ids() {
                self.copy_wal_stream(&table_id, sandbox_wal)?;
            }
        }

        Ok(())
    }

    fn copy_wal_stream(&self, wal_id: &str, sandbox_wal: &ConcurrentWalManager) -> Result<(), String> {
        for record in self.wal.since(wal_id, None) {
            sandbox_wal
                .append(wal_id, record)
                .map_err(|err| format!("failed to seed sandbox WAL for stream '{}': {}", wal_id, err))?;
        }

        Ok(())
    }

    pub fn rollback_session_transaction(&mut self, session_id: &str) -> bool {
        let rolled_back = self
            .transaction_coordinator
            .rollback(session_id)
            .unwrap_or(false);

        if rolled_back {
            if let Err(err) = self.append_session_tx_marker(
                session_id,
                "__disconnect__",
                SessionTxMarkerType::DisconnectRollback,
                0,
            ) {
                log::warn!("failed to append disconnect rollback marker: {}", err);
            }
        }

        rolled_back
    }

    fn append_session_tx_marker(
        &self,
        session_id: &str,
        request_id: &str,
        marker_type: SessionTxMarkerType,
        staged_count: usize,
    ) -> Result<(), String> {
        let wal_id = format!("__session_tx__:{}", session_id);
        let records = self.wal.since(&wal_id, None);
        let next_id = TransactionId(records.last().map(|record| record.id.0 + 1).unwrap_or(1));
        let refid = records.last().map(|record| record.id);
        let timestamp_epoch_ms = common::epochabs!() as u64;

        let encoded = format!(
            "session_id={} request_id={} marker={} staged_count={} ts={}",
            session_id,
            request_id,
            marker_type.as_str(),
            staged_count,
            timestamp_epoch_ms
        )
        .into_bytes();

        self.wal
            .append(
                &wal_id,
                TransactionRecord {
                    id: next_id,
                    groupid: None,
                    refid,
                    timestamp_epoch_ms,
                    actor: UserId::from_username("server"),
                    kind: TransactionKind::MetadataChange,
                    payload: encoded,
                },
            )
            .map_err(|err| format!("failed to append session tx marker: {}", err))
    }

    pub fn shutdown(&self) -> Result<(), ServerAppError> {
        log::info!("server shutting down for node_id={}", self.config.node_id);
        self.wal
            .shutdown_all()
            .map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }

    fn load_catalogs_from_disk(&mut self) -> Result<(), ServerAppError> {
        self.catalogs.clear();

        let files = list_files(&self.node_data_dir)
            .map_err(|e| ServerAppError::Runtime(format!("failed to list data directory: {e}")))?;

        for file in files {
            let ext = file
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or("");

            if ext != FileKind::Catalog.extension() {
                continue;
            }

            let stem = file
                .file_stem()
                .and_then(|value| value.to_str())
                .ok_or_else(|| ServerAppError::Runtime("invalid catalog file name".to_string()))?;

            let catalog = match DatabaseCatalog::load_from_path(&file) {
                Ok(catalog) => catalog,
                Err(_) => {
                    log::warn!(
                        "catalog '{}' could not be deserialized; loading empty catalog from file stem",
                        file.display()
                    );
                    DatabaseCatalog::from_file_stem(stem)
                }
            };

            let table_ids = catalog.table_ids();
            log::info!(
                "loaded catalog '{}' for database='{}' with {} table identifier(s)",
                file.display(),
                catalog.database_id.0,
                table_ids.len()
            );

            self.catalogs.insert(catalog.database_id.0.clone(), catalog);
        }

        Ok(())
    }

    fn replay_catalog_state_from_wal(&mut self) -> Result<(), ServerAppError> {
        for catalog in self.catalogs.values_mut() {
            let wal_id = catalog.database_id.0.clone();
            let applied = catalog
                .replay_entity_construction_from_log(&wal_id, &self.wal)
                .map_err(|msg| ServerAppError::Runtime(msg.to_string()))?;

            if applied > 0 {
                log::info!(
                    "replayed {} catalog transaction(s) for database='{}'",
                    applied,
                    catalog.database_id.0
                );
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum SessionTxMarkerType {
    Begin,
    Commit,
    Rollback,
    DisconnectRollback,
    CommitFailed,
}

impl SessionTxMarkerType {
    fn as_str(self) -> &'static str {
        match self {
            SessionTxMarkerType::Begin => "begin",
            SessionTxMarkerType::Commit => "commit",
            SessionTxMarkerType::Rollback => "rollback",
            SessionTxMarkerType::DisconnectRollback => "disconnect_rollback",
            SessionTxMarkerType::CommitFailed => "commit_failed",
        }
    }
}

fn is_staged_dml_query(query: &connector::DataQuery) -> bool {
    let Ok(parsed) = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) else {
        return false;
    };

    if parsed.len() != 1 {
        return false;
    }

    matches!(
        parsed[0].operation,
        serverlib::SqlOperation::Insert
            | serverlib::SqlOperation::Update
            | serverlib::SqlOperation::Delete
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandKind {
    CreateDatabase,
    Query,
    Schema,
    Mutation,
}

#[derive(Debug, Clone)]
struct CommandInfo {
    kind: CommandKind,
    path: String,
}

fn command_info(command: &ConnectorCommand) -> CommandInfo {
    match command {
        ConnectorCommand::CreateDatabase { database_name } => CommandInfo {
            kind: CommandKind::CreateDatabase,
            path: format!("create_database:{database_name}"),
        },

        ConnectorCommand::Query { query } => CommandInfo {
            kind: CommandKind::Query,
            path: format!("query:{}", query.database_id),
        },

        ConnectorCommand::Schema {
            database_id,
            command,
        } => {
            let path = match command {
                SchemaCommand::CreateTable { table_id, .. } => {
                    format!("schema:create_table:{database_id}:{table_id}")
                }
                SchemaCommand::AlterTable { change } => {
                    format!("schema:alter_table:{database_id}:{}", change.table_id)
                }
                SchemaCommand::DropTable { table_id } => {
                    format!("schema:drop_table:{database_id}:{table_id}")
                }
            };

            CommandInfo {
                kind: CommandKind::Schema,
                path,
            }
        }

        ConnectorCommand::Mutation {
            database_id,
            mutation,
        } => {
            let path = match mutation {
                DataMutation::Insert { table_id, .. } => {
                    format!("mutation:insert:{database_id}:{table_id}")
                }
                DataMutation::Update { table_id, .. } => {
                    format!("mutation:update:{database_id}:{table_id}")
                }
                DataMutation::Delete { table_id, .. } => {
                    format!("mutation:delete:{database_id}:{table_id}")
                }
            };

            CommandInfo {
                kind: CommandKind::Mutation,
                path,
            }
        }
    }
}

fn describe_command_path(command: &ConnectorCommand) -> String {
    command_info(command).path
}

#[cfg(test)]
#[path = "app_test.rs"]
mod tests;
