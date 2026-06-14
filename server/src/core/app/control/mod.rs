use connector::{ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, MutationResult};
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    ConcurrentWalManager, DatabaseCatalog, TransactionId, TransactionKind, TransactionRecord,
    UserId,
};

use crate::core::app::state::SessionSnapshot;
use crate::core::app::ServerApp;
use crate::core::mappings::query::{
    abort_external_write_group, commit_external_write_group, handle_query_command,
    handle_query_command_in_write_group,
};
use crate::core::transaction_coordinator::QueryRoutingDecision;
use super::helpers::{
    CommandKind, SessionTxMarkerType, command_info, is_staged_dml_query,
    is_transactional_read_query,
};

mod isolation;

impl ServerApp {

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
            }

            CommandKind::Query => {
                let ConnectorCommand::Query { query } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                if let Some(response) =
                    self.handle_transaction_control_query(&request.request_id, session_id, query)
                {
                    response
                } else {
                    let transaction_active = self
                        .transaction_coordinator
                        .is_active(session_id)
                        .unwrap_or(false);

                    if transaction_active && is_transactional_read_query(query) {
                        self.execute_transactional_read(&request.request_id, session_id, query)
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
                }
            }

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
            }

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

            self.tx_begin_epoch_ms_by_session
                .insert(session_id.to_string(), common::epochabs!() as u64);

            let snapshot_wal = ConcurrentWalManager::new();
            if let Err(err) = self.seed_sandbox_wal(&snapshot_wal) {
                let _ = self.transaction_coordinator.rollback(session_id);
                self.tx_begin_epoch_ms_by_session.remove(session_id);
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("failed to capture transaction snapshot: {}", err),
                ));
            }

            self.tx_snapshot_by_session.insert(
                session_id.to_string(),
                SessionSnapshot {
                    catalogs: self.catalogs.clone(),
                    runtime_indexes: self.runtime_indexes.clone(),
                    wal: snapshot_wal,
                },
            );
            self.tx_read_observations_by_session
                .insert(session_id.to_string(), Vec::new());

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
            let snapshot_epoch_ms = self
                .tx_begin_epoch_ms_by_session
                .get(session_id)
                .copied()
                .unwrap_or(0);

            let total_affected_rows = match self.commit_staged_queries(
                request_id,
                session_id,
                &staged_queries,
                snapshot_epoch_ms,
            ) {
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

            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);

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

            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);

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
        session_id: &str,
        staged_queries: &[connector::DataQuery],
        snapshot_epoch_ms: u64,
    ) -> Result<u64, String> {

        if snapshot_epoch_ms > 0 {
            if let Some(conflict) =
                self.detect_write_write_conflict(snapshot_epoch_ms, staged_queries)
            {
                return Err(conflict);
            }

            if let Some(conflict) =
                self.detect_predicate_read_conflicts(session_id, snapshot_epoch_ms)
            {
                return Err(conflict);
            }
        }

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
            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);

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

}
