use std::time::Instant;
use std::collections::HashSet;

use connector::{ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, MutationResult};
use serverlib::{ConcurrentWalManager, DatabaseCatalog, TransactionId};

use crate::core::app::helpers::{
    CommandKind, SessionTxMarkerType, command_info, is_staged_dml_query, is_transactional_read_query,
};
use crate::core::app::state::SessionSnapshot;
use crate::core::app::ServerApp;
use crate::core::mappings::query::{
    abort_external_write_group, commit_external_write_group, handle_query_command,
    handle_query_command_in_write_group,
};
use crate::core::transaction_coordinator::QueryRoutingDecision;

impl ServerApp {

    fn staged_query_validation_plan(
        staged_queries: &[connector::DataQuery],
    ) -> Option<(HashSet<String>, bool)> {
        let mut table_ids = HashSet::new();
        let mut insert_only = true;

        for query in staged_queries {
            let parsed = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id).ok()?;
            if parsed.is_empty() {
                return None;
            }

            for statement in parsed {
                let table_id = statement.object_name?;
                table_ids.insert(table_id);

                if !matches!(statement.operation, serverlib::SqlOperation::Insert) {
                    insert_only = false;
                }
            }
        }

        if table_ids.is_empty() {
            return None;
        }

        // For pure INSERT dry-runs, runtime index state is sufficient for duplicate checks.
        // Skipping WAL seed avoids replaying full table snapshots into the sandbox.
        Some((table_ids, insert_only))
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

        // log::info!(
        //     "connector request dispatch request_id={} path={}",
        //     request.request_id,
        //     command_path
        // );

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
                            Ok(QueryRoutingDecision::ExecuteImmediately) => {
                                let session_state = self.get_session(session_id);
                                let (connection_id, session_user) = session_state
                                    .map(|s| (s.connection_id, Some(format!("{}@localhost", s.user_id))))
                                    .unwrap_or((0, None));

                                let response = handle_query_command(
                                    &request.request_id,
                                    query,
                                    &mut self.catalogs,
                                    &self.wal,
                                    &self.node_data_dir,
                                    &mut self.runtime_indexes,
                                    session_id,
                                    connection_id,
                                    session_user,
                                );

                                // Update session last_insert_id if INSERT just happened
                                use crate::core::mappings::query::get_and_clear_last_insert_id;
                                if let Some(last_insert_id) = get_and_clear_last_insert_id() {
                                    self.set_last_insert_id(session_id, last_insert_id);
                                }

                                response
                            },
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
                // log::info!(
                //     "connector request completed request_id={} path={} status={:?}",
                //     request.request_id,
                //     command_path,
                //     response.status
                // );
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
                .insert(session_id.to_string(), common::epoch_nanos!());

            let is_lightweight_import_begin = normalized.contains("distdb_import");

            if !is_lightweight_import_begin {
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
            let has_snapshot = self.tx_snapshot_by_session.contains_key(session_id);
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
                    if has_snapshot {
                        if let Err(restore_err) = self
                            .transaction_coordinator
                            .restore_after_failed_commit(session_id, staged_queries)
                        {
                            log::error!(
                                "failed to restore staged transaction after commit error: {}",
                                restore_err
                            );
                        }
                    } else {
                        if let Err(rollback_err) = self.transaction_coordinator.rollback(session_id) {
                            log::error!(
                                "failed to rollback snapshotless transaction after commit error: {}",
                                rollback_err
                            );
                        }

                        self.tx_begin_epoch_ms_by_session.remove(session_id);
                        self.tx_snapshot_by_session.remove(session_id);
                        self.tx_read_observations_by_session.remove(session_id);
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
        let commit_start = Instant::now();
        let validation_start = Instant::now();

        if snapshot_epoch_ms > 0 {
            if let Some(conflict) = self.detect_write_write_conflict(snapshot_epoch_ms, staged_queries) {
                return Err(conflict);
            }

            if let Some(conflict) = self.detect_predicate_read_conflicts(session_id, snapshot_epoch_ms) {
                return Err(conflict);
            }
        }

        self.validate_staged_queries(request_id, session_id, staged_queries)?;
        let validation_ms = validation_start.elapsed().as_millis() as u64;
        let apply_start = Instant::now();

        let session_state = self.get_session(session_id);
        let (connection_id, session_user) = session_state
            .map(|s| (s.connection_id, Some(format!("{}@localhost", s.user_id))))
            .unwrap_or((0, None));

        let mut total_affected_rows = 0u64;
        let write_group_id = TransactionId(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(common::epoch_nanos!()),
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
                session_id,
                connection_id,
                session_user.clone(),
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
        let apply_ms = apply_start.elapsed().as_millis() as u64;

        let commit_marker_start = Instant::now();

        if let Err(err) = commit_external_write_group(&self.wal, &touched_tables, write_group_id) {
            abort_external_write_group(
                &self.wal,
                &self.catalogs,
                &mut self.runtime_indexes,
                &touched_tables,
                write_group_id,
            );
            let total_ms = commit_start.elapsed().as_millis() as u64;
            log::info!(
                "transaction commit timing request_id={} session_id={} staged_queries={} affected_rows={} validation_ms={} apply_ms={} commit_marker_ms={} total_ms={} status=failed",
                request_id,
                session_id,
                staged_queries.len(),
                total_affected_rows,
                validation_ms,
                apply_ms,
                commit_marker_start.elapsed().as_millis() as u64,
                total_ms,
            );
            return Err(format!("transaction commit marker append failed: {err}"));
        }

        let total_ms = commit_start.elapsed().as_millis() as u64;
        log::info!(
            "transaction commit timing request_id={} session_id={} staged_queries={} affected_rows={} validation_ms={} apply_ms={} commit_marker_ms={} total_ms={} status=ok",
            request_id,
            session_id,
            staged_queries.len(),
            total_affected_rows,
            validation_ms,
            apply_ms,
            commit_marker_start.elapsed().as_millis() as u64,
            total_ms,
        );

        Ok(total_affected_rows)
    }

    fn validate_staged_queries(
        &self,
        request_id: &str,
        session_id: &str,
        staged_queries: &[connector::DataQuery],
    ) -> Result<(), String> {
        let validation_start = Instant::now();

        let validation_plan = Self::staged_query_validation_plan(staged_queries);

        let snapshot_start = Instant::now();

        let mut setup_mode = "fallback_full";
        let mut setup_table_count = 0usize;
        let index_clone_ms;
        let wal_seed_ms;

        let (mut sandbox_catalogs, snapshot_runtime_indexes, snapshot_wal) =
            if let Some(snapshot) = self.tx_snapshot_by_session.get(session_id) {
                (
                    snapshot.catalogs.clone(),
                    snapshot.runtime_indexes.clone(),
                    Some(&snapshot.wal),
                )
            } else if let Some((table_ids, insert_only)) = &validation_plan {
                if !insert_only {
                    return Err("missing transaction snapshot for validation".to_string());
                }

                setup_mode = "current_state_insert_only";
                setup_table_count = table_ids.len();
                (
                    self.catalogs.clone(),
                    self.runtime_indexes.clone(),
                    None,
                )
            } else {
                return Err("missing transaction snapshot for validation".to_string());
            };

        let sandbox_wal = ConcurrentWalManager::new();

        let mut sandbox_indexes = if let Some((table_ids, skip_wal_seed)) = validation_plan {
            setup_table_count = table_ids.len();

            let index_clone_start = Instant::now();
            let scoped_indexes = snapshot_runtime_indexes.clone_for_tables(&sandbox_catalogs, &table_ids);
            index_clone_ms = index_clone_start.elapsed().as_millis() as u64;

            if skip_wal_seed {
                if setup_mode != "current_state_insert_only" {
                    setup_mode = "table_scoped_insert_only";
                }
                wal_seed_ms = 0;
            } else {
                setup_mode = "table_scoped";
                let wal_seed_start = Instant::now();
                let Some(source_wal) = snapshot_wal else {
                    return Err("missing transaction snapshot for validation".to_string());
                };
                self.seed_sandbox_wal_from_source_for_tables(&sandbox_catalogs, source_wal, &sandbox_wal, &table_ids)
                    .map_err(|err| format!("failed to seed validation WAL snapshot: {}", err))?;
                wal_seed_ms = wal_seed_start.elapsed().as_millis() as u64;
            }

            scoped_indexes
        } else {
            let wal_seed_start = Instant::now();
            let Some(source_wal) = snapshot_wal else {
                return Err("missing transaction snapshot for validation".to_string());
            };
            self.seed_sandbox_wal_from_source(&sandbox_catalogs, source_wal, &sandbox_wal)
                .map_err(|err| format!("failed to seed validation WAL snapshot: {}", err))?;
            wal_seed_ms = wal_seed_start.elapsed().as_millis() as u64;

            let index_clone_start = Instant::now();
            let cloned = snapshot_runtime_indexes.clone();
            index_clone_ms = index_clone_start.elapsed().as_millis() as u64;

            cloned
        };

        let snapshot_setup_ms = snapshot_start.elapsed().as_millis() as u64;

        log::info!(
            "transaction validation setup request_id={} session_id={} mode={} tables={} index_clone_ms={} wal_seed_ms={} snapshot_setup_ms={}",
            request_id,
            session_id,
            setup_mode,
            setup_table_count,
            index_clone_ms,
            wal_seed_ms,
            snapshot_setup_ms,
        );

        let session_state = self.get_session(session_id);
        let (connection_id, session_user) = session_state
            .map(|s| (s.connection_id, Some(format!("{}@localhost", s.user_id))))
            .unwrap_or((0, None));

        let mut dry_run_total_ms = 0u64;

        for (idx, staged_query) in staged_queries.iter().enumerate() {
            let staged_query_start = Instant::now();
            let dry_run_request_id = format!("{}::dryrun{}", request_id, idx + 1);
            let response = handle_query_command(
                &dry_run_request_id,
                staged_query,
                &mut sandbox_catalogs,
                &sandbox_wal,
                &self.node_data_dir,
                &mut sandbox_indexes,
                session_id,
                connection_id,
                session_user.clone(),
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

            dry_run_total_ms = dry_run_total_ms.saturating_add(staged_query_start.elapsed().as_millis() as u64);
        }

        let total_ms = validation_start.elapsed().as_millis() as u64;

        log::info!(
            "transaction validation timing request_id={} session_id={} staged_queries={} snapshot_setup_ms={} dry_run_ms={} total_ms={}",
            request_id,
            session_id,
            staged_queries.len(),
            snapshot_setup_ms,
            dry_run_total_ms,
            total_ms,
        );

        Ok(())

    }
    
}
