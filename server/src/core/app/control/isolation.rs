use std::collections::{HashMap, HashSet};

use connector::ConnectorResponse;
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore, TransactionId, TransactionKind};

use crate::core::app::state::ReadObservation;
use crate::core::app::ServerApp;
use crate::core::mappings::query::{
    commit_external_write_group, handle_query_command, handle_query_command_in_write_group,
};

impl ServerApp {

    pub(super) fn detect_write_write_conflict(
        &self,
        snapshot_epoch_ms: u64,
        staged_queries: &[connector::DataQuery],
    ) -> Option<String> {

        let mut touched_tables = HashSet::new();

        for query in staged_queries {
            let Ok(parsed) = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) else {
                continue;
            };

            for statement in parsed {
                let Some(table_id) = statement.object_name else {
                    continue;
                };

                if matches!(
                    statement.operation,
                    serverlib::SqlOperation::Insert
                        | serverlib::SqlOperation::Update
                        | serverlib::SqlOperation::Delete
                ) {
                    touched_tables.insert(table_id);
                }
            }
        }

        for table_id in touched_tables {
            let has_late_write = self.wal.since(&table_id, None).iter().any(|record| {
                record.timestamp_epoch_ms > snapshot_epoch_ms
                    && matches!(
                        record.kind,
                        TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
                    )
            });

            if has_late_write {
                return Some(format!(
                    "snapshot isolation conflict detected for table '{}'",
                    table_id
                ));
            }
        }

        None

    }

    pub(super) fn detect_predicate_read_conflicts(
        &self,
        session_id: &str,
        snapshot_epoch_ms: u64,
    ) -> Option<String> {

        let observations = self.tx_read_observations_by_session.get(session_id)?;

        for observation in observations {
            let has_conflict = self.wal.since(&observation.table_id, None).iter().any(|record| {
                record.timestamp_epoch_ms > snapshot_epoch_ms
                    && matches!(
                        record.kind,
                        TransactionKind::Insert | TransactionKind::Update | TransactionKind::Delete
                    )
            });

            if has_conflict {
                return Some(format!(
                    "serializable predicate conflict detected for database '{}' table '{}'",
                    observation.database_id, observation.table_id
                ));
            }
        }

        None

    }

    pub(super) fn execute_transactional_read(
        &mut self,
        request_id: &str,
        session_id: &str,
        query: &connector::DataQuery,
    ) -> ConnectorResponse {

        let (snapshot_catalogs, snapshot_runtime_indexes, snapshot_wal) = {
            let Some(snapshot) = self.tx_snapshot_by_session.get(session_id) else {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    "missing transaction WAL snapshot state for this session",
                );
            };

            let snapshot_catalogs = snapshot.catalogs.clone();
            let snapshot_runtime_indexes = snapshot.runtime_indexes.clone();
            let snapshot_wal = ConcurrentWalManager::new();

            if let Err(err) = self.seed_sandbox_wal_from_source(
                &snapshot_catalogs,
                &snapshot.wal,
                &snapshot_wal,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("transactional read snapshot seed failed: {err}"),
                );
            }

            (snapshot_catalogs, snapshot_runtime_indexes, snapshot_wal)
        };

        let mut sandbox_catalogs = snapshot_catalogs.clone();
        let mut sandbox_indexes = snapshot_runtime_indexes.clone();
        let sandbox_wal = ConcurrentWalManager::new();

        if let Err(err) =
            self.seed_sandbox_wal_from_source(&sandbox_catalogs, &snapshot_wal, &sandbox_wal)
        {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("transactional read snapshot seed failed: {err}"),
            );
        }

        let staged_queries = self
            .transaction_coordinator
            .staged_queries(session_id)
            .unwrap_or_default();

        let write_group_id = TransactionId(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos() as u64)
                .unwrap_or(common::epoch_nanos!()),
        );
        let mut touched_tables = HashSet::new();

        for (idx, staged_query) in staged_queries.iter().enumerate() {
            let apply_request_id = format!("{}::txread{}", request_id, idx + 1);
            let response = handle_query_command_in_write_group(
                &apply_request_id,
                staged_query,
                &mut sandbox_catalogs,
                &sandbox_wal,
                &self.node_data_dir,
                &mut sandbox_indexes,
                write_group_id,
                &mut touched_tables,
            );

            if matches!(response.status, connector::ResponseStatus::Rejected) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    "failed to apply staged writes for transactional read",
                );
            }
        }

        if !touched_tables.is_empty()
            && let Err(err) = commit_external_write_group(&sandbox_wal, &touched_tables, write_group_id)
            {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("failed to finalize transactional read snapshot: {}", err),
                );
            }

        let response = handle_query_command(
            request_id,
            query,
            &mut sandbox_catalogs,
            &sandbox_wal,
            &self.node_data_dir,
            &mut sandbox_indexes,
        );

        if matches!(response.status, connector::ResponseStatus::Applied) {
            self.record_simple_read_observation(
                session_id,
                query,
                &sandbox_catalogs,
                &sandbox_wal,
                &sandbox_indexes,
            );
        }

        response

    }

    fn seed_sandbox_wal_from_source(
        &self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        source_wal: &ConcurrentWalManager,
        target_wal: &ConcurrentWalManager,
    ) -> Result<(), String> {

        for catalog in catalogs.values() {

            let database_wal_id = catalog.database_id.0.clone();
            
            for record in source_wal.since(&database_wal_id, None) {
                target_wal
                    .append(&database_wal_id, record)
                    .map_err(|err| {
                        format!(
                            "failed to seed sandbox WAL for stream '{}': {}",
                            database_wal_id, err
                        )
                    })?;
            }

            for table_id in catalog.table_ids() {
                for record in source_wal.since(&table_id, None) {
                    target_wal
                        .append(&table_id, record)
                        .map_err(|err| {
                            format!(
                                "failed to seed sandbox WAL for stream '{}': {}",
                                table_id, err
                            )
                        })?;
                }
            }
        }

        Ok(())

    }

    fn record_simple_read_observation(
        &mut self,
        session_id: &str,
        query: &connector::DataQuery,
        catalogs: &HashMap<String, DatabaseCatalog>,
        wal: &ConcurrentWalManager,
        runtime_indexes: &RuntimeIndexStore,
    ) {

        let Ok(read_plan) = serverlib::parse_select_read_plan_from_statement(&query.sql) else {
            return;
        };

        if read_plan.table_id.is_empty() || !read_plan.joins.is_empty() {
            return;
        }

        let Some(catalog) = catalogs.get(&query.database_id) else {
            return;
        };

        let Some(schema) = catalog.table_schema(&read_plan.table_id) else {
            return;
        };

        let observed_row_ids = serverlib::load_live_rows(wal, &read_plan.table_id, schema)
            .into_iter()
            .filter(|(_, row_map)| {
                serverlib::row_matches_select_condition(
                    row_map,
                    read_plan.where_condition.as_ref(),
                    catalog,
                    wal,
                    runtime_indexes,
                )
            })
            .map(|(row_id, _)| row_id)
            .collect::<HashSet<_>>();

        let observations = self
            .tx_read_observations_by_session
            .entry(session_id.to_string())
            .or_default();

        if let Some(existing) = observations
            .iter_mut()
            .find(|obs| obs.database_id == query.database_id && obs.table_id == read_plan.table_id)
        {
            existing.observed_row_ids.extend(observed_row_ids);
            return;
        }

        observations.push(ReadObservation {
            database_id: query.database_id.clone(),
            table_id: read_plan.table_id,
            observed_row_ids,
        });

    }
    
}
