use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use common::helpers::create_dir;
use serverlib::{ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore};

use crate::core::config::ServerRuntimeConfig;
use crate::core::mappings::query::SessionVariableOverrides;
use crate::core::transaction_coordinator::TransactionCoordinator;
use crate::engine::wal_probe::{WalProbeResult, run_wal_probe};
use crate::helpers::ServerAppError;

#[derive(Debug, Clone)]
pub struct SessionState {
    pub session_id: String,
    pub connection_id: usize,
    pub user_id: String,
    pub last_insert_id: i64,
}

#[derive(Debug, Clone)]
pub struct QuerySessionContext {
    pub session_id: String,
    pub connection_id: usize,
    pub session_user: String,
}

#[derive(Debug)]
pub struct ServerApp {
    pub(super) config: ServerRuntimeConfig,
    pub(super) node_data_dir: PathBuf,
    pub(super) wal: Arc<ConcurrentWalManager>,
    pub(super) catalogs: HashMap<String, DatabaseCatalog>,
    pub(super) runtime_indexes: RuntimeIndexStore,
    pub(super) transaction_coordinator: TransactionCoordinator,
    pub(super) tx_begin_epoch_ms_by_session: HashMap<String, u64>,
    pub(super) tx_snapshot_by_session: HashMap<String, SessionSnapshot>,
    pub(super) tx_read_observations_by_session: HashMap<String, Vec<ReadObservation>>,
    pub(super) session_state_by_id: HashMap<String, SessionState>,
    pub(super) session_variable_overrides_by_id: HashMap<String, SessionVariableOverrides>,
}

#[derive(Debug)]
pub(super) struct SessionSnapshot {
    pub(super) catalogs: HashMap<String, DatabaseCatalog>,
    pub(super) runtime_indexes: RuntimeIndexStore,
    pub(super) wal: ConcurrentWalManager,
}

#[derive(Debug, Clone)]
pub(super) struct ReadObservation {
    pub(super) database_id: String,
    pub(super) table_id: String,
    pub(super) observed_row_ids: HashSet<u64>,
}

const TX_SNAPSHOT_TTL_SECONDS_DEFAULT: u64 = 0;
const TX_SNAPSHOT_MAX_SESSIONS_DEFAULT: usize = 0;

impl ServerApp {

    fn transaction_snapshot_ttl_nanos() -> u64 {

        let ttl_seconds = common::settings::u64_allowing_zero(
            common::settings::TX_SNAPSHOT_TTL_SECONDS,
            TX_SNAPSHOT_TTL_SECONDS_DEFAULT,
        );

        ttl_seconds.saturating_mul(1_000_000_000)

    }

    fn transaction_snapshot_max_sessions() -> usize {
        common::settings::usize_allowing_zero(
            common::settings::TX_SNAPSHOT_MAX_SESSIONS,
            TX_SNAPSHOT_MAX_SESSIONS_DEFAULT,
        )
    }

    pub(super) fn enforce_transaction_snapshot_limits(&mut self, reason: &str) {

        if self.tx_snapshot_by_session.is_empty() {
            return;
        }

        let ttl_nanos = Self::transaction_snapshot_ttl_nanos();
        let max_sessions = Self::transaction_snapshot_max_sessions();

        // Limits are opt-in. By default, do not auto-evict transaction snapshots.
        if ttl_nanos == 0 && max_sessions == 0 {
            return;
        }

        let mut sessions_to_remove = Vec::new();

        if ttl_nanos > 0 {
            let now_nanos = common::epoch_nanos!();

            for (session_id, begin_epoch_nanos) in &self.tx_begin_epoch_ms_by_session {
                let age_nanos = now_nanos.saturating_sub(*begin_epoch_nanos);
                if age_nanos >= ttl_nanos {
                    sessions_to_remove.push(session_id.clone());
                }
            }
        }

        if max_sessions > 0 && self.tx_snapshot_by_session.len() > max_sessions {
            let mut sessions_by_age = self
                .tx_snapshot_by_session
                .keys()
                .map(|session_id| {
                    let begin_epoch = self
                        .tx_begin_epoch_ms_by_session
                        .get(session_id)
                        .copied()
                        .unwrap_or(0);
                    (session_id.clone(), begin_epoch)
                })
                .collect::<Vec<_>>();

            sessions_by_age.sort_by_key(|(_, begin_epoch)| *begin_epoch);

            let overflow = self.tx_snapshot_by_session.len() - max_sessions;
            for (session_id, _) in sessions_by_age.into_iter().take(overflow) {
                if !sessions_to_remove.iter().any(|id| id == &session_id) {
                    sessions_to_remove.push(session_id);
                }
            }
        }

        if sessions_to_remove.is_empty() {
            return;
        }

        for session_id in &sessions_to_remove {
            let _ = self.transaction_coordinator.rollback(session_id);
            self.tx_begin_epoch_ms_by_session.remove(session_id);
            self.tx_snapshot_by_session.remove(session_id);
            self.tx_read_observations_by_session.remove(session_id);
        }

        log::warn!(
            "transaction snapshot cleanup reason={} removed_sessions={} remaining_sessions={} ttl_seconds={} max_sessions={}",
            reason,
            sessions_to_remove.len(),
            self.tx_snapshot_by_session.len(),
            ttl_nanos / 1_000_000_000,
            max_sessions,
        );

    }

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

        let wal = Arc::new(ConcurrentWalManager::with_data_dir(node_data_dir.clone()));
        log::info!("server app created for node_id={}", config.node_id);

        Ok(Self {
            config,
            node_data_dir,
            wal,
            catalogs: HashMap::new(),
            runtime_indexes: RuntimeIndexStore::new(),
            transaction_coordinator: TransactionCoordinator::new(),
            tx_begin_epoch_ms_by_session: HashMap::new(),
            tx_snapshot_by_session: HashMap::new(),
            tx_read_observations_by_session: HashMap::new(),
            session_state_by_id: HashMap::new(),
            session_variable_overrides_by_id: HashMap::new(),
        })

    }

    pub fn bootstrap(&mut self) -> Result<(), ServerAppError> {

        let bootstrap_started_at = Instant::now();

        let load_started_at = Instant::now();
        self.load_catalogs_from_disk()?;
        let load_elapsed_ms = load_started_at.elapsed().as_millis();

        let replay_started_at = Instant::now();
        self.replay_catalog_state_from_wal()?;
        let replay_elapsed_ms = replay_started_at.elapsed().as_millis();

        let index_started_at = Instant::now();
        for catalog in self.catalogs.values_mut() {
            catalog
                .begin_indexing()
                .map_err(|err| ServerAppError::Runtime(format!("failed to enter indexing state: {}", err)))?;
        }

        self.runtime_indexes
            .bootstrap_from_catalogs(&self.catalogs, &self.wal);

        for catalog in self.catalogs.values_mut() {
            catalog
                .complete_indexing()
                .map_err(|err| ServerAppError::Runtime(format!("failed to complete indexing state: {}", err)))?;
        }

        let index_elapsed_ms = index_started_at.elapsed().as_millis();

        let total_elapsed_ms = bootstrap_started_at.elapsed().as_millis();

        let table_count = self
            .catalogs
            .values()
            .map(|catalog| catalog.table_ids().len())
            .sum::<usize>();

        log::info!(
            "server bootstrap complete for node_id={} data_dir={} catalogs={} tables={} load_catalogs_ms={} replay_catalog_wal_ms={} runtime_index_bootstrap_ms={} total_ms={}",
            self.config.node_id,
            self.node_data_dir.display(),
            self.catalogs.len(),
            table_count,
            load_elapsed_ms,
            replay_elapsed_ms,
            index_elapsed_ms,
            total_elapsed_ms,
        );

        Ok(())

    }

    /// Load catalogs and replay their WAL, leaving every table in `Indexing`.
    /// Returns the tables still needing their runtime indexes materialized.
    pub fn bootstrap_catalogs(&mut self) -> Result<Vec<(String, String)>, ServerAppError> {

        let started_at = Instant::now();

        self.load_catalogs_from_disk()?;
        self.replay_catalog_state_from_wal()?;

        for catalog in self.catalogs.values_mut() {
            catalog
                .begin_indexing()
                .map_err(|err| ServerAppError::Runtime(format!("failed to enter indexing state: {}", err)))?;

            // A catalog with no tables has nothing to materialize, so no table
            // completion will ever promote it out of the indexing state.
            if catalog.table_ids().is_empty() {
                catalog
                    .complete_indexing()
                    .map_err(|err| ServerAppError::Runtime(format!("failed to complete indexing state: {}", err)))?;
            }
        }

        let mut pending = Vec::new();

        for (database_id, catalog) in &self.catalogs {
            for table_id in catalog.table_ids() {
                pending.push((database_id.clone(), table_id));
            }
        }

        pending.sort();

        log::info!(
            "server catalog bootstrap complete for node_id={} catalogs={} tables_pending={} elapsed_ms={}",
            self.config.node_id,
            self.catalogs.len(),
            pending.len(),
            started_at.elapsed().as_millis(),
        );

        Ok(pending)

    }

    pub fn wal_handle(&self) -> Arc<ConcurrentWalManager> {
        Arc::clone(&self.wal)
    }

    pub fn catalogs_snapshot(&self) -> HashMap<String, DatabaseCatalog> {
        self.catalogs.clone()
    }

    /// Adopt indexes built off-thread for one table and mark it queryable.
    pub fn install_bootstrapped_table(
        &mut self,
        database_id: &str,
        table_id: &str,
        indexes: RuntimeIndexStore,
    ) -> Result<(), ServerAppError> {

        self.runtime_indexes.merge_from(indexes);

        let Some(catalog) = self.catalogs.get_mut(database_id) else {
            return Err(ServerAppError::Runtime(format!(
                "database '{database_id}' disappeared while bootstrapping table '{table_id}'"
            )));
        };

        catalog
            .complete_table_indexing(table_id)
            .map_err(|err| {
                ServerAppError::Runtime(format!(
                    "failed to mark table '{table_id}' ready: {err}"
                ))
            })?;

        if catalog
            .table_ids()
            .iter()
            .all(|table_id| catalog.table_status(table_id) == Some(serverlib::ObjectStatus::Ready))
            && catalog.status() != serverlib::ObjectStatus::Ready
        {
            catalog
                .transition_status(serverlib::ObjectStatus::Ready)
                .map_err(|err| {
                    ServerAppError::Runtime(format!("failed to mark database ready: {err}"))
                })?;
        }

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

    pub fn init_session(&mut self, session_id: String, connection_id: usize, user_id: String) {
        
        self.session_variable_overrides_by_id
            .entry(session_id.clone())
            .or_default();

        self.session_state_by_id.insert(
            session_id.clone(),
            SessionState {
                session_id,
                connection_id,
                user_id,
                last_insert_id: 0,
            },
        );

    }

    pub fn get_session(&self, session_id: &str) -> Option<SessionState> {
        self.session_state_by_id.get(session_id).cloned()
    }

    pub fn query_session_context(&self, session_id: &str) -> Option<QuerySessionContext> {

        self.get_session(session_id).map(|session| QuerySessionContext {
                session_id: session.session_id,
                connection_id: session.connection_id,
                session_user: format!("{}@localhost", session.user_id),
            })

    }

    pub fn set_last_insert_id(&mut self, session_id: &str, last_insert_id: i64) {
        if let Some(session) = self.session_state_by_id.get_mut(session_id) {
            session.last_insert_id = last_insert_id;
        }
    }

    pub fn take_session_variable_overrides(&mut self, session_id: &str) -> SessionVariableOverrides {
        self.session_variable_overrides_by_id
            .remove(session_id)
            .unwrap_or_default()
    }

    pub fn session_variable_overrides_for(&self, session_id: &str) -> SessionVariableOverrides {
        self.session_variable_overrides_by_id
            .get(session_id)
            .cloned()
            .unwrap_or_default()
    }

    pub fn put_session_variable_overrides(
        &mut self,
        session_id: &str,
        overrides: SessionVariableOverrides,
    ) {
        self.session_variable_overrides_by_id
            .insert(session_id.to_string(), overrides);
    }

    pub fn cleanup_scoped_temporary_tables_for_session(&mut self, session_id: &str) -> usize {

        let normalized_session_id = common::normalize_identifier!(session_id);
        if normalized_session_id.is_empty() {
            return 0;
        }

        let scope_prefix = format!("__scope_proc_{}", normalized_session_id);
        let mut cleaned_tables = 0usize;

        for (database_id, catalog) in self.catalogs.iter_mut() {

            let scoped_table_ids = catalog
                .table_ids()
                .into_iter()
                .filter(|table_id| table_id.starts_with(scope_prefix.as_str()))
                .collect::<Vec<_>>();

            for table_id in scoped_table_ids {

                let stream_id = catalog
                    .entity_wal_stream_id(&table_id)
                    .unwrap_or_else(|| table_id.clone());

                match catalog.drop_table(&table_id) {

                    Ok(()) | Err(serverlib::DatabaseError::TableNotFound) => {}

                    Err(err) => {
                        log::warn!(
                            "failed to drop scoped temporary table during session cleanup session_id={} database={} table={} err={}",
                            normalized_session_id,
                            database_id,
                            table_id,
                            err,
                        );
                        continue;
                    }

                }

                if let Err(err) = self.wal.delete_stream(&stream_id) {
                    log::warn!(
                        "failed to delete scoped temporary stream during session cleanup session_id={} database={} table={} stream={} err={}",
                        normalized_session_id,
                        database_id,
                        table_id,
                        stream_id,
                        err,
                    );
                }

                if stream_id != table_id {
                    let _ = self.wal.delete_stream(&table_id);
                }

                cleaned_tables += 1;

            }

        }

        cleaned_tables

    }

    pub fn run_wal_smoke_test(&self) -> Result<WalProbeResult, ServerAppError> {
        // Keep startup probe isolated so repeated process boots do not mutate
        // persisted WAL streams and trigger out-of-order validation errors.
        let probe_wal = ConcurrentWalManager::new();
        run_wal_probe(&probe_wal).map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }
    
}
