use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
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
    pub(super) wal: ConcurrentWalManager,
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

    pub fn run_wal_smoke_test(&self) -> Result<WalProbeResult, ServerAppError> {
        // Keep startup probe isolated so repeated process boots do not mutate
        // persisted WAL streams and trigger out-of-order validation errors.
        let probe_wal = ConcurrentWalManager::new();
        run_wal_probe(&probe_wal).map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }
    
}
