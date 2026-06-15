use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use common::helpers::create_dir;
use serverlib::{ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore};

use crate::core::config::ServerRuntimeConfig;
use crate::core::transaction_coordinator::TransactionCoordinator;
use crate::engine::wal_probe::{WalProbeResult, run_wal_probe};
use crate::helpers::ServerAppError;

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
    
}
