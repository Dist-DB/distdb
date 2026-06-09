use std::path::PathBuf;

use common::helpers::create_dir;
use serverlib::ConcurrentWalManager;

use crate::core::config::ServerRuntimeConfig;
use crate::engine::wal_probe::{WalProbeResult, run_wal_probe};
use crate::helpers::ServerAppError;

#[derive(Debug)]
pub struct ServerApp {
    config: ServerRuntimeConfig,
    node_data_dir: PathBuf,
    wal: ConcurrentWalManager,
}

impl ServerApp {

    pub fn new(config: ServerRuntimeConfig) -> Result<Self, ServerAppError> {
        let node_config = config.to_node_config();
        node_config
            .validate()
            .map_err(|msg| ServerAppError::InvalidConfig(msg.to_string()))?;

        let node_data_dir = config.data_dir.join(&config.node_id);

        create_dir(&node_data_dir)
            .map_err(|e| ServerAppError::InvalidConfig(format!("cannot create node data directory '{}': {}", node_data_dir.display(), e)))?;

        log::info!("node data directory: {}", node_data_dir.display());

        let wal = ConcurrentWalManager::with_data_dir(node_data_dir.clone());
        log::info!("server app created for node_id={}", config.node_id);
        Ok(Self { config, node_data_dir, wal })
    }

    pub fn bootstrap(&mut self) -> Result<(), ServerAppError> {
        log::info!("server bootstrap complete for node_id={} data_dir={}", self.config.node_id, self.node_data_dir.display());
        Ok(())
    }

    pub fn node_data_dir(&self) -> &PathBuf {
        &self.node_data_dir
    }

    pub fn node_id(&self) -> &str {
        &self.config.node_id
    }

    pub fn run_wal_smoke_test(&self) -> Result<WalProbeResult, ServerAppError> {
        run_wal_probe(&self.wal).map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }

    pub fn shutdown(&self) -> Result<(), ServerAppError> {
        log::info!("server shutting down for node_id={}", self.config.node_id);
        self.wal
            .shutdown_all()
            .map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }

}