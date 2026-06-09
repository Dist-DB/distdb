use std::collections::HashMap;
use std::path::PathBuf;

use common::helpers::{create_dir, list_files};
use common::helpers::format::FileKind;
use serverlib::{ConcurrentWalManager, DatabaseCatalog};

use crate::core::config::ServerRuntimeConfig;
use crate::engine::wal_probe::{WalProbeResult, run_wal_probe};
use crate::helpers::ServerAppError;

#[derive(Debug)]
pub struct ServerApp {
    config: ServerRuntimeConfig,
    node_data_dir: PathBuf,
    wal: ConcurrentWalManager,
    catalogs: HashMap<String, DatabaseCatalog>,
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
        Ok(Self {
            config,
            node_data_dir,
            wal,
            catalogs: HashMap::new(),
        })
    }

    pub fn bootstrap(&mut self) -> Result<(), ServerAppError> {
        self.load_catalogs_from_disk()?;
        log::info!("server bootstrap complete for node_id={} data_dir={}", self.config.node_id, self.node_data_dir.display());
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
        run_wal_probe(&self.wal).map_err(|msg| ServerAppError::Runtime(msg.to_string()))
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

            self.catalogs
                .insert(catalog.database_id.0.clone(), catalog);
        }

        Ok(())
    }

}