use common::helpers::format::FileKind;
use common::helpers::list_files;
use serverlib::DatabaseCatalog;
use std::time::Instant;

use crate::core::app::ServerApp;
use crate::helpers::ServerAppError;

const DEFAULT_SYSTEM_DATABASE: &str = "main";

impl ServerApp {

    pub fn shutdown(&self) -> Result<(), ServerAppError> {

        log::info!("server shutting down for node_id={}", self.config.node_id);
        self.wal
            .shutdown_all()
            .map_err(|msg| ServerAppError::Runtime(msg.to_string()))

    }

    pub(super) fn load_catalogs_from_disk(&mut self) -> Result<(), ServerAppError> {

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
                "loaded catalog '{}' for database='{}' with {} table identifier(s) before WAL replay",
                file.display(),
                catalog.database_id.0,
                table_ids.len()
            );

            self.catalogs.insert(catalog.database_id.0.clone(), catalog);
            
        }

        if !self.catalogs.contains_key(DEFAULT_SYSTEM_DATABASE) {
            let catalog = DatabaseCatalog::create_new_database(
                DEFAULT_SYSTEM_DATABASE,
                &self.node_data_dir,
            )
            .map_err(|err| {
                ServerAppError::Runtime(format!(
                    "failed to initialize default system catalog '{}': {}",
                    DEFAULT_SYSTEM_DATABASE,
                    err
                ))
            })?;

            log::info!(
                "initialized default system catalog database='{}'",
                DEFAULT_SYSTEM_DATABASE,
            );

            self.catalogs.insert(catalog.database_id.0.clone(), catalog);
        }

        Ok(())

    }

    pub(super) fn replay_catalog_state_from_wal(&mut self) -> Result<(), ServerAppError> {

        let replay_started_at = Instant::now();
        let mut replayed_transactions = 0usize;

        for catalog in self.catalogs.values_mut() {

            let catalog_started_at = Instant::now();

            let wal_id = catalog.database_id.0.clone();
            let applied = catalog
                .replay_entity_construction_from_log(&wal_id, &self.wal)
                .map_err(|msg| ServerAppError::Runtime(msg.to_string()))?;

            replayed_transactions += applied;

            let catalog_elapsed_ms = catalog_started_at.elapsed().as_millis();

            if applied > 0 {
                log::info!(
                    "replayed {} catalog transaction(s) for database='{}' elapsed_ms={} tables_after_replay={}",
                    applied,
                    catalog.database_id.0,
                    catalog_elapsed_ms,
                    catalog.table_ids().len(),
                );
            } else if catalog_elapsed_ms >= 1_000 {
                log::info!(
                    "catalog replay scanned zero transactions for database='{}' elapsed_ms={} tables_after_replay={}",
                    catalog.database_id.0,
                    catalog_elapsed_ms,
                    catalog.table_ids().len(),
                );
            }

        }

        let replayed_table_count = self
            .catalogs
            .values()
            .map(|catalog| catalog.table_ids().len())
            .sum::<usize>();

        log::info!(
            "catalog replay complete catalogs={} tables={} transactions={} elapsed_ms={}",
            self.catalogs.len(),
            replayed_table_count,
            replayed_transactions,
            replay_started_at.elapsed().as_millis(),
        );

        Ok(())

    }
    
}
