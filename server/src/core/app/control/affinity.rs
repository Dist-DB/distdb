use connector::{ConnectorCommand, ConnectorRequest, ConnectorResult};
use serverlib::{DatabaseCatalog, DatabaseId, ObjectStatus};

use crate::core::app::ServerApp;

impl ServerApp {

    pub(super) fn resolve_catalog_key(&self, database_id: &str) -> Option<String> {

        if self.catalogs.contains_key(database_id) {
            return Some(database_id.to_string());
        }

        let normalized = DatabaseId::from_database_name(database_id).ok()?.0;
        if self.catalogs.contains_key(&normalized) {
            return Some(normalized);
        }

        None

    }

    pub fn ensure_affinity_catalog_exists(&mut self, database_id: &str) -> Result<String, String> {

        if let Some(key) = self.resolve_catalog_key(database_id) {
            return Ok(key);
        }

        // Affinity sync callers may pass an already-normalized remote database id.
        // Persist the incoming identifier as-is to avoid re-hashing ids cross-node.
        let key = database_id.to_string();

        let mut catalog = DatabaseCatalog::new(DatabaseId(key.clone()));
        catalog.set_database_name(&key);
        catalog
            .transition_status(ObjectStatus::Ready)
            .map_err(|err| format!("failed preparing replicated catalog '{}': {}", key, err))?;
        catalog
            .save_in_directory(&self.node_data_dir)
            .map_err(|err| format!("failed persisting replicated catalog '{}' to disk: {:?}", key, err))?;
        self.catalogs.insert(key.clone(), catalog);

        Ok(key)

    }

    pub fn resolve_catalog_wal_stream_for_database(&self, database_id: &str) -> String {

        if let Some(key) = self.resolve_catalog_key(database_id) {
            return key;
        }

        if let Some(default_key) = self.resolve_catalog_key("main") {
            return default_key;
        }

        DatabaseId::from_database_name("main")
            .map(|id| id.0)
            .unwrap_or_else(|_| "main".to_string())

    }

    pub fn set_affinity_catalog_database_name(&mut self, database_id: &str, name: &str) -> Result<(), String> {

        if name.is_empty() {
            return Ok(());
        }
        let Some(key) = self.resolve_catalog_key(database_id) else {
            return Ok(());
        };
        let Some(catalog) = self.catalogs.get_mut(&key) else {
            return Ok(());
        };
        catalog.set_database_name(name);
        catalog
            .save_in_directory(&self.node_data_dir)
            .map_err(|err| format!("failed persisting catalog name update for '{}': {:?}", key, err))

    }

    pub(super) fn begin_affinity_sync_lock(&mut self, database_id: &str) -> Result<(), String> {

        let database_key = self.ensure_affinity_catalog_exists(database_id)?;
        let catalog = self
            .catalogs
            .get_mut(&database_key)
            .ok_or_else(|| format!("database '{}' not found", database_key))?;

        if catalog.status() == ObjectStatus::Lock {
            return Err(format!(
                "database '{}' is already locked by another operation",
                database_key
            ));
        }

        catalog
            .transition_status(ObjectStatus::Lock)
            .map_err(|err| format!("failed locking database '{}' for affinity sync: {}", database_key, err))

    }

    pub(super) fn finish_affinity_sync_lock(&mut self, database_id: &str) -> Result<(), String> {

        let Some(database_key) = self.resolve_catalog_key(database_id) else {
            return Err(format!("database '{}' not found", database_id));
        };

        let catalog = self
            .catalogs
            .get_mut(&database_key)
            .ok_or_else(|| format!("database '{}' not found", database_key))?;

        if catalog.status() == ObjectStatus::Ready {
            return Ok(());
        }

        catalog
            .transition_status(ObjectStatus::Ready)
            .map_err(|err| format!("failed returning database '{}' to ready: {}", database_key, err))

    }

    pub fn apply_affinity_schema_definitions(
        &mut self,
        database_id: &str,
        schema_definitions: &[String],
    ) -> Result<(), String> {

        self.begin_affinity_sync_lock(database_id)?;

        let apply_result = (|| {

            for (idx, sql) in schema_definitions.iter().enumerate() {

                let request = ConnectorRequest {
                    request_id: format!("replication-schema-apply-{}-{}", database_id, idx),
                    command: ConnectorCommand::Query {
                        query: connector::DataQuery {
                            database_id: database_id.to_string(),
                            sql: sql.clone(),
                        },
                    },
                };

                let response = self.handle_connector_request_for_session(
                    &request,
                    "__affinity_replication_schema_sync__",
                );

                if matches!(
                    response.status,
                    connector::ResponseStatus::Applied | connector::ResponseStatus::Accepted
                ) {
                    continue;
                }

                let error_message = match response.result {
                    ConnectorResult::Error(message) => message,
                    _ => "schema apply rejected without error message".to_string(),
                };

                if error_message.to_ascii_lowercase().contains("already exists") {
                    continue;
                }

                return Err(format!(
                    "failed applying schema definition '{}' to database '{}': {}",
                    sql, database_id, error_message
                ));

            }

            Ok(())

        })();

        let save_result = if apply_result.is_ok() {

            if let Some(key) = self.resolve_catalog_key(database_id) {

                if let Some(catalog) = self.catalogs.get(&key) {

                    catalog
                        .save_in_directory(&self.node_data_dir)
                        .map_err(|err| format!("failed persisting catalog '{}' after schema sync: {:?}", key, err))

                } else {

                    Ok(())

                }

            } else {

                Ok(())

            }

        } else {

            Ok(())

        };

        let release_result = self.finish_affinity_sync_lock(database_id);

        match (apply_result, save_result, release_result) {

            (Err(apply_err), _, Err(release_err)) => {
                Err(format!("{}; cleanup failed: {}", apply_err, release_err))
            }

            (Err(apply_err), _, Ok(())) => Err(apply_err),

            (Ok(()), Err(save_err), _) => Err(save_err),

            (Ok(()), Ok(()), Err(release_err)) => Err(release_err),

            (Ok(()), Ok(()), Ok(())) => Ok(()),
            
        }

    }

}
