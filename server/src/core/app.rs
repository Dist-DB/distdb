use std::collections::HashMap;
use std::path::PathBuf;

use common::helpers::{create_dir, list_files};
use common::helpers::format::FileKind;
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult,
    MutationResult, QueryResult,
};
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
        self.replay_catalog_schemas_from_wal()?;
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

    pub fn handle_connector_request(&mut self, request: &ConnectorRequest) -> ConnectorResponse {
        match &request.command {
            ConnectorCommand::CreateDatabase { database_name } => {
                match DatabaseCatalog::create_new_database(database_name, &self.node_data_dir) {
                    Ok(catalog) => {
                        self.catalogs
                            .insert(catalog.database_id.0.clone(), catalog);

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
            ConnectorCommand::Query { query } => {
                match serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) {
                    Ok(parsed) => {
                        let rows = parsed
                            .into_iter()
                            .map(|statement| {
                                vec![
                                    statement.database_id.into_bytes(),
                                    format!("{:?}", statement.directive).into_bytes(),
                                    format!("{:?}", statement.operation).into_bytes(),
                                    statement
                                        .object_name
                                        .unwrap_or_default()
                                        .into_bytes(),
                                    statement.sql.into_bytes(),
                                ]
                            })
                            .collect::<Vec<_>>();

                        ConnectorResponse::applied(
                            request.request_id.clone(),
                            ConnectorResult::Query(QueryResult {
                                columns: vec![
                                    "database_id".to_string(),
                                    "directive".to_string(),
                                    "operation".to_string(),
                                    "object_name".to_string(),
                                    "statement".to_string(),
                                ],
                                rows,
                            }),
                        )
                    }
                    Err(err) => ConnectorResponse::rejected(
                        request.request_id.clone(),
                        format!("sql parse failed: {err}"),
                    ),
                }
            }
            ConnectorCommand::Schema { .. } => ConnectorResponse::rejected(
                request.request_id.clone(),
                "schema command execution is not wired yet",
            ),
            ConnectorCommand::Mutation { .. } => ConnectorResponse::rejected(
                request.request_id.clone(),
                "mutation command execution is not wired yet",
            ),
        }
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

    fn replay_catalog_schemas_from_wal(&mut self) -> Result<(), ServerAppError> {

        for catalog in self.catalogs.values_mut() {
            let wal_id = catalog.database_id.0.clone();
            let applied = catalog
                .replay_schema_from_log(&wal_id, &self.wal)
                .map_err(|msg| ServerAppError::Runtime(msg.to_string()))?;

            if applied > 0 {
                log::info!(
                    "replayed {} schema change transaction(s) for database='{}'",
                    applied,
                    catalog.database_id.0
                );
            }
        }

        Ok(())

    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use connector::{ConnectorCommand, ConnectorRequest, ConnectorResult, ResponseStatus};
    use serverlib::{FieldDef, FieldType, SchemaChangePayload, TableSchema, TransactionId, TransactionKind, TransactionRecord, UserId};
    use serverlib::engine::transaction::TransactionLog;

    #[test]
    fn bootstrap_replays_latest_schema_from_wal() {
        
        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-bootstrap-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let database_name = "schema_bootstrap";
        let mut catalog = DatabaseCatalog::create_empty_from_name(database_name)
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "name".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: false,
                    default_value: None,
                }]),
            )
            .expect("base table should register");

        catalog
            .save_in_directory(&app.node_data_dir)
            .expect("catalog should be persisted");

        let schema = TableSchema::new(vec![FieldDef {
            seqno: 1,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: true,
            default_value: None,
        }]);

        let payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 2,
            schema: schema.clone(),
        };

        app.wal
            .append(
                &catalog.database_id.0,
                TransactionRecord {
                    id: TransactionId(1),
                    refid: None,
                    timestamp_epoch_ms: 1,
                    actor: UserId::from_username("bootstrap-tester"),
                    kind: TransactionKind::SchemaChange,
                    payload: payload.encode().expect("schema payload should encode"),
                },
            )
            .expect("schema transaction should append");

        app.bootstrap().expect("bootstrap should replay schemas");

        let loaded = app
            .catalogs()
            .get(&catalog.database_id.0)
            .expect("catalog should be loaded");

        assert_eq!(loaded.table_schema("users"), Some(&schema));
        assert_eq!(loaded.table_schema_revision("users"), Some(2));
        assert!(loaded.index("users:email").is_some());
        
    }

    #[test]
    fn query_requests_are_parsed_server_side_into_directive_rows() {
        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-query-routing-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let request = ConnectorRequest::new(
            "req-query-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select * from users; update users set active=1 where id=7".to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);

        assert_eq!(response.request_id, "req-query-1");
        assert_eq!(response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = response.result else {
            panic!("expected query result");
        };

        assert_eq!(
            result.columns,
            vec!["database_id", "directive", "operation", "object_name", "statement"]
        );
        assert_eq!(result.rows.len(), 2);

        let first_row = result.rows.first().expect("first row should exist");
        assert_eq!(String::from_utf8_lossy(&first_row[2]), "Select");
        assert_eq!(String::from_utf8_lossy(&first_row[3]), "users");
    }
    
}