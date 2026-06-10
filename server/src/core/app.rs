use std::collections::HashMap;
use std::path::PathBuf;

use common::helpers::{create_dir, list_files};
use common::helpers::format::FileKind;
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult,
    DataMutation, MutationResult, SchemaCommand,
};
use serverlib::{ConcurrentWalManager, DatabaseCatalog};

use crate::core::config::ServerRuntimeConfig;
use crate::core::mappings::query::handle_query_command;
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
        self.replay_catalog_state_from_wal()?;
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
        // Keep startup probe isolated so repeated process boots do not mutate
        // persisted WAL streams and trigger out-of-order validation errors.
        let probe_wal = ConcurrentWalManager::new();
        run_wal_probe(&probe_wal).map_err(|msg| ServerAppError::Runtime(msg.to_string()))
    }

    pub fn handle_connector_request(&mut self, request: &ConnectorRequest) -> ConnectorResponse {
        let command_path = describe_command_path(&request.command);
        log::info!(
            "connector request dispatch request_id={} path={}",
            request.request_id,
            command_path
        );

        let response = match &request.command {
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
            ConnectorCommand::Query { query } => handle_query_command(
                &request.request_id,
                query,
                &mut self.catalogs,
                &self.wal,
                &self.node_data_dir,
            ),
            ConnectorCommand::Schema { .. } => ConnectorResponse::rejected(
                request.request_id.clone(),
                "schema command execution is not wired yet",
            ),
            ConnectorCommand::Mutation { .. } => ConnectorResponse::rejected(
                request.request_id.clone(),
                "mutation command execution is not wired yet",
            ),
        };

        match &response.result {
            ConnectorResult::Error(message) => {
                log::warn!(
                    "connector request completed request_id={} path={} status={:?} error={}"
                    ,
                    request.request_id,
                    command_path,
                    response.status,
                    message
                );
            }
            _ => {
                log::info!(
                    "connector request completed request_id={} path={} status={:?}",
                    request.request_id,
                    command_path,
                    response.status
                );
            }
        }

        response
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

    fn replay_catalog_state_from_wal(&mut self) -> Result<(), ServerAppError> {

        for catalog in self.catalogs.values_mut() {
            let wal_id = catalog.database_id.0.clone();
            let applied = catalog
                .replay_entity_construction_from_log(&wal_id, &self.wal)
                .map_err(|msg| ServerAppError::Runtime(msg.to_string()))?;

            if applied > 0 {
                log::info!(
                    "replayed {} catalog transaction(s) for database='{}'",
                    applied,
                    catalog.database_id.0
                );
            }
        }

        Ok(())

    }

}

fn describe_command_path(command: &ConnectorCommand) -> String {
    match command {
        ConnectorCommand::CreateDatabase { database_name } => {
            format!("create_database:{}", database_name)
        }
        ConnectorCommand::Query { query } => {
            format!("query:{}", query.database_id)
        }
        ConnectorCommand::Schema {
            database_id,
            command,
        } => match command {
            SchemaCommand::CreateTable { table_id, .. } => {
                format!("schema:create_table:{}:{}", database_id, table_id)
            }
            SchemaCommand::AlterTable { change } => {
                format!("schema:alter_table:{}:{}", database_id, change.table_id)
            }
            SchemaCommand::DropTable { table_id } => {
                format!("schema:drop_table:{}:{}", database_id, table_id)
            }
        },
        ConnectorCommand::Mutation {
            database_id,
            mutation,
        } => match mutation {
            DataMutation::Insert { table_id, .. } => {
                format!("mutation:insert:{}:{}", database_id, table_id)
            }
            DataMutation::Update { table_id, .. } => {
                format!("mutation:update:{}:{}", database_id, table_id)
            }
            DataMutation::Delete { table_id, .. } => {
                format!("mutation:delete:{}:{}", database_id, table_id)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::core::mappings::perf::QueryTimingThresholds;
    use connector::{
        ConnectorClient, ConnectorCommand, ConnectorError, ConnectorRequest,
        ConnectorResult, ConnectorTransport, ResponseStatus,
    };
    use serverlib::{
        EntityMetadata, EntityMetadataPayload, FieldDef, FieldIndex, FieldType,
        SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload, SqlObjectKind,
        TableSchema, TransactionId, TransactionKind, TransactionRecord, UserId,
    };
    use serverlib::engine::database::transaction::TransactionLog;

    #[derive(Debug)]
    struct InProcessServerTransport {
        app: RefCell<ServerApp>,
    }

    impl ConnectorTransport for InProcessServerTransport {
        fn request(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError> {
            Ok(self.app.borrow_mut().handle_connector_request(request))
        }
    }

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
                    indexed: FieldIndex::None,
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
            indexed: FieldIndex::Indexed,
            default_value: None,
        }]);

        let payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 2,
            schema_epoch: 2,
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
    fn bootstrap_replays_sql_definition_and_metadata_from_wal() {
        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-bootstrap-sql-definition-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let database_name = "sql_definition_bootstrap";
        let mut catalog = DatabaseCatalog::create_empty_from_name(database_name)
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                }]),
            )
            .expect("base table should register");

        catalog
            .save_in_directory(&app.node_data_dir)
            .expect("catalog should be persisted");

        let wal_id = catalog.database_id.0.clone();
        let actor = UserId::from_username("bootstrap-object-replay");

        let trigger_payload = SqlDefinitionPayload {
            object_id: "trg_users_bi".to_string(),
            object_kind: SqlObjectKind::Trigger,
            action: SqlDefinitionAction::Upsert,
            schema_epoch: 1,
            sql: "create trigger trg_users_bi before insert on users for each row begin end"
                .to_string(),
            dependencies: vec!["users".to_string()],
        };

        app.wal
            .append(
                &wal_id,
                TransactionRecord {
                    id: TransactionId(1),
                    refid: None,
                    timestamp_epoch_ms: 1,
                    actor: actor.clone(),
                    kind: TransactionKind::SqlDefinitionChange,
                    payload: trigger_payload
                        .encode()
                        .expect("trigger sql payload should encode"),
                },
            )
            .expect("trigger sql definition append should succeed");

        let trigger_metadata_payload = EntityMetadataPayload {
            entity_id: "trg_users_bi".to_string(),
            metadata: EntityMetadata::default()
                .with_creator("bootstrap-object-replay")
                .with_created_at(2),
        };

        app.wal
            .append(
                &wal_id,
                TransactionRecord {
                    id: TransactionId(2),
                    refid: Some(TransactionId(1)),
                    timestamp_epoch_ms: 2,
                    actor: actor.clone(),
                    kind: TransactionKind::MetadataChange,
                    payload: trigger_metadata_payload
                        .encode()
                        .expect("trigger metadata payload should encode"),
                },
            )
            .expect("trigger metadata append should succeed");

        let view_upsert_payload = SqlDefinitionPayload {
            object_id: "users_v".to_string(),
            object_kind: SqlObjectKind::View,
            action: SqlDefinitionAction::Upsert,
            schema_epoch: 1,
            sql: "create view users_v as select * from users".to_string(),
            dependencies: vec!["users".to_string()],
        };

        app.wal
            .append(
                &wal_id,
                TransactionRecord {
                    id: TransactionId(3),
                    refid: Some(TransactionId(2)),
                    timestamp_epoch_ms: 3,
                    actor: actor.clone(),
                    kind: TransactionKind::SqlDefinitionChange,
                    payload: view_upsert_payload
                        .encode()
                        .expect("view upsert payload should encode"),
                },
            )
            .expect("view upsert append should succeed");

        let view_drop_payload = SqlDefinitionPayload {
            object_id: "users_v".to_string(),
            object_kind: SqlObjectKind::View,
            action: SqlDefinitionAction::Drop,
            schema_epoch: 2,
            sql: String::new(),
            dependencies: Vec::new(),
        };

        app.wal
            .append(
                &wal_id,
                TransactionRecord {
                    id: TransactionId(4),
                    refid: Some(TransactionId(3)),
                    timestamp_epoch_ms: 4,
                    actor,
                    kind: TransactionKind::SqlDefinitionChange,
                    payload: view_drop_payload
                        .encode()
                        .expect("view drop payload should encode"),
                },
            )
            .expect("view drop append should succeed");

        app.bootstrap()
            .expect("bootstrap should replay entity construction records");

        let loaded = app
            .catalogs()
            .get(&wal_id)
            .expect("catalog should be loaded");

        assert!(loaded.trigger("trg_users_bi").is_some());
        assert_eq!(
            loaded
                .entity_metadata("trg_users_bi")
                .and_then(|metadata| metadata.created_by.as_deref()),
            Some("bootstrap-object-replay")
        );
        assert!(loaded.view("users_v").is_none());
    }

    #[test]
    fn select_query_returns_table_schema_columns() {

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

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![
                    FieldDef {
                        seqno: 1,
                        field_name: "id".to_string(),
                        field_type: FieldType::Int(64),
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "email".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::Indexed,
                        default_value: None,
                    },
                ]),
            )
            .expect("table should register");

        app.catalogs.insert("main".to_string(), catalog);

        let request = ConnectorRequest::new(
            "req-query-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select * from users".to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);

        assert_eq!(response.request_id, "req-query-1");
        assert_eq!(response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = response.result else {
            panic!("expected query result");
        };

        let column_names = result
            .columns
            .iter()
            .map(|field| field.field_name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(column_names, vec!["id", "email"]);
        assert!(result.rows.is_empty());

    }

    #[test]
    fn show_tables_query_returns_table_name_rows() {
        
        let unique_suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock should be after unix epoch")
            .as_nanos();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-show-tables-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                }]),
            )
            .expect("users table should register");

        catalog
            .register_table(
                "accounts",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                }]),
            )
            .expect("accounts table should register");

        app.catalogs.insert("main".to_string(), catalog);

        let request = ConnectorRequest::new(
            "req-show-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "show tables".to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);

        assert_eq!(response.request_id, "req-show-1");
        assert_eq!(response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = response.result else {
            panic!("expected query result");
        };

        let column_names = result
            .columns
            .iter()
            .map(|field| field.field_name.as_str())
            .collect::<Vec<_>>();
        
        assert_eq!(column_names, vec!["table_name"]);
        assert_eq!(result.rows.len(), 2);

        let row_values = result
            .rows
            .iter()
            .map(|row| String::from_utf8_lossy(&row[0]).to_string())
            .collect::<Vec<_>>();

        assert_eq!(row_values, vec!["accounts", "users"]);

    }

    #[test]
    fn create_table_query_registers_table_with_schema() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-create-table-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");
        app.catalogs.insert("main".to_string(), catalog);

        let request = ConnectorRequest::new(
            "req-create-table-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255))"
                        .to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);
        assert_eq!(response.status, ResponseStatus::Applied);

        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        let schema = catalog
            .table_schema("users")
            .expect("users schema should exist");
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fields[0].field_name, "id");
        assert_eq!(schema.fields[0].indexed, FieldIndex::PrimaryKey);
        assert_eq!(schema.fields[1].field_name, "email");
    }

    #[test]
    fn drop_table_query_removes_table() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-drop-table-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");
        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("users table should register");
        app.catalogs.insert("main".to_string(), catalog);

        let request = ConnectorRequest::new(
            "req-drop-table-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "drop table users".to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);
        assert_eq!(response.status, ResponseStatus::Applied);

        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        assert!(catalog.table("users").is_none());
    }

    #[test]
    fn create_database_query_creates_catalog() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-create-db-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let request = ConnectorRequest::new(
            "req-create-db-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create database analytics".to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);
        assert_eq!(response.status, ResponseStatus::Applied);
        assert!(!app.catalogs().is_empty());
    }

    #[test]
    fn drop_database_query_removes_catalog() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-drop-db-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root.clone());
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog = DatabaseCatalog::create_empty_from_name("analytics")
            .expect("catalog should be created");
        catalog
            .save_in_directory(&app.node_data_dir)
            .expect("catalog should be persisted");

        app.catalogs
            .insert(catalog.database_id.0.clone(), catalog.clone());

        let catalog_file = app.node_data_dir.join(catalog.file_name());
        assert!(catalog_file.exists());

        let request = ConnectorRequest::new(
            "req-drop-db-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "drop database analytics".to_string(),
                },
            },
        );

        let response = app.handle_connector_request(&request);
        assert_eq!(response.status, ResponseStatus::Applied);
        assert!(app.catalogs().get("analytics").is_none());
        assert!(!catalog_file.exists());
    }

    #[test]
    fn create_and_drop_sql_backed_objects_are_wired() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-sql-backed-objects-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                }]),
            )
            .expect("users table should register");

        app.catalogs.insert("main".to_string(), catalog);

        let create_view = ConnectorRequest::new(
            "req-create-view",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create view users_v as select * from users".to_string(),
                },
            },
        );

        let create_trigger = ConnectorRequest::new(
            "req-create-trigger",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create trigger trg_users_bi before insert on users for each row begin end"
                        .to_string(),
                },
            },
        );

        let create_procedure = ConnectorRequest::new(
            "req-create-procedure",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create procedure p_sync() begin end".to_string(),
                },
            },
        );

        let create_view_response = app.handle_connector_request(&create_view);
        let create_trigger_response = app.handle_connector_request(&create_trigger);
        let create_procedure_response = app.handle_connector_request(&create_procedure);

        assert_eq!(create_view_response.status, ResponseStatus::Applied);
        assert_eq!(create_trigger_response.status, ResponseStatus::Applied);
        assert_eq!(create_procedure_response.status, ResponseStatus::Applied);

        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        assert!(catalog.view("users_v").is_some());
        assert!(catalog.trigger("trg_users_bi").is_some());
        assert!(catalog.stored_procedure("p_sync").is_some());

        let drop_view = ConnectorRequest::new(
            "req-drop-view",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "drop view users_v".to_string(),
                },
            },
        );

        let drop_trigger = ConnectorRequest::new(
            "req-drop-trigger",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "drop trigger trg_users_bi on users".to_string(),
                },
            },
        );

        let drop_procedure = ConnectorRequest::new(
            "req-drop-procedure",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "drop procedure p_sync".to_string(),
                },
            },
        );

        let drop_view_response = app.handle_connector_request(&drop_view);
        let drop_trigger_response = app.handle_connector_request(&drop_trigger);
        let drop_procedure_response = app.handle_connector_request(&drop_procedure);

        assert_eq!(drop_view_response.status, ResponseStatus::Applied);
        assert_eq!(drop_trigger_response.status, ResponseStatus::Applied);
        assert_eq!(drop_procedure_response.status, ResponseStatus::Applied);

        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        assert!(catalog.view("users_v").is_none());
        assert!(catalog.trigger("trg_users_bi").is_none());
        assert!(catalog.stored_procedure("p_sync").is_none());
    }

    #[test]
    fn connector_client_path_can_query_show_tables_without_simulation() {
        
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-client-path-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                }]),
            )
            .expect("users table should register");

        catalog
            .register_table(
                "accounts",
                TableSchema::new(vec![FieldDef {
                    seqno: 1,
                    field_name: "id".to_string(),
                    field_type: FieldType::Int(64),
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                }]),
            )
            .expect("accounts table should register");

        app.catalogs.insert("main".to_string(), catalog);

        let transport = InProcessServerTransport {
            app: RefCell::new(app),
        };
        let client = ConnectorClient::new(transport);

        let request = ConnectorRequest::new(
            "req-client-show-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "show tables".to_string(),
                },
            },
        );

        let response = client
            .execute(&request)
            .expect("connector client should receive applied response");

        assert_eq!(response.request_id, "req-client-show-1");
        assert_eq!(response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = response.result else {
            panic!("expected query result");
        };

        let row_values = result
            .rows
            .iter()
            .map(|row| String::from_utf8_lossy(&row[0]).to_string())
            .collect::<Vec<_>>();

        assert_eq!(row_values, vec!["accounts", "users"]);

    }

    #[test]
    fn connector_client_path_can_query_select_without_simulation() {

        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-client-select-path-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![
                    FieldDef {
                        seqno: 1,
                        field_name: "id".to_string(),
                        field_type: FieldType::Int(64),
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "email".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::Indexed,
                        default_value: None,
                    },
                ]),
            )
            .expect("users table should register");

        app.catalogs.insert("main".to_string(), catalog);

        let transport = InProcessServerTransport {
            app: RefCell::new(app),
        };

        let client = ConnectorClient::new(transport);

        let request = ConnectorRequest::new(
            "req-client-select-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select * from users".to_string(),
                },
            },
        );

        let response = client
            .execute(&request)
            .expect("connector client should receive applied response");

        assert_eq!(response.request_id, "req-client-select-1");
        assert_eq!(response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = response.result else {
            panic!("expected query result");
        };

        let column_names = result
            .columns
            .iter()
            .map(|field| field.field_name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(column_names, vec!["id", "email"]);
        assert!(result.rows.is_empty());
        
    }

    #[test]
    fn query_path_stress_respects_timing_thresholds() {
        
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-query-stress-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let mut catalog = DatabaseCatalog::create_empty_from_name("main")
            .expect("catalog should be created");

        catalog
            .register_table(
                "users",
                TableSchema::new(vec![
                    FieldDef {
                        seqno: 1,
                        field_name: "id".to_string(),
                        field_type: FieldType::Int(64),
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "email".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::Indexed,
                        default_value: None,
                    },
                ]),
            )
            .expect("users table should register");

        app.catalogs.insert("main".to_string(), catalog);

        let thresholds = QueryTimingThresholds::from_env();
        let mut durations_ms = Vec::with_capacity(thresholds.stress_iterations);

        let batch_start = std::time::Instant::now();

        for idx in 0..thresholds.stress_iterations {
            let sql = if idx % 2 == 0 {
                "select * from users"
            } else {
                "show tables"
            };

            let request = ConnectorRequest::new(
                format!("stress-req-{idx}"),
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            );

            let start = std::time::Instant::now();
            let response = app.handle_connector_request(&request);
            let elapsed_ms = start.elapsed().as_millis();

            assert_eq!(response.status, ResponseStatus::Applied);
            durations_ms.push(elapsed_ms);
        }

        let batch_elapsed_ms = batch_start.elapsed().as_millis();
        durations_ms.sort_unstable();

        let p95 = percentile(&durations_ms, 95);
        let p99 = percentile(&durations_ms, 99);

        assert!(
            p95 <= thresholds.p95_max_ms,
            "p95 latency exceeded threshold: p95={}ms threshold={}ms",
            p95,
            thresholds.p95_max_ms
        );
        assert!(
            p99 <= thresholds.p99_max_ms,
            "p99 latency exceeded threshold: p99={}ms threshold={}ms",
            p99,
            thresholds.p99_max_ms
        );
        assert!(
            batch_elapsed_ms <= thresholds.batch_max_ms,
            "batch duration exceeded threshold: batch={}ms threshold={}ms",
            batch_elapsed_ms,
            thresholds.batch_max_ms
        );
    }

    fn percentile(sorted_values: &[u128], pct: usize) -> u128 {
        if sorted_values.is_empty() {
            return 0;
        }

        let rank = ((pct * sorted_values.len()) + 99) / 100;
        let idx = rank.saturating_sub(1).min(sorted_values.len() - 1);
        sorted_values[idx]
    }
    
}