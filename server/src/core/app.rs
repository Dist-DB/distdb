use std::collections::HashMap;
use std::path::PathBuf;

use common::helpers::format::FileKind;
use common::helpers::{create_dir, list_files};
use connector::{
    ConnectorCommand, ConnectorRequest, ConnectorResponse, ConnectorResult, DataMutation,
    MutationResult, SchemaCommand,
};
#[cfg(test)]
use serverlib::decode_row_payload;
use serverlib::{ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore};

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
    runtime_indexes: RuntimeIndexStore,
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

    pub fn handle_connector_request(&mut self, request: &ConnectorRequest) -> ConnectorResponse {
        let command_info = command_info(&request.command);
        let command_path = command_info.path;
        log::info!(
            "connector request dispatch request_id={} path={}",
            request.request_id,
            command_path
        );

        let response = match command_info.kind {
            CommandKind::CreateDatabase => {
                let ConnectorCommand::CreateDatabase { database_name } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                match DatabaseCatalog::create_new_database(database_name, &self.node_data_dir) {
                    Ok(catalog) => {
                        self.catalogs.insert(catalog.database_id.0.clone(), catalog);

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

            CommandKind::Query => {
                let ConnectorCommand::Query { query } = &request.command else {
                    unreachable!("command info kind must align with command variant")
                };

                handle_query_command(
                    &request.request_id,
                    query,
                    &mut self.catalogs,
                    &self.wal,
                    &self.node_data_dir,
                    &mut self.runtime_indexes,
                )
            }

            CommandKind::Schema => ConnectorResponse::rejected(
                request.request_id.clone(),
                "schema command execution is not wired yet",
            ),

            CommandKind::Mutation => ConnectorResponse::rejected(
                request.request_id.clone(),
                "mutation command execution is not wired yet",
            ),
        };

        match &response.result {
            ConnectorResult::Error(message) => {
                log::warn!(
                    "connector request completed request_id={} path={} status={:?} error={}",
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

            self.catalogs.insert(catalog.database_id.0.clone(), catalog);
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommandKind {
    CreateDatabase,
    Query,
    Schema,
    Mutation,
}

#[derive(Debug, Clone)]
struct CommandInfo {
    kind: CommandKind,
    path: String,
}

fn command_info(command: &ConnectorCommand) -> CommandInfo {
    match command {
        ConnectorCommand::CreateDatabase { database_name } => CommandInfo {
            kind: CommandKind::CreateDatabase,
            path: format!("create_database:{database_name}"),
        },

        ConnectorCommand::Query { query } => CommandInfo {
            kind: CommandKind::Query,
            path: format!("query:{}", query.database_id),
        },

        ConnectorCommand::Schema {
            database_id,
            command,
        } => {
            let path = match command {
                SchemaCommand::CreateTable { table_id, .. } => {
                    format!("schema:create_table:{database_id}:{table_id}")
                }
                SchemaCommand::AlterTable { change } => {
                    format!("schema:alter_table:{database_id}:{}", change.table_id)
                }
                SchemaCommand::DropTable { table_id } => {
                    format!("schema:drop_table:{database_id}:{table_id}")
                }
            };

            CommandInfo {
                kind: CommandKind::Schema,
                path,
            }
        }

        ConnectorCommand::Mutation {
            database_id,
            mutation,
        } => {
            let path = match mutation {
                DataMutation::Insert { table_id, .. } => {
                    format!("mutation:insert:{database_id}:{table_id}")
                }
                DataMutation::Update { table_id, .. } => {
                    format!("mutation:update:{database_id}:{table_id}")
                }
                DataMutation::Delete { table_id, .. } => {
                    format!("mutation:delete:{database_id}:{table_id}")
                }
            };

            CommandInfo {
                kind: CommandKind::Mutation,
                path,
            }
        }
    }
}

fn describe_command_path(command: &ConnectorCommand) -> String {
    command_info(command).path
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::core::mappings::perf::QueryTimingThresholds;
    use connector::{
        ConnectorClient, ConnectorCommand, ConnectorError, ConnectorRequest, ConnectorResult,
        ConnectorTransport, ResponseStatus,
    };
    use serverlib::engine::database::transaction::TransactionLog;
    use serverlib::{
        DatabaseIndex, DatabaseIndexKind, EntityMetadata, EntityMetadataPayload, FieldDef,
        FieldIndex, FieldType, SchemaChangePayload, SqlDefinitionAction, SqlDefinitionPayload,
        SqlObjectKind, TableSchema, TransactionId, TransactionKind, TransactionRecord, UserId,
    };

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
                    metadata: None,
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
            metadata: None,
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
        let email_index_id = DatabaseIndex::from_table_fields(
            "users",
            DatabaseIndexKind::Indexed,
            vec!["email".to_string()],
        )
        .index_id
        .0;
        assert!(loaded.index(&email_index_id).is_some());
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
                    metadata: None,
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

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
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "email".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::Indexed,
                        default_value: None,
                        metadata: None,
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

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
                    metadata: None,
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
                    metadata: None,
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

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
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
    fn insert_query_appends_insert_record_to_table_wal() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-insert-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        let create_request = ConnectorRequest::new(
            "req-create-table-insert-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                        .to_string(),
                },
            },
        );

        let create_response = app.handle_connector_request(&create_request);
        assert_eq!(create_response.status, ResponseStatus::Applied);

        let insert_request = ConnectorRequest::new(
            "req-insert-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
                },
            },
        );

        let insert_response = app.handle_connector_request(&insert_request);
        assert_eq!(insert_response.status, ResponseStatus::Applied);

        let ConnectorResult::Mutation(mutation) = insert_response.result else {
            panic!("expected mutation result");
        };
        assert_eq!(mutation.affected_rows, 1);

        let records = app.wal.since("users", None);
        let insert_record = records
            .iter()
            .find(|record| record.kind == TransactionKind::Insert)
            .expect("insert transaction should be present in table WAL");

        let schema = app
            .catalogs
            .get("main")
            .and_then(|catalog| catalog.table_schema("users"))
            .expect("users schema should exist");

        let payload = decode_row_payload(schema, &insert_record.payload)
            .expect("insert payload should deserialize");

        assert_eq!(payload.get("id"), Some(&b"1".to_vec()));
        assert_eq!(payload.get("email"), Some(&b"sam@example.com".to_vec()));
    }

    #[test]
    fn update_query_updates_live_row() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-update-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        let create_request = ConnectorRequest::new(
            "req-create-table-update-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                        .to_string(),
                },
            },
        );

        let create_response = app.handle_connector_request(&create_request);
        assert_eq!(create_response.status, ResponseStatus::Applied);

        let insert_request = ConnectorRequest::new(
            "req-insert-update-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
                },
            },
        );

        let insert_response = app.handle_connector_request(&insert_request);
        assert_eq!(insert_response.status, ResponseStatus::Applied);

        let update_request = ConnectorRequest::new(
            "req-update-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "update users set email='sam+updated@example.com' where id=1".to_string(),
                },
            },
        );

        let update_response = app.handle_connector_request(&update_request);
        assert_eq!(update_response.status, ResponseStatus::Applied);

        let ConnectorResult::Mutation(update_mutation) = update_response.result else {
            panic!("expected mutation result");
        };
        assert_eq!(update_mutation.affected_rows, 1);

        let select_request = ConnectorRequest::new(
            "req-select-update-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select id, email from users where id=1".to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], b"1".to_vec());
        assert_eq!(result.rows[0][1], b"sam+updated@example.com".to_vec());
    }

    #[test]
    fn delete_query_removes_live_row() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-delete-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        let create_request = ConnectorRequest::new(
            "req-create-table-delete-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                        .to_string(),
                },
            },
        );

        let create_response = app.handle_connector_request(&create_request);
        assert_eq!(create_response.status, ResponseStatus::Applied);

        let insert_request = ConnectorRequest::new(
            "req-insert-delete-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
                },
            },
        );

        let insert_response = app.handle_connector_request(&insert_request);
        assert_eq!(insert_response.status, ResponseStatus::Applied);

        let delete_request = ConnectorRequest::new(
            "req-delete-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "delete from users where id=1".to_string(),
                },
            },
        );

        let delete_response = app.handle_connector_request(&delete_request);
        assert_eq!(delete_response.status, ResponseStatus::Applied);

        let ConnectorResult::Mutation(delete_mutation) = delete_response.result else {
            panic!("expected mutation result");
        };
        assert_eq!(delete_mutation.affected_rows, 1);

        let select_request = ConnectorRequest::new(
            "req-select-delete-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select id, email from users where id=1".to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert!(result.rows.is_empty());
    }

    #[test]
    fn update_query_with_join_updates_matching_target_row() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-update-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-update-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-update-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-update-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-update-join-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-update-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let update_request = ConnectorRequest::new(
            "req-update-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "update users u join profiles p on u.id = p.user_id set email='sam+updated@example.com' where p.name = 'Sam'"
                        .to_string(),
                },
            },
        );

        let update_response = app.handle_connector_request(&update_request);
        assert_eq!(update_response.status, ResponseStatus::Applied);

        let ConnectorResult::Mutation(mutation) = update_response.result else {
            panic!("expected mutation result");
        };
        assert_eq!(mutation.affected_rows, 1);

        let select_request = ConnectorRequest::new(
            "req-select-update-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select id, email from users".to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 2);

        let mut emails = result
            .rows
            .iter()
            .map(|row| String::from_utf8(row[1].clone()).expect("email should be valid utf8"))
            .collect::<Vec<_>>();

        emails.sort();

        assert_eq!(
            emails,
            vec![
                "alex@example.com".to_string(),
                "sam+updated@example.com".to_string(),
            ]
        );
    }

    #[test]
    fn delete_query_with_left_outer_join_removes_unmatched_target_row() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-delete-left-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-delete-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-delete-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-delete-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-delete-join-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-delete-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let delete_request = ConnectorRequest::new(
            "req-delete-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "delete u from users u left join profiles p on u.id = p.user_id where p.name is null"
                        .to_string(),
                },
            },
        );

        let delete_response = app.handle_connector_request(&delete_request);
        assert_eq!(delete_response.status, ResponseStatus::Applied);

        let ConnectorResult::Mutation(mutation) = delete_response.result else {
            panic!("expected mutation result");
        };
        assert_eq!(mutation.affected_rows, 1);

        let select_request = ConnectorRequest::new(
            "req-select-delete-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select id, email from users".to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], b"1".to_vec());
    }

    #[test]
    fn select_inner_join_returns_matching_rows() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-profiles-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let select_request = ConnectorRequest::new(
            "req-select-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email, p.name from users u inner join profiles p on u.id = p.user_id where u.id = 1"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
        assert_eq!(result.rows[0][1], b"Sam".to_vec());
    }

    #[test]
    fn select_inner_join_preserves_one_to_many_matches() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-join-many-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-join-many-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-join-many-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-join-many-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-profiles-join-many-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
            (
                "req-insert-profiles-join-many-2",
                "insert into profiles (id, user_id, name) values (11, 1, 'Samuel')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let select_request = ConnectorRequest::new(
            "req-select-join-many-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email, p.name from users u inner join profiles p on u.id = p.user_id where u.id = 1"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 2);

        let mut names = result
            .rows
            .iter()
            .map(|row| String::from_utf8(row[1].clone()).expect("name should be valid utf8"))
            .collect::<Vec<_>>();

        names.sort();

        assert_eq!(names, vec!["Sam".to_string(), "Samuel".to_string()]);

        for row in &result.rows {
            assert_eq!(row[0], b"sam@example.com".to_vec());
        }
    }

    #[test]
    fn select_left_join_returns_unmatched_left_rows_with_nulls() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-left-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-left-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-left-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-left-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-left-join-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-left-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let select_request = ConnectorRequest::new(
            "req-select-left-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email, p.name from users u left join profiles p on u.id = p.user_id"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 2);

        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                (
                    String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                    String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
                )
            })
            .collect::<Vec<_>>();

        rows.sort();

        assert_eq!(
            rows,
            vec![
                ("alex@example.com".to_string(), "NULL".to_string()),
                ("sam@example.com".to_string(), "Sam".to_string()),
            ]
        );
    }

    #[test]
    fn select_left_join_where_right_field_is_null_filters_after_tuple_formation() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-left-join-where-null-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-left-join-null-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-left-join-null-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-left-join-null-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-left-join-null-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-left-join-null-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let select_request = ConnectorRequest::new(
            "req-select-left-join-null-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email from users u left join profiles p on u.id = p.user_id where p.name is null"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], b"alex@example.com".to_vec());
    }

    #[test]
    fn select_left_outer_join_null_extends_unmatched_rows() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-left-outer-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-left-outer-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-left-outer-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-left-outer-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-left-outer-join-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-left-outer-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(response.status, ResponseStatus::Applied);
        }

        let select_request = ConnectorRequest::new(
            "req-select-left-outer-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email, p.name from users u left outer join profiles p on u.id = p.user_id"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 2);

        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                (
                    String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                    String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
                )
            })
            .collect::<Vec<_>>();

        rows.sort();

        assert_eq!(
            rows,
            vec![
                ("alex@example.com".to_string(), "NULL".to_string()),
                ("sam@example.com".to_string(), "Sam".to_string()),
            ]
        );
    }

    #[test]
    fn select_right_outer_join_null_extends_unmatched_rows() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-right-outer-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-right-outer-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-right-outer-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-right-outer-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-profiles-right-outer-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
            (
                "req-insert-profiles-right-outer-join-2",
                "insert into profiles (id, user_id, name) values (11, 2, 'Orphan')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(response.status, ResponseStatus::Applied);
        }

        let select_request = ConnectorRequest::new(
            "req-select-right-outer-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email, p.name from users u right outer join profiles p on u.id = p.user_id"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 2);

        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                (
                    String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                    String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
                )
            })
            .collect::<Vec<_>>();

        rows.sort();

        assert_eq!(
            rows,
            vec![
                ("NULL".to_string(), "Orphan".to_string()),
                ("sam@example.com".to_string(), "Sam".to_string()),
            ]
        );
    }

    #[test]
    fn select_full_outer_join_null_extends_both_sides() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-full-outer-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-full-outer-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-full-outer-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-insert-users-full-outer-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-full-outer-join-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-full-outer-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
            (
                "req-insert-profiles-full-outer-join-2",
                "insert into profiles (id, user_id, name) values (11, 3, 'Orphan')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(response.status, ResponseStatus::Applied);
        }

        let select_request = ConnectorRequest::new(
            "req-select-full-outer-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.email, p.name from users u full outer join profiles p on u.id = p.user_id"
                        .to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 3);

        let mut rows = result
            .rows
            .iter()
            .map(|row| {
                (
                    String::from_utf8(row[0].clone()).expect("email should be valid utf8"),
                    String::from_utf8(row[1].clone()).expect("name should be valid utf8"),
                )
            })
            .collect::<Vec<_>>();

        rows.sort();

        assert_eq!(
            rows,
            vec![
                ("NULL".to_string(), "Orphan".to_string()),
                ("alex@example.com".to_string(), "NULL".to_string()),
                ("sam@example.com".to_string(), "Sam".to_string()),
            ]
        );
    }

    #[test]
    fn explain_select_with_multiple_joins_returns_join_steps() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-explain-multi-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-explain-multi-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-explain-multi-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-create-teams-explain-multi-join-1",
                "create table teams (id bigint not null primary key, profile_id bigint not null, label varchar(255) not null)",
            ),
            (
                "req-insert-users-explain-multi-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-profiles-explain-multi-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
            (
                "req-insert-teams-explain-multi-join-1",
                "insert into teams (id, profile_id, label) values (100, 10, 'core')",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(response.status, ResponseStatus::Applied);
        }

        let explain_request = ConnectorRequest::new(
            "req-explain-multi-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "explain select u.email, p.name, t.label from users u inner join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where u.id = 1"
                        .to_string(),
                },
            },
        );

        let explain_response = app.handle_connector_request(&explain_request);
        assert_eq!(explain_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = explain_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 3);
        assert_eq!(result.rows[0][1], b"base".to_vec());
        assert_eq!(result.rows[1][1], b"inner".to_vec());
        assert_eq!(result.rows[2][1], b"left".to_vec());
    }

    #[test]
    fn explain_insert_update_delete_return_plan_details() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-explain-mutations-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        let explain_insert = ConnectorRequest::new(
            "req-explain-insert-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "explain insert into users (id, email) values (1, 'sam@example.com')"
                        .to_string(),
                },
            },
        );

        let insert_response = app.handle_connector_request(&explain_insert);
        assert_eq!(insert_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(insert_result) = insert_response.result else {
            panic!("expected query result");
        };

        assert!(
            insert_result
                .rows
                .iter()
                .any(|row| row == &vec![b"operation".to_vec(), b"insert".to_vec()])
        );

        let explain_update = ConnectorRequest::new(
            "req-explain-update-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "explain update users u join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id set u.email = 'sam+updated@example.com' where t.label = 'core'"
                        .to_string(),
                },
            },
        );

        let update_response = app.handle_connector_request(&explain_update);
        assert_eq!(update_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(update_result) = update_response.result else {
            panic!("expected query result");
        };

        assert!(
            update_result
                .rows
                .iter()
                .any(|row| row == &vec![b"join_count".to_vec(), b"2".to_vec()])
        );
        assert!(
            update_result
                .rows
                .iter()
                .any(|row| row == &vec![b"join[0].kind".to_vec(), b"inner".to_vec()])
        );
        assert!(
            update_result
                .rows
                .iter()
                .any(|row| row == &vec![b"join[1].kind".to_vec(), b"left".to_vec()])
        );

        let explain_delete = ConnectorRequest::new(
            "req-explain-delete-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "explain delete u from users u join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where t.label is null"
                        .to_string(),
                },
            },
        );

        let delete_response = app.handle_connector_request(&explain_delete);
        assert_eq!(delete_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(delete_result) = delete_response.result else {
            panic!("expected query result");
        };

        assert!(
            delete_result
                .rows
                .iter()
                .any(|row| row == &vec![b"operation".to_vec(), b"delete".to_vec()])
        );
        assert!(
            delete_result
                .rows
                .iter()
                .any(|row| row == &vec![b"join_count".to_vec(), b"2".to_vec()])
        );
    }

    #[test]
    fn insert_select_copies_rows_into_target_table() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-insert-select-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-insert-select-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-users-archive-insert-select-1",
                "create table users_archive (id bigint not null, email varchar(255) not null)",
            ),
            (
                "req-insert-users-insert-select-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-insert-select-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-select-run-1",
                "insert into users_archive (id, email) select id, email from users where id = 1",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(
                response.status,
                ResponseStatus::Applied,
                "request '{}' failed with result {:?}",
                request_id,
                response.result,
            );
        }

        let select_request = ConnectorRequest::new(
            "req-select-users-archive-insert-select-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select id, email from users_archive".to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], b"1".to_vec());
        assert_eq!(result.rows[0][1], b"sam@example.com".to_vec());
    }

    #[test]
    fn insert_select_with_join_materializes_joined_source_rows() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-insert-select-join-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

        app.catalogs.insert("main".to_string(), catalog);

        for (request_id, sql) in [
            (
                "req-create-users-insert-select-join-1",
                "create table users (id bigint not null primary key, email varchar(255) not null)",
            ),
            (
                "req-create-profiles-insert-select-join-1",
                "create table profiles (id bigint not null primary key, user_id bigint not null, name varchar(255) not null)",
            ),
            (
                "req-create-flat-insert-select-join-1",
                "create table user_profile_flat (email varchar(255) not null, profile_name varchar(255) not null)",
            ),
            (
                "req-insert-users-insert-select-join-1",
                "insert into users (id, email) values (1, 'sam@example.com')",
            ),
            (
                "req-insert-users-insert-select-join-2",
                "insert into users (id, email) values (2, 'alex@example.com')",
            ),
            (
                "req-insert-profiles-insert-select-join-1",
                "insert into profiles (id, user_id, name) values (10, 1, 'Sam')",
            ),
            (
                "req-insert-select-join-run-1",
                "insert into user_profile_flat (email, profile_name) select u.email, p.name from users u inner join profiles p on u.id = p.user_id",
            ),
        ] {
            let response = app.handle_connector_request(&ConnectorRequest::new(
                request_id,
                ConnectorCommand::Query {
                    query: connector::DataQuery {
                        database_id: "main".to_string(),
                        sql: sql.to_string(),
                    },
                },
            ));

            assert_eq!(response.status, ResponseStatus::Applied);
        }

        let select_request = ConnectorRequest::new(
            "req-select-flat-insert-select-join-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select email, profile_name from user_profile_flat".to_string(),
                },
            },
        );

        let select_response = app.handle_connector_request(&select_request);
        assert_eq!(select_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = select_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
        assert_eq!(result.rows[0][1], b"Sam".to_vec());
    }

    #[test]
    fn select_alias_where_pk_falls_back_to_scan_when_runtime_index_is_empty() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-select-empty-index-fallback-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
        app.catalogs.insert("main".to_string(), catalog);

        let create_request = ConnectorRequest::new(
            "req-create-table-empty-index-fallback-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255) not null)"
                        .to_string(),
                },
            },
        );

        let create_response = app.handle_connector_request(&create_request);
        assert_eq!(create_response.status, ResponseStatus::Applied);

        let insert_request = ConnectorRequest::new(
            "req-insert-empty-index-fallback-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "insert into users (id, email) values (1, 'sam@example.com')".to_string(),
                },
            },
        );

        let insert_response = app.handle_connector_request(&insert_request);
        assert_eq!(insert_response.status, ResponseStatus::Applied);

        // Simulate stale runtime index state: index registered but no entries.
        app.runtime_indexes = RuntimeIndexStore::new();
        let index_defs = {
            let catalog = app.catalogs.get("main").expect("main catalog should exist");
            let table = catalog.table("users").expect("users table should exist");
            table.indexes.values().cloned().collect::<Vec<_>>()
        };

        let primary_index_id = index_defs
            .iter()
            .find(|index| index.is_primary_key())
            .map(|index| index.index_id.0.clone())
            .expect("primary key index should exist");

        for index in index_defs {
            app.runtime_indexes.register_index(index);
        }

        assert_eq!(app.runtime_indexes.cardinality(&primary_index_id), Some(0));

        let query_request = ConnectorRequest::new(
            "req-select-empty-index-fallback-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "select u.* from users u where u.id = '1'".to_string(),
                },
            },
        );

        let query_response = app.handle_connector_request(&query_request);
        assert_eq!(query_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = query_response.result else {
            panic!("expected query result");
        };

        assert_eq!(result.rows.len(), 1);
        assert_eq!(String::from_utf8_lossy(&result.rows[0][0]), "1");
        assert_eq!(
            String::from_utf8_lossy(&result.rows[0][1]),
            "sam@example.com"
        );
    }

    #[test]
    fn describe_table_query_returns_schema_rows() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-describe-table-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
        app.catalogs.insert("main".to_string(), catalog);

        let create_request = ConnectorRequest::new(
            "req-create-table-2",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255))"
                        .to_string(),
                },
            },
        );

        let create_response = app.handle_connector_request(&create_request);
        assert_eq!(create_response.status, ResponseStatus::Applied);

        let describe_request = ConnectorRequest::new(
            "req-describe-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "describe users".to_string(),
                },
            },
        );

        let describe_response = app.handle_connector_request(&describe_request);
        assert_eq!(describe_response.status, ResponseStatus::Applied);

        let ConnectorResult::Query(result) = describe_response.result else {
            panic!("expected query result");
        };

        let column_names = result
            .columns
            .iter()
            .map(|field| field.field_name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            column_names,
            vec!["field", "type", "null", "key", "default"]
        );
        assert_eq!(result.rows.len(), 2);

        let first_row = result
            .rows
            .first()
            .expect("describe should return first row");
        assert_eq!(String::from_utf8_lossy(&first_row[0]), "id");
        assert_eq!(String::from_utf8_lossy(&first_row[3]), "PRI");

        let second_row = result
            .rows
            .get(1)
            .expect("describe should return second row");

        assert_eq!(String::from_utf8_lossy(&second_row[0]), "email");
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
        catalog
            .register_table("users", TableSchema::new(Vec::new()))
            .expect("users table should register");
        app.catalogs.insert("main".to_string(), catalog);

        let normalized_table_id = common::normalize_identifier!("users");
        let legacy_table_stream_file = app
            .node_data_dir
            .join(FileKind::Data.file_name(&normalized_table_id));
        let hashed_table_stream_file = app
            .node_data_dir
            .join(FileKind::Data.file_name(common::helpers::stable_id(&[&normalized_table_id])));

        std::fs::write(&legacy_table_stream_file, b"legacy stream")
            .expect("legacy table stream file should be created");
        std::fs::write(&hashed_table_stream_file, b"hashed stream")
            .expect("hashed table stream file should be created");

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
        assert!(!legacy_table_stream_file.exists());
        assert!(!hashed_table_stream_file.exists());
    }

    #[test]
    fn alter_table_query_updates_schema() {
        let unique_suffix = common::epochabs!();

        let temp_root = std::env::temp_dir().join(format!(
            "distdb-server-alter-table-query-{}-{}",
            std::process::id(),
            unique_suffix
        ));

        let config = ServerRuntimeConfig::default_local_with_data_dir(temp_root);
        let mut app = ServerApp::new(config).expect("server app should initialize");

        let catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
        app.catalogs.insert("main".to_string(), catalog);

        let create_request = ConnectorRequest::new(
            "req-create-table-alter-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "create table users (id bigint not null primary key, email varchar(255))"
                        .to_string(),
                },
            },
        );

        let create_response = app.handle_connector_request(&create_request);
        assert_eq!(create_response.status, ResponseStatus::Applied);

        let alter_request = ConnectorRequest::new(
            "req-alter-table-1",
            ConnectorCommand::Query {
                query: connector::DataQuery {
                    database_id: "main".to_string(),
                    sql: "alter table users add column status varchar(20) not null default 'active', rename column email to login_email"
                        .to_string(),
                },
            },
        );

        let alter_response = app.handle_connector_request(&alter_request);
        assert_eq!(alter_response.status, ResponseStatus::Applied);

        let catalog = app.catalogs.get("main").expect("main catalog should exist");
        let schema = catalog
            .table_schema("users")
            .expect("users schema should exist");

        assert!(schema.field("status").is_some());
        assert!(schema.field("login_email").is_some());
        assert!(schema.field("email").is_none());
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

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
                    metadata: None,
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
                    sql:
                        "create trigger trg_users_bi before insert on users for each row begin end"
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

        let view_snapshot = app
            .node_data_dir
            .join(FileKind::Entity.file_name(common::helpers::stable_id(&["users_v"])));
        let trigger_snapshot = app
            .node_data_dir
            .join(FileKind::Entity.file_name(common::helpers::stable_id(&["trg_users_bi"])));
        let procedure_snapshot = app
            .node_data_dir
            .join(FileKind::Entity.file_name(common::helpers::stable_id(&["p_sync"])));

        let view_wal = app
            .node_data_dir
            .join(FileKind::Data.file_name(common::helpers::stable_id(&["users_v"])));
        let trigger_wal = app
            .node_data_dir
            .join(FileKind::Data.file_name(common::helpers::stable_id(&["trg_users_bi"])));
        let procedure_wal = app
            .node_data_dir
            .join(FileKind::Data.file_name(common::helpers::stable_id(&["p_sync"])));

        assert!(view_snapshot.exists());
        assert!(trigger_snapshot.exists());
        assert!(procedure_snapshot.exists());
        assert!(view_wal.exists());
        assert!(trigger_wal.exists());
        assert!(procedure_wal.exists());

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
        assert!(!view_snapshot.exists());
        assert!(!trigger_snapshot.exists());
        assert!(!procedure_snapshot.exists());
        assert!(!view_wal.exists());
        assert!(!trigger_wal.exists());
        assert!(!procedure_wal.exists());
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

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
                    metadata: None,
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
                    metadata: None,
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

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
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "email".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::Indexed,
                        default_value: None,
                        metadata: None,
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

        let mut catalog =
            DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

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
                        metadata: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "email".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::Indexed,
                        default_value: None,
                        metadata: None,
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
