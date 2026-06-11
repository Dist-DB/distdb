use std::collections::HashMap;
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Instant;

use common::helpers::format::{make_header, FileKind, HEADER_SIZE};
use common::helpers::write_bytes;
use connector::{
    ConnectorResponse, ConnectorResult, DataQuery, FieldDef, FieldIndex, FieldType, QueryResult,
    QueryTimings, MutationResult,
};
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseId, EntityMetadata, EntityMetadataPayload,
    SchemaChangePayload,
    DatabaseObjectType,
    SqlDefinitionAction, SqlDefinitionPayload, SqlObjectKind, SqlOperation, SqlRequest,
    TableLifecycleAction, TableLifecyclePayload,
    TableSchema, TransactionId, TransactionKind, TransactionRecord, UserId,
};

pub(crate) fn handle_query_command(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
) -> ConnectorResponse {

    let request_start = Instant::now();
    let parse_start = Instant::now();
    
    match serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) {
        Ok(parsed) => {
            let parse_ms = parse_start.elapsed().as_millis() as u64;
            let response = execute_parsed_query(
                request_id,
                query,
                catalogs,
                wal,
                node_data_dir,
                parsed,
            );
            with_query_timings(response, make_query_timings(request_start, parse_ms))
        }
        Err(err) => ConnectorResponse::rejected(request_id.to_string(), format!("sql parse failed: {err}")),
    }
    
}

fn execute_parsed_query(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    parsed: Vec<SqlRequest>,
) -> ConnectorResponse {

    if parsed.len() != 1 {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "multi-statement query execution is not wired yet",
        );
    }

    let statement = &parsed[0];

    match statement.operation {
        SqlOperation::CreateDatabase => {
            execute_create_database(request_id, statement, catalogs, node_data_dir)
        }
        SqlOperation::CreateTable => {
            execute_create_table(request_id, query, catalogs, wal, node_data_dir, statement)
        }
        SqlOperation::DropDatabase
        | SqlOperation::DropTable
        | SqlOperation::DropView
        | SqlOperation::DropTrigger
        | SqlOperation::DropStoredProcedure => execute_drop_directive(
            request_id,
            query,
            catalogs,
            wal,
            node_data_dir,
            statement,
        ),
        SqlOperation::Select => execute_select(request_id, query, catalogs, statement),
        SqlOperation::CreateView => {
            execute_create_view(request_id, query, catalogs, wal, node_data_dir, statement)
        }
        SqlOperation::CreateTrigger => {
            execute_create_trigger(request_id, query, catalogs, wal, node_data_dir, statement)
        }
        SqlOperation::CreateStoredProcedure => {
            execute_create_stored_procedure(request_id, query, catalogs, wal, node_data_dir, statement)
        }
        _ => ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "query operation '{:?}' execution is not wired yet",
                statement.operation
            ),
        ),
    }
}

fn execute_create_database(
    request_id: &str,
    statement: &SqlRequest,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    node_data_dir: &Path,
) -> ConnectorResponse {
    let Some(database_name) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "create database missing database identifier",
        );
    };

    match DatabaseCatalog::create_new_database(database_name, node_data_dir) {
        Ok(catalog) => {
            catalogs.insert(catalog.database_id.0.clone(), catalog);
            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
            )
        }
        Err(err) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create database failed: {err}"),
        ),
    }
}

fn execute_create_table(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let (table_id, schema) = match serverlib::create_table_schema_from_statement(&statement.sql) {
        Ok(tuple) => tuple,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create table schema parse failed: {err}"),
            )
        }
    };

    let normalized_table_id = common::normalize_identifier!(table_id);
    if catalog.table(&normalized_table_id).is_some() {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table failed: table '{}' already exists", normalized_table_id),
        );
    }

    let created_at = common::epochabs!() as u64;
    if let Err(err) = catalog.create_table(normalized_table_id.clone(), schema.clone()) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table failed: {err}"),
        );
    }

    let wal_id = catalog.database_id.0.clone();
    let entity_wal_id = normalized_table_id.clone();
    let schema_payload = SchemaChangePayload {
        table_id: normalized_table_id.clone(),
        schema_revision: 1,
        schema_epoch: catalog.schema_epoch(),
        schema,
    };

    let encoded_schema = match schema_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create table schema payload encode failed: {err}"),
            )
        }
    };

    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::SchemaChange,
        encoded_schema.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table schema WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::SchemaChange,
        encoded_schema,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table entity WAL append failed: {err}"),
        );
    }

    let lifecycle_payload = TableLifecyclePayload {
        table_id: normalized_table_id.clone(),
        action: TableLifecycleAction::Create,
        schema_epoch: catalog.schema_epoch(),
        schema: Some(
            catalog
                .table_schema(&normalized_table_id)
                .cloned()
                .unwrap_or_else(|| TableSchema::new(Vec::new())),
        ),
    };

    let encoded_lifecycle = match lifecycle_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create table lifecycle payload encode failed: {err}"),
            )
        }
    };

    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::TableLifecycle,
        encoded_lifecycle.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table lifecycle WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::TableLifecycle,
        encoded_lifecycle,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table entity lifecycle WAL append failed: {err}"),
        );
    }

    let metadata = EntityMetadata::default()
        .with_creator("server")
        .with_created_at(created_at);

    if let Err(err) = catalog.set_entity_metadata(&normalized_table_id, metadata.clone()) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table metadata apply failed: {err}"),
        );
    }

    let metadata_payload = EntityMetadataPayload {
        entity_id: normalized_table_id.clone(),
        metadata,
    };

    let encoded_metadata = match metadata_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create table metadata payload encode failed: {err}"),
            )
        }
    };

    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table metadata WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create table entity metadata WAL append failed: {err}"),
        );
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )
}

fn execute_drop_directive(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {
    match statement.operation {
        SqlOperation::DropDatabase => execute_drop_database(request_id, catalogs, node_data_dir, statement),
        SqlOperation::DropTable
        | SqlOperation::DropView
        | SqlOperation::DropTrigger
        | SqlOperation::DropStoredProcedure => {
            execute_drop_entity_object(request_id, query, catalogs, wal, node_data_dir, statement)
        }
        _ => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop directive is not supported for operation '{:?}'", statement.operation),
        ),
    }
}

fn execute_drop_database(
    request_id: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(database_name) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "drop database missing database identifier",
        );
    };

    let direct_key = database_name.to_string();
    let normalized_key = DatabaseId::from_database_name(database_name)
        .map(|dbid| dbid.0)
        .ok();

    let removed = if catalogs.contains_key(&direct_key) {
        catalogs.remove(&direct_key)
    } else if let Some(key) = normalized_key.as_ref() {
        catalogs.remove(key)
    } else {
        None
    };

    let Some(catalog) = removed else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop database failed: database '{}' not found", database_name),
        );
    };

    let catalog_file = node_data_dir.join(catalog.file_name());
    if let Err(err) = fs::remove_file(&catalog_file) {
        if err.kind() != ErrorKind::NotFound {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("drop database failed: cannot remove catalog file: {err}"),
            );
        }
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )
}

fn execute_drop_entity_object(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let Some(object_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop operation '{:?}' missing object identifier", statement.operation),
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let normalized_object_id = common::normalize_identifier!(object_id);
    let (object_type, kind_label, sql_object_kind) = match statement.operation {
        SqlOperation::DropTable => (DatabaseObjectType::Table, "table", None),
        SqlOperation::DropView => (DatabaseObjectType::View, "view", Some(SqlObjectKind::View)),
        SqlOperation::DropTrigger => {
            (DatabaseObjectType::Trigger, "trigger", Some(SqlObjectKind::Trigger))
        }
        SqlOperation::DropStoredProcedure => (
            DatabaseObjectType::StoredProcedure,
            "stored procedure",
            Some(SqlObjectKind::StoredProcedure),
        ),
        _ => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "drop entity-object handler does not support operation '{:?}'",
                    statement.operation
                ),
            )
        }
    };

    if catalog.object(object_type, &normalized_object_id).is_none() {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop {kind_label} failed: '{normalized_object_id}' not found"),
        );
    }

    let entity_wal_stream_id = catalog.entity_wal_stream_id(&normalized_object_id);

    if let Err(err) = catalog.drop_object(object_type, &normalized_object_id) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop {kind_label} failed: {err}"),
        );
    }

    if let Err(err) = remove_entity_snapshot_file(node_data_dir, &normalized_object_id) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop {kind_label} entity snapshot cleanup failed: {err}"),
        );
    }

    let wal_id = catalog.database_id.0.clone();

    // Only remove a dedicated entity stream. SQL-backed objects currently
    // share the database stream and must not delete it.
    if let Some(stream_id) = entity_wal_stream_id {
        if stream_id != wal_id {
            if let Err(err) = wal.delete_stream(&stream_id) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("drop {kind_label} WAL delete failed: {err}"),
                );
            }
        }
    }

    let timestamp = common::epochabs!() as u64;

    if object_type == DatabaseObjectType::Table {
        
        let lifecycle_payload = TableLifecyclePayload {
            table_id: normalized_object_id.clone(),
            action: TableLifecycleAction::Drop,
            schema_epoch: catalog.schema_epoch(),
            schema: None,
        };

        let encoded = match lifecycle_payload.encode() {
            Ok(encoded) => encoded,
            Err(err) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("drop table lifecycle payload encode failed: {err}"),
                )
            }
        };

        if let Err(err) = append_payload_record(
            wal,
            &wal_id,
            TransactionKind::TableLifecycle,
            encoded,
            timestamp,
        ) {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("drop table lifecycle WAL append failed: {err}"),
            );
        }

        if let Err(err) = remove_table_stream_files(node_data_dir, &normalized_object_id) {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("drop table cleanup failed: {err}"),
            );
        }

    } else {

        let sql_payload = SqlDefinitionPayload {
            object_id: normalized_object_id,
            object_kind: sql_object_kind.expect("sql object kind should be present for non-table drop"),
            action: SqlDefinitionAction::Drop,
            schema_epoch: catalog.schema_epoch(),
            sql: String::new(),
            dependencies: Vec::new(),
        };

        let encoded = match sql_payload.encode() {
            Ok(encoded) => encoded,
            Err(err) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("drop sql payload encode failed: {err}"),
                )
            }
        };

        if let Err(err) = append_payload_record(
            wal,
            &wal_id,
            TransactionKind::SqlDefinitionChange,
            encoded,
            timestamp,
        ) {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("drop sql definition WAL append failed: {err}"),
            );
        }

    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )

}

fn execute_select(
    request_id: &str,
    query: &DataQuery,
    catalogs: &HashMap<String, DatabaseCatalog>,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let statement_sql_lower = statement.sql.to_ascii_lowercase();

    if statement_sql_lower.starts_with("show databases") {
        let mut database_ids = catalogs.keys().cloned().collect::<Vec<_>>();
        database_ids.sort();

        let rows = database_ids
            .into_iter()
            .map(|database_id| vec![database_id.into_bytes()])
            .collect::<Vec<_>>();

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: vec![FieldDef {
                    seqno: 1,
                    field_name: "database_name".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                metadata: None,
                }],
                rows,
                timings: empty_query_timings(),
            }),
        );

    }

    if statement_sql_lower.starts_with("show tables") {

        let target_db = statement
            .object_name
            .as_deref()
            .unwrap_or(&query.database_id);

        let Some(catalog) = resolve_catalog(catalogs, target_db) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", target_db),
            );
        };

        let mut table_ids = catalog.table_ids();
        table_ids.sort();
        
        let rows = table_ids
            .into_iter()
            .map(|table_id| vec![table_id.into_bytes()])
            .collect::<Vec<_>>();

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: vec![FieldDef {
                    seqno: 1,
                    field_name: "table_name".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                metadata: None,
                }],
                rows,
                timings: empty_query_timings(),
            }),
        );

    }

    let Some(catalog) = resolve_catalog(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    if statement_sql_lower.starts_with("describe ")
        || statement_sql_lower.starts_with("desc ")
        || statement_sql_lower.starts_with("show columns")
    {
        let Some(object_name) = statement.object_name.as_deref() else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                "describe/show columns missing table identifier",
            );
        };

        let table_id = object_name.rsplit('.').next().unwrap_or(object_name);
        let Some(schema) = catalog.table_schema(table_id) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("table '{}' not found in database '{}'", table_id, query.database_id),
            );
        };

        let rows = schema
            .fields
            .iter()
            .map(|field| {
                let nullable = if field.nullable { "YES" } else { "NO" };
                let key = match field.indexed {
                    FieldIndex::PrimaryKey => "PRI",
                    FieldIndex::Indexed => "MUL",
                    FieldIndex::None => "",
                };
                let default_value = field
                    .default_value
                    .as_ref()
                    .map(|value| String::from_utf8_lossy(value).to_string())
                    .unwrap_or_else(|| "NULL".to_string());

                vec![
                    field.field_name.clone().into_bytes(),
                    field
                        .metadata
                        .as_ref()
                        .and_then(|meta| meta.original_sql_type.clone())
                        .unwrap_or_else(|| format!("{:?}", field.field_type))
                        .into_bytes(),
                    nullable.as_bytes().to_vec(),
                    key.as_bytes().to_vec(),
                    default_value.into_bytes(),
                ]
            })
            .collect::<Vec<_>>();

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: vec![
                    FieldDef {
                        seqno: 1,
                        field_name: "field".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    metadata: None,
                    },
                    FieldDef {
                        seqno: 2,
                        field_name: "type".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    metadata: None,
                    },
                    FieldDef {
                        seqno: 3,
                        field_name: "null".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    metadata: None,
                    },
                    FieldDef {
                        seqno: 4,
                        field_name: "key".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    metadata: None,
                    },
                    FieldDef {
                        seqno: 5,
                        field_name: "default".to_string(),
                        field_type: FieldType::Text,
                        nullable: false,
                        indexed: FieldIndex::None,
                        default_value: None,
                    metadata: None,
                    },
                ],
                rows,
                timings: empty_query_timings(),
            }),
        );
    }

    let Some(object_name) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "select statements without a table source are not wired yet",
        );
    };

    let table_id = object_name.rsplit('.').next().unwrap_or(object_name);
    let Some(schema) = catalog.table_schema(table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("table '{}' not found in database '{}'", table_id, query.database_id),
        );
    };

    let columns = schema
        .fields
        .iter()
        .map(|field| FieldDef {
            seqno: field.seqno,
            field_name: field.field_name.clone(),
            field_type: field.field_type.clone(),
            nullable: field.nullable,
            indexed: field.indexed,
            default_value: field.default_value.clone(),
            metadata: field.metadata.clone(),
        })
        .collect::<Vec<_>>();

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns,
            rows: Vec::new(),
            timings: empty_query_timings(),
        }),
    )

}

fn execute_create_view(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(view_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "create view missing view identifier",
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let wal_id = catalog.database_id.0.clone();
    let entity_wal_id = common::normalize_identifier!(view_id);
    let created_at = common::epochabs!() as u64;

    match catalog.register_view(view_id, statement.sql.clone(), TableSchema::new(Vec::new())) {
        Ok(()) => {
            let metadata = EntityMetadata::default()
                .with_creator("server")
                .with_created_at(created_at);

            if let Err(err) = catalog.set_entity_metadata(view_id, metadata.clone()) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view metadata apply failed: {err}"),
                );
            }

            let metadata_payload = EntityMetadataPayload {
                entity_id: view_id.to_string(),
                metadata,
            };
            let encoded_metadata = match metadata_payload.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("create view metadata payload encode failed: {err}"),
                    )
                }
            };
            if let Err(err) = append_payload_record(
                wal,
                &wal_id,
                TransactionKind::MetadataChange,
                encoded_metadata.clone(),
                created_at,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view metadata WAL append failed: {err}"),
                );
            }

            if let Err(err) = append_payload_record(
                wal,
                &entity_wal_id,
                TransactionKind::MetadataChange,
                encoded_metadata,
                created_at,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view entity metadata WAL append failed: {err}"),
                );
            }

            let sql_payload = SqlDefinitionPayload {
                object_id: view_id.to_string(),
                object_kind: SqlObjectKind::View,
                action: SqlDefinitionAction::Upsert,
                schema_epoch: catalog.schema_epoch(),
                sql: statement.sql.clone(),
                dependencies: Vec::new(),
            };
            let encoded = match sql_payload.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("create view sql payload encode failed: {err}"),
                    )
                }
            };
            if let Err(err) = append_payload_record(
                wal,
                &wal_id,
                TransactionKind::SqlDefinitionChange,
                encoded.clone(),
                created_at,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view sql definition WAL append failed: {err}"),
                );
            }

            if let Err(err) = append_payload_record(
                wal,
                &entity_wal_id,
                TransactionKind::SqlDefinitionChange,
                encoded,
                created_at,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view entity sql WAL append failed: {err}"),
                );
            }

            if let Err(err) = persist_entity_snapshot(catalog, view_id, node_data_dir) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view entity snapshot write failed: {err}"),
                );
            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
            )
        }
        Err(err) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create view failed: {err}"),
        ),
    }
}

fn execute_create_trigger(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(trigger_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "create trigger missing trigger identifier",
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let wal_id = catalog.database_id.0.clone();
    let entity_wal_id = common::normalize_identifier!(trigger_id);
    let created_at = common::epochabs!() as u64;

    let created = catalog.register_trigger(trigger_id, statement.sql.clone(), Vec::new());
    if let Err(err) = created {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger failed: {err}"),
        );
    }

    if let Err(err) = catalog.set_sql_definition(
        trigger_id,
        SqlObjectKind::Trigger,
        statement.sql.clone(),
        Vec::new(),
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger definition apply failed: {err}"),
        );
    }

    let metadata = EntityMetadata::default()
        .with_creator("server")
        .with_created_at(created_at);
    if let Err(err) = catalog.set_entity_metadata(trigger_id, metadata.clone()) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger metadata apply failed: {err}"),
        );
    }

    let metadata_payload = EntityMetadataPayload {
        entity_id: trigger_id.to_string(),
        metadata,
    };
    let encoded_metadata = match metadata_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create trigger metadata payload encode failed: {err}"),
            )
        }
    };
    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger metadata WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger entity metadata WAL append failed: {err}"),
        );
    }

    let sql_payload = SqlDefinitionPayload {
        object_id: trigger_id.to_string(),
        object_kind: SqlObjectKind::Trigger,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: catalog.schema_epoch(),
        sql: statement.sql.clone(),
        dependencies: Vec::new(),
    };
    let encoded_sql = match sql_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create trigger sql payload encode failed: {err}"),
            )
        }
    };
    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::SqlDefinitionChange,
        encoded_sql.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger sql definition WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::SqlDefinitionChange,
        encoded_sql,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger entity sql WAL append failed: {err}"),
        );
    }

    if let Err(err) = persist_entity_snapshot(catalog, trigger_id, node_data_dir) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger entity snapshot write failed: {err}"),
        );
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )
}

fn execute_create_stored_procedure(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(procedure_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "create procedure missing identifier",
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let wal_id = catalog.database_id.0.clone();
    let entity_wal_id = common::normalize_identifier!(procedure_id);
    let created_at = common::epochabs!() as u64;

    let created = catalog.register_stored_procedure(
        procedure_id,
        statement.sql.clone(),
        Vec::new(),
    );
    if let Err(err) = created {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure failed: {err}"),
        );
    }

    if let Err(err) = catalog.set_sql_definition(
        procedure_id,
        SqlObjectKind::StoredProcedure,
        statement.sql.clone(),
        Vec::new(),
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure definition apply failed: {err}"),
        );
    }

    let metadata = EntityMetadata::default()
        .with_creator("server")
        .with_created_at(created_at);
    if let Err(err) = catalog.set_entity_metadata(procedure_id, metadata.clone()) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure metadata apply failed: {err}"),
        );
    }

    let metadata_payload = EntityMetadataPayload {
        entity_id: procedure_id.to_string(),
        metadata,
    };
    let encoded_metadata = match metadata_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create procedure metadata payload encode failed: {err}"),
            )
        }
    };
    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure metadata WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure entity metadata WAL append failed: {err}"),
        );
    }

    let sql_payload = SqlDefinitionPayload {
        object_id: procedure_id.to_string(),
        object_kind: SqlObjectKind::StoredProcedure,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: catalog.schema_epoch(),
        sql: statement.sql.clone(),
        dependencies: Vec::new(),
    };
    let encoded_sql = match sql_payload.encode() {
        Ok(encoded) => encoded,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create procedure sql payload encode failed: {err}"),
            )
        }
    };
    if let Err(err) = append_payload_record(
        wal,
        &wal_id,
        TransactionKind::SqlDefinitionChange,
        encoded_sql.clone(),
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure sql definition WAL append failed: {err}"),
        );
    }

    if let Err(err) = append_payload_record(
        wal,
        &entity_wal_id,
        TransactionKind::SqlDefinitionChange,
        encoded_sql,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure entity sql WAL append failed: {err}"),
        );
    }

    if let Err(err) = persist_entity_snapshot(catalog, procedure_id, node_data_dir) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure entity snapshot write failed: {err}"),
        );
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )
}

fn with_query_timings(mut response: ConnectorResponse, timings: QueryTimings) -> ConnectorResponse {
    if let ConnectorResult::Query(result) = &mut response.result {
        result.timings = timings;
    }

    response
}

fn empty_query_timings() -> QueryTimings {
    QueryTimings {
        server_parse_ms: 0,
        server_execute_ms: 0,
        server_total_ms: 0,
        network_round_trip_ms: None,
        cache: None,
    }
}

fn make_query_timings(request_start: Instant, parse_ms: u64) -> QueryTimings {
    let total_ms = request_start.elapsed().as_millis() as u64;
    QueryTimings {
        server_parse_ms: parse_ms,
        server_execute_ms: total_ms.saturating_sub(parse_ms),
        server_total_ms: total_ms,
        network_round_trip_ms: None,
        cache: None,
    }
}

fn append_payload_record(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
) -> Result<(), String> {
    let existing = wal.since(wal_id, None);
    let last = existing.last();
    let next_id = TransactionId(last.map(|record| record.id.0 + 1).unwrap_or(1));
    let refid = last.map(|record| record.id);

    wal.append(
        wal_id,
        TransactionRecord {
            id: next_id,
            refid,
            timestamp_epoch_ms,
            actor: UserId::from_username("server"),
            kind,
            payload,
        },
    )
    .map_err(|e| e.to_string())
}

fn remove_table_stream_files(node_data_dir: &Path, table_id: &str) -> Result<(), String> {
    let normalized_table_id = common::normalize_identifier!(table_id);

    // Keep compatibility with any legacy plain-name stream files while also
    // deleting the obfuscated stream file naming currently used by WAL.
    let candidates = [
        FileKind::Data.file_name(&normalized_table_id),
        FileKind::Data.file_name(common::helpers::stable_id(&[&normalized_table_id])),
    ];

    for file_name in candidates {
        let path = node_data_dir.join(file_name);
        if let Err(err) = fs::remove_file(&path) {
            if err.kind() != ErrorKind::NotFound {
                return Err(format!("cannot remove '{}': {err}", path.display()));
            }
        }
    }

    Ok(())
}

fn persist_entity_snapshot(
    catalog: &DatabaseCatalog,
    entity_id: &str,
    node_data_dir: &Path,
) -> Result<(), String> {
    let normalized_entity_id = common::normalize_identifier!(entity_id);
    let entity = catalog
        .entity(&normalized_entity_id)
        .ok_or_else(|| format!("entity '{}' not found in catalog", normalized_entity_id))?;

    let payload = bincode::serialize(entity)
        .map_err(|_| "failed to serialize entity snapshot".to_string())?;

    let mut file = Vec::with_capacity(HEADER_SIZE + payload.len());
    file.extend_from_slice(&make_header(FileKind::Entity));
    file.extend_from_slice(&payload);

    write_bytes(entity_snapshot_path(node_data_dir, &normalized_entity_id), &file)
        .map_err(|err| err.to_string())
}

fn remove_entity_snapshot_file(node_data_dir: &Path, entity_id: &str) -> Result<(), String> {
    let path = entity_snapshot_path(node_data_dir, entity_id);
    if let Err(err) = fs::remove_file(&path) {
        if err.kind() != ErrorKind::NotFound {
            return Err(format!("cannot remove '{}': {err}", path.display()));
        }
    }
    Ok(())
}

fn entity_snapshot_path(node_data_dir: &Path, entity_id: &str) -> PathBuf {
    let normalized_entity_id = common::normalize_identifier!(entity_id);
    let entity_stem = common::helpers::stable_id(&[&normalized_entity_id]);
    node_data_dir.join(FileKind::Entity.file_name(entity_stem))
}

fn resolve_catalog<'a>(
    catalogs: &'a HashMap<String, DatabaseCatalog>,
    database_id: &str,
) -> Option<&'a DatabaseCatalog> {
    catalogs.get(database_id).or_else(|| {
        DatabaseId::from_database_name(database_id)
            .ok()
            .and_then(|dbid| catalogs.get(&dbid.0))
    })
}

fn resolve_catalog_mut<'a>(
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    database_id: &str,
) -> Option<&'a mut DatabaseCatalog> {
    if catalogs.contains_key(database_id) {
        return catalogs.get_mut(database_id);
    }

    let normalized = DatabaseId::from_database_name(database_id).ok()?.0;
    catalogs.get_mut(&normalized)
}
