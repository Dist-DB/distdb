use std::collections::{HashMap, HashSet};
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
use serverlib::engine::database::runtime_index::derived_indexes_for_table;
use serverlib::{primary_key_index, RuntimeIndexStore};
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    AlterTableChangeOp,
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
    runtime_indexes: &mut RuntimeIndexStore,
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
                runtime_indexes,
                parsed,
            );
            with_query_timings(response, make_query_timings(request_start, parse_ms))
        },

        Err(err) => ConnectorResponse::rejected(request_id.to_string(), format!("sql parse failed: {err}")),
        
    }
    
}

type QueryOperationHandler = fn(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse;

fn execute_parsed_query(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    parsed: Vec<SqlRequest>,
) -> ConnectorResponse {

    if parsed.len() != 1 {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "multi-statement query execution is not wired yet",
        );
    }

    let statement = &parsed[0];

    // Insert is handled separately because it needs mutable access to runtime_indexes.
    if statement.operation == SqlOperation::Insert {
        return execute_insert(request_id, query, catalogs, wal, node_data_dir, runtime_indexes, statement);
    }

    let handler: Option<QueryOperationHandler> = match statement.operation {
        SqlOperation::CreateDatabase => Some(execute_create_database),
        SqlOperation::CreateTable => Some(execute_create_table),
        SqlOperation::DropDatabase
        | SqlOperation::DropTable
        | SqlOperation::DropView
        | SqlOperation::DropTrigger
        | SqlOperation::DropStoredProcedure => Some(execute_drop_directive),
        SqlOperation::Select => Some(execute_select),
        SqlOperation::CreateView => Some(execute_create_view),
        SqlOperation::CreateTrigger => Some(execute_create_trigger),
        SqlOperation::CreateStoredProcedure => Some(execute_create_stored_procedure),
        SqlOperation::AlterTable => Some(execute_alter_table),
        _ => None,
    };

    match handler {
        Some(handler) => handler(request_id, query, catalogs, wal, node_data_dir, statement),
        None => ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "query operation '{:?}' execution is not wired yet",
                statement.operation
            ),
        ),
    }

}

fn execute_alter_table(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let plan = match serverlib::parse_alter_table_change_plan_from_statement(&statement.sql) {
        Ok(plan) => plan,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("alter table parse failed: {err}"),
            )
        }
    };

    let table_id = common::normalize_identifier!(plan.table_id);
    if catalog.table(&table_id).is_none() {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("alter table failed: table '{}' not found", table_id),
        );
    }

    let mut tx = match catalog.begin_schema_change(&table_id) {
        Ok(tx) => tx,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("alter table failed: {err}"),
            )
        }
    };

    let mut renames = Vec::new();
    let mut removals = Vec::new();
    let mut additions = Vec::new();
    let mut type_changes = Vec::new();

    for operation in plan.operations {

        let apply_result = match operation {

            AlterTableChangeOp::AddField(mut field) => {
                let next_seqno = tx
                    .pending_schema()
                    .fields
                    .iter()
                    .map(|f| f.seqno)
                    .max()
                    .unwrap_or(0)
                    .saturating_add(1);
                field.seqno = next_seqno;
                additions.push((field.field_name.clone(), field.field_type.clone()));
                tx.add_field(field)
            },

            AlterTableChangeOp::DropField(name) => {
                removals.push(name.clone());
                tx.remove_field(&name)
            },

            AlterTableChangeOp::RenameField { from, to } => {
                let existing = tx.pending_schema().field(&from).cloned();
                match existing {
                    Some(mut field) => {
                        renames.push((from.clone(), to.clone()));
                        if let Err(err) = tx.remove_field(&from) {
                            Err(err)
                        } else {
                            field.field_name = to;
                            tx.add_field(field)
                        }
                    }
                    None => Err(serverlib::DatabaseError::SchemaChange(
                        serverlib::SchemaError::FieldNotFound,
                    )),
                }
            },

            AlterTableChangeOp::ModifyField { field_name, new_type } => {
                type_changes.push(serverlib::FieldTypeChangeRule {
                    field_name: field_name.clone(),
                    target_type: new_type.clone(),
                });
                Ok(())
            }

        };

        if let Err(err) = apply_result {
            let _ = tx.abort(catalog);
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("alter table failed: {err}"),
            );
        }

    }

    let wal_id = catalog.database_id.0.clone();
    let entity_wal_id = table_id.clone();
    let created_at = common::epochabs!() as u64;

    if let Err(err) = tx.commit::<serverlib::DatabaseError, _>(catalog, |payload| {

        let encoded = payload
            .encode()
            .map_err(|_| serverlib::DatabaseError::CatalogSerialize)?;

        append_payload_record(
            wal,
            &wal_id,
            TransactionKind::SchemaChange,
            encoded.clone(),
            created_at,
        )
        .map_err(|_| serverlib::DatabaseError::CatalogWrite)?;

        append_payload_record(
            wal,
            &entity_wal_id,
            TransactionKind::SchemaChange,
            encoded,
            created_at,
        )
        .map_err(|_| serverlib::DatabaseError::CatalogWrite)?;

        Ok(())

    }) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("alter table failed: {err}"),
        );
    }

    // If there are type changes or field modifications, run the migration executor
    if !type_changes.is_empty() {

        let mutation_rule_set = serverlib::SchemaMutationRuleSet {
            renames,
            removals,
            additions: additions.into_iter().map(|(name, _)| (name, vec![])).collect(),
            type_changes,
            conversion_policy: serverlib::TypeConversionPolicy::Safe,
        };

        let executor = serverlib::DiskToMemorySchemaMigrationExecutor::new(node_data_dir.to_path_buf());
        executor
            .set_rules_for_table(&table_id, mutation_rule_set)
            .ok();

        if let Err(err) = serverlib::run_schema_migration(catalog, &table_id, &executor) {
            log::warn!(
                "schema migration failed for table '{}': {err}",
                table_id
            );
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("alter table schema migration failed: {err}"),
            );
        }

    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )

}

fn execute_create_database(
    request_id: &str,
    _query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    _wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
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

    if let Err(err) = append_payload_record_pair(
        wal,
        &wal_id,
        &entity_wal_id,
        TransactionKind::SchemaChange,
        encoded_schema,
        created_at,
        "create table schema WAL append failed",
        "create table entity WAL append failed",
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            err,
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

    if let Err(err) = append_payload_record_pair(
        wal,
        &wal_id,
        &entity_wal_id,
        TransactionKind::TableLifecycle,
        encoded_lifecycle,
        created_at,
        "create table lifecycle WAL append failed",
        "create table entity lifecycle WAL append failed",
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            err,
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

    if let Err(err) = append_payload_record_pair(
        wal,
        &wal_id,
        &entity_wal_id,
        TransactionKind::MetadataChange,
        encoded_metadata,
        created_at,
        "create table metadata WAL append failed",
        "create table entity metadata WAL append failed",
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            err,
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
        _ if drop_entity_operation_metadata(statement.operation).is_some() => {
            execute_drop_entity_object(request_id, query, catalogs, wal, node_data_dir, statement)
        }
        _ => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop directive is not supported for operation '{:?}'", statement.operation),
        ),
    }

}

fn drop_entity_operation_metadata(
    operation: SqlOperation,
) -> Option<(DatabaseObjectType, &'static str, Option<SqlObjectKind>)> {
    match operation {
        SqlOperation::DropTable => Some((DatabaseObjectType::Table, "table", None)),
        SqlOperation::DropView => Some((DatabaseObjectType::View, "view", Some(SqlObjectKind::View))),
        SqlOperation::DropTrigger => {
            Some((DatabaseObjectType::Trigger, "trigger", Some(SqlObjectKind::Trigger)))
        }
        SqlOperation::DropStoredProcedure => Some((
            DatabaseObjectType::StoredProcedure,
            "stored procedure",
            Some(SqlObjectKind::StoredProcedure),
        )),
        _ => None,
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
    let Some((object_type, kind_label, sql_object_kind)) =
        drop_entity_operation_metadata(statement.operation)
    else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "drop entity-object handler does not support operation '{:?}'",
                statement.operation
            ),
        );
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

fn execute_insert(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let Some(catalog) = resolve_catalog(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let plan = match serverlib::parse_insert_rows_from_statement(&statement.sql) {
        Ok(plan) => plan,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("insert parse failed: {err}"),
            )
        }
    };

    let Some(schema) = catalog.table_schema(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id,
                query.database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id,
                query.database_id
            ),
        );
    };

    let columns = if plan.columns.is_empty() {
        schema
            .fields
            .iter()
            .map(|field| field.field_name.clone())
            .collect::<Vec<_>>()
    } else {
        plan.columns.clone()
    };

    let mut seen = HashSet::with_capacity(columns.len());
    for column in &columns {

        if !seen.insert(column.clone()) {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("insert failed: duplicate column '{}'", column),
            );
        }

        if schema.field(column).is_none() {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("insert failed: unknown column '{}'", column),
            );
        }

    }

    let mut affected_rows = 0u64;
    for row in &plan.rows {

        if row.len() != columns.len() {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "insert failed: values count {} does not match columns count {}",
                    row.len(),
                    columns.len()
                ),
            );
        }

        let mut payload_row = HashMap::with_capacity(schema.fields.len());

        for (column, value) in columns.iter().zip(row.iter()) {

            let field = schema
                .field(column)
                .expect("column existence already validated");

            match value {

                Some(value_bytes) => {
                    payload_row.insert(column.clone(), value_bytes.clone());
                },

                None => {
                    if let Some(default) = &field.default_value {
                        payload_row.insert(column.clone(), default.clone());
                    } else if !field.nullable {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert failed: column '{}' cannot be null", column),
                        );
                    }
                }

            }

        }

        for field in &schema.fields {

            if payload_row.contains_key(&field.field_name) {
                continue;
            }

            if let Some(default) = &field.default_value {
                payload_row.insert(field.field_name.clone(), default.clone());
                continue;
            }

            if !field.nullable {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("insert failed: missing required column '{}'", field.field_name),
                );
            }

        }

        // Primary key uniqueness check
        if let Some(pk_index) = primary_key_index(table) {
            let pk_fields = if pk_index.field_names.is_empty() && !pk_index.field_name.is_empty() {
                vec![pk_index.field_name.clone()]
            } else {
                pk_index.field_names.clone()
            };

            let incoming_pk = pk_fields
                .iter()
                .map(|pk| payload_row.get(pk).cloned().unwrap_or_default())
                .collect::<Vec<_>>();

            let pk_runtime = runtime_indexes.index(&pk_index.index_id.0);
            if pk_runtime.map(|idx| idx.contains(&incoming_pk)).unwrap_or(false) {
                let pk_display = pk_fields
                    .iter()
                    .zip(incoming_pk.iter())
                    .map(|(name, val)| format!("{}={}", name, String::from_utf8_lossy(val)))
                    .collect::<Vec<_>>()
                    .join(", ");
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("insert failed: duplicate primary key ({})", pk_display),
                );
            }
        }

        let encoded = match bincode::serialize(&payload_row) {

            Ok(encoded) => encoded,
            
            Err(err) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("insert payload encode failed: {err}"),
                )
            }

        };

        if let Err(err) = append_row_payload_record(
            wal,
            &plan.table_id,
            table,
            runtime_indexes,
            TransactionKind::Insert,
            encoded,
            common::epochabs!() as u64,
        ) {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("insert WAL append failed: {err}"),
            );
        }

        affected_rows = affected_rows.saturating_add(1);

    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows }),
    )

}

fn execute_select(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
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

    // Replay the table's WAL to reconstruct live rows:
    // - Insert records add a row
    // - Delete records (by refid) remove a row
    let wal_records = wal.since(table_id, None);

    // Track rows by their WAL transaction id so Delete records can remove them.
    let mut live_rows: Vec<(u64, HashMap<String, Vec<u8>>)> = Vec::new();
    let mut deleted_ids: HashSet<u64> = HashSet::new();

    for record in &wal_records {
        match record.kind {
            TransactionKind::Insert => {
                match bincode::deserialize::<HashMap<String, Vec<u8>>>(&record.payload) {
                    Ok(row_map) => live_rows.push((record.id.0, row_map)),
                    Err(_) => continue,
                }
            }
            TransactionKind::Delete => {
                if let Some(refid) = record.refid {
                    deleted_ids.insert(refid.0);
                }
            }
            _ => {}
        }
    }

    let rows = live_rows
        .into_iter()
        .filter(|(id, _)| !deleted_ids.contains(id))
        .map(|(_, row_map)| {
            columns
                .iter()
                .map(|col| {
                    row_map
                        .get(&col.field_name)
                        .cloned()
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns,
            rows,
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
            if let Err(err) = apply_entity_metadata_with_wal(
                catalog,
                wal,
                &wal_id,
                &entity_wal_id,
                view_id,
                created_at,
                "create view",
            ) {
                return ConnectorResponse::rejected(request_id.to_string(), err);
            }

            if let Err(err) = append_sql_definition_upsert_with_wal(
                catalog,
                wal,
                &wal_id,
                &entity_wal_id,
                view_id,
                SqlObjectKind::View,
                &statement.sql,
                created_at,
                "create view",
            ) {
                return ConnectorResponse::rejected(request_id.to_string(), err);
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
        
        },

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

    if let Err(err) = apply_entity_metadata_with_wal(
        catalog,
        wal,
        &wal_id,
        &entity_wal_id,
        trigger_id,
        created_at,
        "create trigger",
    ) {
        return ConnectorResponse::rejected(request_id.to_string(), err);
    }

    if let Err(err) = append_sql_definition_upsert_with_wal(
        catalog,
        wal,
        &wal_id,
        &entity_wal_id,
        trigger_id,
        SqlObjectKind::Trigger,
        &statement.sql,
        created_at,
        "create trigger",
    ) {
        return ConnectorResponse::rejected(request_id.to_string(), err);
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

    if let Err(err) = apply_entity_metadata_with_wal(
        catalog,
        wal,
        &wal_id,
        &entity_wal_id,
        procedure_id,
        created_at,
        "create procedure",
    ) {
        return ConnectorResponse::rejected(request_id.to_string(), err);
    }

    if let Err(err) = append_sql_definition_upsert_with_wal(
        catalog,
        wal,
        &wal_id,
        &entity_wal_id,
        procedure_id,
        SqlObjectKind::StoredProcedure,
        &statement.sql,
        created_at,
        "create procedure",
    ) {
        return ConnectorResponse::rejected(request_id.to_string(), err);
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

/// Scans the WAL for a table and returns the set of PK value tuples for all live rows
/// (inserts minus deletes). Each entry is a Vec of raw byte values, one per PK field,
/// in the same order as `pk_fields`.
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

fn append_row_payload_record(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
) -> Result<(), String> {

    let existing = wal.since(wal_id, None);
    let last = existing.last();
    let next_id = TransactionId(last.map(|record| record.id.0 + 1).unwrap_or(1));
    let refid = last.map(|record| record.id);

    let row_map = bincode::deserialize::<HashMap<String, Vec<u8>>>(&payload)
        .map_err(|err| format!("row payload decode failed: {err}"))?;

    let record = TransactionRecord {
        id: next_id,
        refid,
        timestamp_epoch_ms,
        actor: UserId::from_username("server"),
        kind: kind.clone(),
        payload,
    };

    wal.append(wal_id, record)
        .map_err(|e| e.to_string())?;

    runtime_indexes.apply_table_row_mutation(
        derived_indexes_for_table(table),
        kind,
        &row_map,
    );

    Ok(())
}

fn append_payload_record_pair(
    wal: &ConcurrentWalManager,
    database_wal_id: &str,
    entity_wal_id: &str,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    database_error_context: &str,
    entity_error_context: &str,
) -> Result<(), String> {
    
    append_payload_record(
        wal,
        database_wal_id,
        kind.clone(),
        payload.clone(),
        timestamp_epoch_ms,
    )
    .map_err(|err| format!("{database_error_context}: {err}"))?;

    append_payload_record(
        wal,
        entity_wal_id,
        kind,
        payload,
        timestamp_epoch_ms,
    )
    .map_err(|err| format!("{entity_error_context}: {err}"))?;

    Ok(())
}

fn apply_entity_metadata_with_wal(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    entity_wal_id: &str,
    entity_id: &str,
    created_at: u64,
    operation_label: &str,
) -> Result<(), String> {

    let metadata = EntityMetadata::default()
        .with_creator("server")
        .with_created_at(created_at);

    catalog
        .set_entity_metadata(entity_id, metadata.clone())
        .map_err(|err| format!("{operation_label} metadata apply failed: {err}"))?;

    let payload = EntityMetadataPayload {
        entity_id: entity_id.to_string(),
        metadata,
    };

    let encoded = payload
        .encode()
        .map_err(|err| format!("{operation_label} metadata payload encode failed: {err}"))?;

    append_payload_record_pair(
        wal,
        wal_id,
        entity_wal_id,
        TransactionKind::MetadataChange,
        encoded,
        created_at,
        &format!("{operation_label} metadata WAL append failed"),
        &format!("{operation_label} entity metadata WAL append failed"),
    )
    
}

fn append_sql_definition_upsert_with_wal(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    wal_id: &str,
    entity_wal_id: &str,
    object_id: &str,
    object_kind: SqlObjectKind,
    sql: &str,
    created_at: u64,
    operation_label: &str,
) -> Result<(), String> {

    let payload = SqlDefinitionPayload {
        object_id: object_id.to_string(),
        object_kind,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: catalog.schema_epoch(),
        sql: sql.to_string(),
        dependencies: Vec::new(),
    };

    let encoded = payload
        .encode()
        .map_err(|err| format!("{operation_label} sql payload encode failed: {err}"))?;

    append_payload_record_pair(
        wal,
        wal_id,
        entity_wal_id,
        TransactionKind::SqlDefinitionChange,
        encoded,
        created_at,
        &format!("{operation_label} sql definition WAL append failed"),
        &format!("{operation_label} entity sql WAL append failed"),
    )

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
