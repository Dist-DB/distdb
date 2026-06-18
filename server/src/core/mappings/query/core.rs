
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::cell::RefCell;

use common::helpers::format::{FileKind, HEADER_SIZE, make_header};
use common::helpers::write_bytes;
use connector::{ConnectorResponse, ConnectorResult, DataQuery, MutationResult, QueryResult};
use serverlib::engine::database::inbuilt::{
    evaluate_inbuilt_sql_function,
    with_inbuilt_sql_runtime_context,
    InbuiltSqlRuntimeContext,
};
use serverlib::engine::database::runtime_index::derived_indexes_for_table;
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    AlterTableChangeOp, ConcurrentWalManager, DatabaseCatalog, DatabaseId, DatabaseObjectType,
    EntityMetadata, EntityMetadataPayload, SchemaChangePayload, SelectCondition,
    SqlDefinitionAction, SqlDefinitionPayload, SqlObjectKind, SqlOperation, SqlRequest,
    TableLifecycleAction, TableLifecyclePayload, TableSchema, TransactionId, TransactionKind,
    TransactionRecord, UserId,
};

use serverlib::{
    RuntimeIndexStore, collect_indexable_equality_filters, count_condition_predicates,
    decode_row_payload, encode_row_payload, index_value_tuple, load_live_rows,
    plan_relation_access, primary_key_index,
};

thread_local! {
    static LAST_INSERT_ID_CONTEXT: RefCell<i64> = RefCell::new(0);
}

pub(crate) fn get_and_clear_last_insert_id() -> Option<i64> {
    LAST_INSERT_ID_CONTEXT.with(|ctx| {
        let mut val = ctx.borrow_mut();
        if *val > 0 {
            let result = Some(*val);
            *val = 0;
            result
        } else {
            None
        }
    })
}

use super::catalogs::{resolve_catalog, resolve_catalog_mut};
use super::explain::{
    connector_field_defs, explain_inner_statement, explain_join_mutation_plan,
    explain_mutation_plan, explain_select_plan,
};

use super::timings::{empty_query_timings, make_query_timings, with_query_timings};



pub(crate) fn handle_query_command(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
) -> ConnectorResponse {

    let runtime_context = inbuilt_runtime_context_for_query(
        request_id,
        query,
        session_id,
        connection_id,
        session_user,
    );

    with_inbuilt_sql_runtime_context(&runtime_context, || {
        handle_query_command_internal(
            request_id,
            query,
            catalogs,
            wal,
            node_data_dir,
            runtime_indexes,
            None,
            None,
            session_id,
        )
    })

}

pub(crate) fn handle_query_command_in_write_group(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    write_group_id: TransactionId,
    touched_tables: &mut HashSet<String>,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
) -> ConnectorResponse {

    let runtime_context = inbuilt_runtime_context_for_query(
        request_id,
        query,
        session_id,
        connection_id,
        session_user,
    );

    with_inbuilt_sql_runtime_context(&runtime_context, || {
        handle_query_command_internal(
            request_id,
            query,
            catalogs,
            wal,
            node_data_dir,
            runtime_indexes,
            Some(write_group_id),
            Some(touched_tables),
            session_id,
        )
    })

}

fn inbuilt_runtime_context_for_query(
    _request_id: &str,
    query: &DataQuery,
    _session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
) -> InbuiltSqlRuntimeContext {
    let user = session_user.unwrap_or_else(|| {
        std::env::var("USER")
            .ok()
            .map(|user| format!("{}@localhost", user))
            .unwrap_or_else(|| "root@localhost".to_string())
    });

    InbuiltSqlRuntimeContext {
        current_database: Some(query.database_id.clone()),
        current_user: Some(user.clone()),
        session_user: Some(user.clone()),
        system_user: Some(user),
        connection_id: Some(connection_id as i64),
        last_insert_id: None,
        version: None,
    }
}

fn handle_query_command_internal(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_tables: Option<&mut HashSet<String>>,
    session_id: &str,
) -> ConnectorResponse {

    let request_start = Instant::now();
    let parse_start = Instant::now();

    match serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) {

        Ok(parsed) => {
            let parse_ms = parse_start.elapsed().as_millis() as u64;

            if let Some(statement) = parsed.first() {
                log::debug!(
                    "query directive parsed request_id={} database_id={} directive={:?} operation={:?} object_name={:?}",
                    request_id,
                    query.database_id,
                    statement.directive,
                    statement.operation,
                    statement.object_name
                );
            }

            let response = execute_parsed_query(
                request_id,
                query,
                catalogs,
                wal,
                node_data_dir,
                runtime_indexes,
                parsed,
                external_write_group_id,
                touched_tables,
                session_id,
            );
            with_query_timings(response, make_query_timings(request_start, parse_ms))
        },

        Err(err) => {
            ConnectorResponse::rejected(request_id.to_string(), format!("sql parse failed: {err}"))
        }

    }

}

struct QueryExecutionContext<'a> {
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    wal: &'a ConcurrentWalManager,
    node_data_dir: &'a Path,
    runtime_indexes: &'a mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&'a mut HashSet<String>>,
    session_id: &'a str,
}

type QueryOperationHandler = fn(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
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
    external_write_group_id: Option<TransactionId>,
    touched_tables: Option<&mut HashSet<String>>,
    session_id: &str,
) -> ConnectorResponse {

    if parsed.len() != 1 {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "multi-statement query execution is not wired yet",
        );
    }

    let statement = &parsed[0];

    let mut ctx = QueryExecutionContext {
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        external_write_group_id,
        touched_write_tables: touched_tables,
        session_id,
    };

    log::debug!(
        "query directive dispatch request_id={} database_id={} directive={:?} operation={:?} object_name={:?}",
        request_id,
        query.database_id,
        statement.directive,
        statement.operation,
        statement.object_name
    );

    let handler: Option<QueryOperationHandler> = match statement.operation {
        SqlOperation::Insert => Some(execute_insert),
        SqlOperation::Update => Some(execute_update),
        SqlOperation::Delete => Some(execute_delete),
        SqlOperation::Select => Some(execute_select),
        SqlOperation::CreateDatabase => Some(execute_create_database),
        SqlOperation::CreateTable => Some(execute_create_table),
        SqlOperation::DropDatabase
        | SqlOperation::DropTable
        | SqlOperation::DropView
        | SqlOperation::DropTrigger
        | SqlOperation::DropStoredProcedure => Some(execute_drop_directive),
        SqlOperation::CreateView => Some(execute_create_view),
        SqlOperation::CreateTrigger => Some(execute_create_trigger),
        SqlOperation::CreateStoredProcedure => Some(execute_create_stored_procedure),
        SqlOperation::AlterTable => Some(execute_alter_table),
        SqlOperation::AlterOther => Some(execute_alter_other),
        _ => None,
    };

    match handler {

        Some(handler) => handler(&mut ctx, request_id, query, statement),
        
        None => {
            log::debug!(
                "query directive missing handler request_id={} database_id={} directive={:?} operation={:?} object_name={:?}",
                request_id,
                query.database_id,
                statement.directive,
                statement.operation,
                statement.object_name
            );

            ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "query operation '{:?}' execution is not wired yet",
                    statement.operation
                ),
            )
        },

    }

}

fn execute_alter_other(
    _ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    _query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let lowered = statement.sql.trim().to_ascii_lowercase();

    if lowered.starts_with("begin") || lowered.starts_with("start transaction") {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "transaction control recognized but session transactions are not wired yet; current mode is autocommit per statement",
        );
    }

    if lowered.starts_with("commit") {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "commit recognized but session transactions are not wired yet; current mode is autocommit per statement",
        );
    }

    if lowered.starts_with("rollback") {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "rollback recognized but session transactions are not wired yet; current mode is autocommit per statement",
        );
    }

    ConnectorResponse::rejected(
        request_id.to_string(),
        format!(
            "query operation '{:?}' execution is not wired yet",
            statement.operation
        ),
    )

}

fn execute_alter_table(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_alter_table_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_create_database(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_create_database_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_create_table(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_create_table_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_drop_directive(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_drop_directive_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_insert(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_insert_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        ctx.runtime_indexes,
        ctx.external_write_group_id,
        ctx.touched_write_tables.as_deref_mut(),
        statement,
    )

}

fn execute_update(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {
    
    execute_update_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        ctx.runtime_indexes,
        ctx.external_write_group_id,
        ctx.touched_write_tables.as_deref_mut(),
        statement,
    )

}

fn execute_delete(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_delete_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        ctx.runtime_indexes,
        ctx.external_write_group_id,
        ctx.touched_write_tables.as_deref_mut(),
        statement,
    )

}

fn execute_select(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_select_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        ctx.runtime_indexes,
        statement,
    )

}

fn execute_create_view(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_create_view_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_create_trigger(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_create_trigger_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_create_stored_procedure(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    execute_create_stored_procedure_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
        ctx.node_data_dir,
        statement,
    )

}

fn execute_alter_table_impl(
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
            );
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
            );
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

            AlterTableChangeOp::ModifyField {
                field_name,
                new_type,
            } => {
                type_changes.push(serverlib::FieldTypeChangeRule {
                    field_name: field_name.clone(),
                    target_type: new_type.clone(),
                });
                Ok(())
            },

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
    let created_at = common::epoch_nanos!();

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
            additions: additions
                .into_iter()
                .map(|(name, _)| (name, vec![]))
                .collect(),
            type_changes,
            conversion_policy: serverlib::TypeConversionPolicy::Safe,
        };

        let executor =
            serverlib::DiskToMemorySchemaMigrationExecutor::new(node_data_dir.to_path_buf());
        
        executor
            .set_rules_for_table(&table_id, mutation_rule_set)
            .ok();

        if let Err(err) = serverlib::run_schema_migration(catalog, &table_id, &executor) {
            log::warn!("schema migration failed for table '{}': {err}", table_id);
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

fn execute_create_database_impl(
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
        },

        Err(err) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create database failed: {err}"),
        ),

    }

}

fn execute_create_table_impl(
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
            );
        }
    };

    let normalized_table_id = common::normalize_identifier!(table_id);
    if catalog.table(&normalized_table_id).is_some() {
        if request_id.starts_with("replication-schema-apply-") {
            return ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            );
        }

        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "create table failed: table '{}' already exists",
                normalized_table_id
            ),
        );
    }

    let created_at = common::epoch_nanos!();
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
            );
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
        return ConnectorResponse::rejected(request_id.to_string(), err);
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
            );
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
        return ConnectorResponse::rejected(request_id.to_string(), err);
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
            );
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
        return ConnectorResponse::rejected(request_id.to_string(), err);
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )

}

fn execute_drop_directive_impl(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    statement: &SqlRequest,
) -> ConnectorResponse {

    match statement.operation {

        SqlOperation::DropDatabase => {
            execute_drop_database(request_id, catalogs, node_data_dir, statement)
        },

        _ if drop_entity_operation_metadata(statement.operation).is_some() => {
            execute_drop_entity_object(request_id, query, catalogs, wal, node_data_dir, statement)
        },

        _ => ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "drop directive is not supported for operation '{:?}'",
                statement.operation
            ),
        ),

    }

}

fn drop_entity_operation_metadata(
    operation: SqlOperation,
) -> Option<(DatabaseObjectType, &'static str, Option<SqlObjectKind>)> {

    match operation {
        
        SqlOperation::DropTable => Some((DatabaseObjectType::Table, "table", None)),
        
        SqlOperation::DropView => {
            Some((DatabaseObjectType::View, "view", Some(SqlObjectKind::View)))
        },
        
        SqlOperation::DropTrigger => Some((
            DatabaseObjectType::Trigger,
            "trigger",
            Some(SqlObjectKind::Trigger),
        )),

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
            format!(
                "drop database failed: database '{}' not found",
                database_name
            ),
        );
    };

    let catalog_file = node_data_dir.join(catalog.file_name());
    if let Err(err) = fs::remove_file(&catalog_file)
        && err.kind() != ErrorKind::NotFound {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("drop database failed: cannot remove catalog file: {err}"),
            );
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
            format!(
                "drop operation '{:?}' missing object identifier",
                statement.operation
            ),
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

    if let Some(stream_id) = entity_wal_stream_id
        && stream_id != wal_id
            && let Err(err) = wal.delete_stream(&stream_id) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("drop {kind_label} WAL delete failed: {err}"),
                );
    }

    let timestamp = common::epoch_nanos!();

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
                );
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
            object_kind: sql_object_kind
                .expect("sql object kind should be present for non-table drop"),
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
                );
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

fn execute_insert_impl(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&mut HashSet<String>>,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let (statement_sql, is_explain) = explain_inner_statement(&statement.sql);

    let plan = match serverlib::parse_insert_rows_from_statement(statement_sql) {
        Ok(plan) => plan,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("insert parse failed: {err}"),
            );
        }
    };

    if is_explain {

        let mut rows = vec![
            vec!["operation".to_string(), "insert".to_string()],
            vec!["table".to_string(), plan.table_id.clone()],
            vec!["column_count".to_string(), plan.columns.len().to_string()],
        ];

        match &plan.source {

            serverlib::InsertRowsSource::Values(values) => {
                rows.push(vec!["source".to_string(), "values".to_string()]);
                rows.push(vec!["row_count".to_string(), values.len().to_string()]);
            },

            serverlib::InsertRowsSource::Select(select_plan) => {
                rows.push(vec!["source".to_string(), "select".to_string()]);
                rows.push(vec![
                    "source_relations".to_string(),
                    select_plan.relations.len().to_string(),
                ]);
                rows.push(vec![
                    "source_joins".to_string(),
                    select_plan.joins.len().to_string(),
                ]);
            }

        }

        return explain_mutation_plan(request_id, rows);

    }

    with_table_write_guard(
        request_id,
        catalogs,
        &query.database_id,
        &plan.table_id,
        |catalog| {
        execute_insert_locked(
            request_id,
            query,
            catalog,
            wal,
            runtime_indexes,
            external_write_group_id,
            touched_write_tables,
            &plan,
        )
        },
    )

}

fn execute_insert_locked(
    request_id: &str,
    query: &DataQuery,
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&mut HashSet<String>>,
    plan: &serverlib::InsertRowsPlan,
) -> ConnectorResponse {

    let Some(schema) = catalog.table_schema(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, query.database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, query.database_id
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

    let insert_rows =
        match materialize_insert_source_rows(catalog, wal, runtime_indexes, &plan.source) {
            Ok(rows) => rows,
            Err(message) => return ConnectorResponse::rejected(request_id.to_string(), message),
        };

    with_statement_write_batch(
        request_id,
        wal,
        table,
        runtime_indexes,
        external_write_group_id,
        touched_write_tables,
        |group_id, runtime_indexes| {

            let mut affected_rows = 0u64;

            for row in &insert_rows {

                if row.len() != columns.len() {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!(
                            "insert failed: row has {} values but {} columns were specified",
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
                            format!(
                                "insert failed: missing required column '{}'",
                                field.field_name
                            ),
                        );
                    }

                }

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

                    if pk_runtime
                        .map(|idx| idx.contains(&incoming_pk))
                        .unwrap_or(false)
                    {
                        
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

                let encoded = match encode_row_payload(schema, &payload_row) {
                    
                    Ok(encoded) => encoded,
                    
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert payload encode failed: {err}"),
                        );
                    }

                };

                if let Err(err) = append_row_payload_record(
                    wal,
                    &plan.table_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Insert,
                    encoded,
                    common::epoch_nanos!(),
                    None,
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("insert WAL append failed: {err}"),
                    );
                }

                affected_rows = affected_rows.saturating_add(1);

            }

            // Capture last insert id if any rows were inserted
            if affected_rows > 0 {
                LAST_INSERT_ID_CONTEXT.with(|ctx| {
                    *ctx.borrow_mut() = 1; // Simplified: set to 1 for now (future: track actual auto-increment)
                });
            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows }),
            )
            
        },
    )

}

fn materialize_insert_source_rows(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    source: &serverlib::InsertRowsSource,
) -> Result<Vec<Vec<Option<Vec<u8>>>>, String> {

    match source {

        serverlib::InsertRowsSource::Values(rows) => Ok(rows.clone()),

        serverlib::InsertRowsSource::Select(read_plan) => {

            let select_result = if !read_plan.joins.is_empty() {

                serverlib::execute_joined_select_plan(
                    catalog,
                    wal,
                    runtime_indexes,
                    read_plan,
                    &mut |function| evaluate_inbuilt_sql_function(function),
                    &mut |row_map, condition| {
                        serverlib::row_matches_select_condition(
                            row_map,
                            condition,
                            catalog,
                            wal,
                            runtime_indexes,
                        )
                    },
                    &mut |row_tuple, condition| {
                        serverlib::row_matches_select_condition(
                            row_tuple,
                            condition,
                            catalog,
                            wal,
                            runtime_indexes,
                        )
                    },
                )

            } else if read_plan.table_id.is_empty() {
                
                serverlib::execute_projection_only_select_plan(read_plan, &mut |function| {
                    evaluate_inbuilt_sql_function(function)
                })

            } else {
                
                let table_id = read_plan.table_id.as_str();

                let schema = catalog.table_schema(table_id).ok_or_else(|| {
                    format!("insert select failed: table '{}' not found", table_id)
                })?;

                let table = catalog.table(table_id).ok_or_else(|| {
                    format!("insert select failed: table '{}' not found", table_id)
                })?;

                let mut index_filter_map = HashMap::new();

                let allow_index_short_circuit = read_plan
                    .where_condition
                    .as_ref()
                    .map(|condition| {
                        collect_indexable_equality_filters(condition, &mut index_filter_map)
                    })
                    .unwrap_or(true);

                let access_plan =
                    plan_relation_access(table, allow_index_short_circuit, index_filter_map);

                serverlib::execute_relation_select_plan(
                    wal,
                    table,
                    schema,
                    runtime_indexes,
                    read_plan,
                    &access_plan,
                    &mut |function| evaluate_inbuilt_sql_function(function),
                    &mut |row_map, condition| {
                        serverlib::row_matches_select_condition(
                            row_map,
                            condition,
                            catalog,
                            wal,
                            runtime_indexes,
                        )
                    },
                )

            }
            .map_err(|message| format!("insert select failed: {message}"))?;

            let rows =
                select_result
                    .rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .enumerate()
                            .map(|(index, value)| {
                                if select_result.columns.get(index).is_some_and(|column| {
                                    column.nullable && value == b"NULL".to_vec()
                                }) {
                                    None
                                } else {
                                    Some(value)
                                }
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();

            Ok(rows)

        }

    }

}

fn execute_update_impl(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&mut HashSet<String>>,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let (statement_sql, is_explain) = explain_inner_statement(&statement.sql);

    let plan = match serverlib::parse_update_rows_from_statement(statement_sql) {
        Ok(plan) => plan,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("update parse failed: {err}"),
            );
        }
    };

    if is_explain {
        return explain_join_mutation_plan(
            request_id,
            "update",
            &plan.table_id,
            &plan.relations,
            &plan.joins,
            &plan.pushdown_conditions,
            plan.assignments.len(),
            plan.where_condition.is_some(),
        );
    }

    with_table_write_guard(
        request_id,
        catalogs,
        &query.database_id,
        &plan.table_id,
        |catalog| {
        execute_update_locked(
            request_id,
            query,
            catalog,
            wal,
            runtime_indexes,
            external_write_group_id,
            touched_write_tables,
            &plan,
        )
        },
    )

}

fn execute_update_locked(
    request_id: &str,
    query: &DataQuery,
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&mut HashSet<String>>,
    plan: &serverlib::UpdateRowsPlan,
) -> ConnectorResponse {

    let Some(schema) = catalog.table_schema(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, query.database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, query.database_id
            ),
        );
    };

    for assignment in &plan.assignments {
        if schema.field(&assignment.field_name).is_none() {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("update failed: unknown column '{}'", assignment.field_name),
            );
        }
    }

    let mutation_uses_joins = !plan.joins.is_empty();

    let current_live_rows = match load_mutation_rows(
        catalog,
        wal,
        runtime_indexes,
        schema,
        &plan.table_id,
        &plan.relations,
        &plan.pushdown_conditions,
        &plan.joins,
        plan.where_condition.as_ref(),
    ) {
        Ok(rows) => rows,
        Err(message) => {
            return ConnectorResponse::rejected(request_id.to_string(), message);
        }
    };

    let mut pk_keys = if let Some(pk_index) = primary_key_index(table) {
        current_live_rows
            .iter()
            .map(|(_, row_map)| index_value_tuple(pk_index, row_map))
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    with_statement_write_batch(
        request_id,
        wal,
        table,
        runtime_indexes,
        external_write_group_id,
        touched_write_tables,
        |group_id, runtime_indexes| {

            let mut affected_rows = 0u64;

            for (row_id, row_map) in current_live_rows {

                if !mutation_uses_joins
                    && !serverlib::row_matches_select_condition(
                        &row_map,
                        plan.where_condition.as_ref(),
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                {
                    continue;
                }

                let mut updated_row = row_map.clone();

                for assignment in &plan.assignments {
                    match &assignment.value {
                        Some(value) => {
                            updated_row.insert(assignment.field_name.clone(), value.clone());
                        }
                        None => {
                            updated_row.remove(&assignment.field_name);
                        }
                    }
                }

                for field in &schema.fields {
                    if !updated_row.contains_key(&field.field_name) && !field.nullable {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!(
                                "update failed: column '{}' cannot be null",
                                field.field_name
                            ),
                        );
                    }
                }

                if let Some(pk_index) = primary_key_index(table) {

                    let old_pk = index_value_tuple(pk_index, &row_map);
                    let new_pk = index_value_tuple(pk_index, &updated_row);

                    if old_pk != new_pk && pk_keys.contains(&new_pk) {
                        let pk_fields =
                            if pk_index.field_names.is_empty() && !pk_index.field_name.is_empty() {
                                vec![pk_index.field_name.clone()]
                            } else {
                                pk_index.field_names.clone()
                            };

                        let pk_display = pk_fields
                            .iter()
                            .zip(new_pk.iter())
                            .map(|(name, val)| format!("{}={}", name, String::from_utf8_lossy(val)))
                            .collect::<Vec<_>>()
                            .join(", ");

                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("update failed: duplicate primary key ({})", pk_display),
                        );
                    }

                    pk_keys.remove(&old_pk);
                    pk_keys.insert(new_pk);

                }

                let delete_payload = match encode_row_payload(schema, &row_map) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("update delete payload encode failed: {err}"),
                        );
                    }
                };

                if let Err(err) = append_row_payload_record(
                    wal,
                    &plan.table_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Delete,
                    delete_payload,
                    common::epoch_nanos!(),
                    Some(TransactionId(row_id)),
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("update delete WAL append failed: {err}"),
                    );
                }

                let insert_payload = match encode_row_payload(schema, &updated_row) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("update insert payload encode failed: {err}"),
                        );
                    }
                };

                if let Err(err) = append_row_payload_record(
                    wal,
                    &plan.table_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Insert,
                    insert_payload,
                    common::epoch_nanos!(),
                    None,
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("update insert WAL append failed: {err}"),
                    );
                }

                affected_rows = affected_rows.saturating_add(1);

            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows }),
            )

        },
    )

}

fn execute_delete_impl(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&mut HashSet<String>>,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let (statement_sql, is_explain) = explain_inner_statement(&statement.sql);

    let plan = match serverlib::parse_delete_rows_from_statement(statement_sql) {
        Ok(plan) => plan,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("delete parse failed: {err}"),
            );
        }
    };

    if is_explain {
        return explain_join_mutation_plan(
            request_id,
            "delete",
            &plan.table_id,
            &plan.relations,
            &plan.joins,
            &plan.pushdown_conditions,
            0,
            plan.where_condition.is_some(),
        );
    }

    with_table_write_guard(
        request_id,
        catalogs,
        &query.database_id,
        &plan.table_id,
        |catalog| {
        execute_delete_locked(
            request_id,
            query,
            catalog,
            wal,
            runtime_indexes,
            external_write_group_id,
            touched_write_tables,
            &plan,
        )
        },
    )

}

fn execute_delete_locked(
    request_id: &str,
    query: &DataQuery,
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_write_tables: Option<&mut HashSet<String>>,
    plan: &serverlib::DeleteRowsPlan,
) -> ConnectorResponse {

    let Some(schema) = catalog.table_schema(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, query.database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, query.database_id
            ),
        );
    };

    let mutation_uses_joins = !plan.joins.is_empty();

    let current_live_rows = match load_mutation_rows(
        catalog,
        wal,
        runtime_indexes,
        schema,
        &plan.table_id,
        &plan.relations,
        &plan.pushdown_conditions,
        &plan.joins,
        plan.where_condition.as_ref(),
    ) {
        Ok(rows) => rows,
        Err(message) => {
            return ConnectorResponse::rejected(request_id.to_string(), message);
        }
    };

    with_statement_write_batch(
        request_id,
        wal,
        table,
        runtime_indexes,
        external_write_group_id,
        touched_write_tables,
        |group_id, runtime_indexes| {

            let mut affected_rows = 0u64;

            for (row_id, row_map) in current_live_rows {

                if !mutation_uses_joins
                    && !serverlib::row_matches_select_condition(
                        &row_map,
                        plan.where_condition.as_ref(),
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                {
                    continue;
                }

                let delete_payload = match encode_row_payload(schema, &row_map) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("delete payload encode failed: {err}"),
                        );
                    }
                };

                if let Err(err) = append_row_payload_record(
                    wal,
                    &plan.table_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Delete,
                    delete_payload,
                    common::epoch_nanos!(),
                    Some(TransactionId(row_id)),
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("delete WAL append failed: {err}"),
                    );
                }

                affected_rows = affected_rows.saturating_add(1);
            
            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows }),
            )

        },
    )

}

fn with_table_write_guard<F>(
    request_id: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    database_id: &str,
    table_id: &str,
    execute: F,
) -> ConnectorResponse
where
    F: FnOnce(&mut DatabaseCatalog) -> ConnectorResponse,
{
    let Some(catalog) = resolve_catalog_mut(catalogs, database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", database_id),
        );
    };

    if let Err(err) = catalog.begin_table_write(table_id) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("table write lock failed: {err}"),
        );
    }

    let response = execute(catalog);

    if matches!(response.status, connector::ResponseStatus::Applied) {

        match catalog.finalize_table_write(table_id) {

            Ok(()) => response,

            Err(err) => {
                let _ = catalog.abort_table_write(table_id);
                ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("table write finalize failed: {err}"),
                )
            }
        
        }

    } else {
        let _ = catalog.abort_table_write(table_id);
        response
    }

}

#[expect(clippy::type_complexity, reason="complexity is inherent to the operation being performed and attempting to simplify it would reduce readability")]
fn load_mutation_rows(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    schema: &TableSchema,
    table_id: &str,
    relations: &[serverlib::SelectRelation],
    pushdown_conditions: &[Option<SelectCondition>],
    joins: &[serverlib::SelectJoin],
    where_condition: Option<&SelectCondition>,
) -> Result<Vec<(u64, HashMap<String, Vec<u8>>)>, String> {

    if joins.is_empty() {
        return Ok(load_live_rows(wal, table_id, schema));
    }

    serverlib::select_mutation_target_rows(
        catalog,
        wal,
        runtime_indexes,
        relations,
        pushdown_conditions,
        joins,
        where_condition,
        &mut |row_map, condition| {
            serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
    )
    .map(|rows| {
        rows.into_iter()
            .map(|row| (row.row_id, row.row_map))
            .collect()
    })

}

fn execute_select_impl(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &RuntimeIndexStore,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let statement_sql_lower = statement.sql.to_ascii_lowercase();

    if statement_sql_lower.starts_with("show databases") {
        
        let result = serverlib::show_databases_result(
            catalogs.values().map(|catalog| {
                if catalog.database_name().is_empty() {
                    catalog.database_id.0.clone()
                } else {
                    catalog.database_name().to_string()
                }
            }),
        );

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: connector_field_defs(result.columns),
                rows: result.rows,
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

        let result = serverlib::show_tables_result(catalog.table_ids());

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: connector_field_defs(result.columns),
                rows: result.rows,
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
                format!(
                    "table '{}' not found in database '{}'",
                    table_id, query.database_id
                ),
            );
        };

        let result = serverlib::describe_table_result(schema);

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: connector_field_defs(result.columns),
                rows: result.rows,
                timings: empty_query_timings(),
            }),
        );

    }

    let read_plan = match serverlib::parse_select_read_plan_from_statement(&statement.sql) {

        Ok(plan) => plan,
        
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("select parse failed: {err}"),
            );
        }

    };

    if !read_plan.joins.is_empty() {
        return execute_joined_select(request_id, query, catalog, wal, runtime_indexes, &read_plan);
    }

    let table_id = read_plan.table_id.as_str();

    if table_id.is_empty() {

        if read_plan.is_explain {
            return explain_select_plan(
                request_id,
                serverlib::explain_select_plan_result(
                    "<no-from>",
                    read_plan
                        .where_condition
                        .as_ref()
                        .map(count_condition_predicates)
                        .unwrap_or(0),
                    None,
                    runtime_indexes,
                ),
            );
        }

        let result =
            match serverlib::execute_projection_only_select_plan(&read_plan, &mut |function| {
                evaluate_inbuilt_sql_function(function)
            }) {
                Ok(result) => result,
                Err(message) => {
                    return ConnectorResponse::rejected(request_id.to_string(), message);
                }
            };

        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Query(QueryResult {
                columns: connector_field_defs(result.columns),
                rows: result.rows,
                timings: empty_query_timings(),
            }),
        );

    }

    let Some(schema) = catalog.table_schema(table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                table_id, query.database_id
            ),
        );
    };

    let Some(table) = catalog.table(table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                table_id, query.database_id
            ),
        );
    };

    let mut index_filter_map = HashMap::new();
    let allow_index_short_circuit = read_plan
        .where_condition
        .as_ref()
        .map(|condition| collect_indexable_equality_filters(condition, &mut index_filter_map))
        .unwrap_or(true);

    let access_plan = plan_relation_access(table, allow_index_short_circuit, index_filter_map);
    let index_lookup = access_plan.runtime_index_lookup(table);

    if read_plan.is_explain {
        return explain_select_plan(
            request_id,
            serverlib::explain_select_plan_result(
                table_id,
                read_plan
                    .where_condition
                    .as_ref()
                    .map(count_condition_predicates)
                    .unwrap_or(0),
                index_lookup,
                runtime_indexes,
            ),
        );
    }

    let result = match serverlib::execute_relation_select_plan(
        wal,
        table,
        schema,
        runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |function| evaluate_inbuilt_sql_function(function),
        &mut |row_map, condition| {
            serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
    ) {
        Ok(result) => result,
        Err(message) => return ConnectorResponse::rejected(request_id.to_string(), message),
    };

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns: connector_field_defs(result.columns),
            rows: result.rows,
            timings: empty_query_timings(),
        }),
    )

}

fn execute_joined_select(
    request_id: &str,
    _query: &DataQuery,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
) -> ConnectorResponse {

    if read_plan.is_explain {
        return explain_select_plan(
            request_id,
            serverlib::explain_joined_select_plan_result(read_plan),
        );
    }

    let result = match serverlib::execute_joined_select_plan(
        catalog,
        wal,
        runtime_indexes,
        read_plan,
        &mut |function| evaluate_inbuilt_sql_function(function),
        &mut |row_map, condition| {
            serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
        &mut |row_tuple, condition| {
            serverlib::row_matches_select_condition(
                row_tuple,
                condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
    ) {
        Ok(result) => result,
        Err(message) => return ConnectorResponse::rejected(request_id.to_string(), message),
    };

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns: connector_field_defs(result.columns),
            rows: result.rows,
            timings: empty_query_timings(),
        }),
    )

}

fn execute_create_view_impl(
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
    let created_at = common::epoch_nanos!();

    match catalog.register_view(view_id, statement.sql.clone(), TableSchema::new(Vec::new())) {

        Ok(()) => {

            let Some(entity_wal_id) = catalog.entity_wal_stream_id(view_id) else {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    "create view WAL stream lookup failed".to_string(),
                );
            };

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

fn execute_create_trigger_impl(
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
    let created_at = common::epoch_nanos!();

    let created = catalog.register_trigger(trigger_id, statement.sql.clone(), Vec::new());

    if let Err(err) = created {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger failed: {err}"),
        );
    }

    let Some(entity_wal_id) = catalog.entity_wal_stream_id(trigger_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "create trigger WAL stream lookup failed".to_string(),
        );
    };

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

fn execute_create_stored_procedure_impl(
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
    let created_at = common::epoch_nanos!();

    let created =
        catalog.register_stored_procedure(procedure_id, statement.sql.clone(), Vec::new());

    if let Err(err) = created {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure failed: {err}"),
        );
    }

    let Some(entity_wal_id) = catalog.entity_wal_stream_id(procedure_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "create procedure WAL stream lookup failed".to_string(),
        );
    };

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

fn append_payload_record(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
) -> Result<(), String> {
    append_payload_record_with_group(wal, wal_id, kind, payload, timestamp_epoch_ms, None)
        .map(|_| ())
}

fn append_payload_record_with_group(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    group_id: Option<TransactionId>,
) -> Result<TransactionId, String> {

    let existing = wal.since(wal_id, None);
    let last = existing.last();
    let next_id = TransactionId(last.map(|record| record.id.0 + 1).unwrap_or(1));
    let refid = last.map(|record| record.id);
    
    let record_group_id = group_id.or({
        if matches!(kind, TransactionKind::WriteBegin) {
            Some(next_id)
        } else {
            None
        }
    });

    wal.append(
        wal_id,
        TransactionRecord {
            id: next_id,
            groupid: record_group_id,
            refid,
            timestamp_epoch_ms,
            actor: UserId::from_username("server"),
            kind,
            payload,
        },
    )
    .map_err(|e| e.to_string())?;

    Ok(next_id)

}

pub(super) fn append_row_payload_record(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    group_id: Option<TransactionId>,
) -> Result<(), String> {
    let existing = wal.since(wal_id, None);

    if let Some(expected_refid) = refid {
        let live_row_ids = load_live_rows(wal, wal_id, table.schema())
            .into_iter()
            .map(|(row_id, _)| row_id)
            .collect::<HashSet<_>>();

        if !live_row_ids.contains(&expected_refid.0) {
            return Err(format!(
                "row mutation references stale or missing live transaction id {}",
                expected_refid.0
            ));
        }
    }

    let last = existing.last();
    let next_id = TransactionId(last.map(|record| record.id.0 + 1).unwrap_or(1));
    let refid = refid.or_else(|| last.map(|record| record.id));

    let row_map = decode_row_payload(table.schema(), &payload)
        .map_err(|err| format!("row payload decode failed: {err}"))?;

    let record = TransactionRecord {
        id: next_id,
        groupid: group_id,
        refid,
        timestamp_epoch_ms,
        actor: UserId::from_username("server"),
        kind: kind.clone(),
        payload,
    };

    wal.append(wal_id, record).map_err(|e| e.to_string())?;

    runtime_indexes.apply_table_row_mutation(derived_indexes_for_table(table), kind, &row_map);

    Ok(())

}

fn with_statement_write_batch<F>(
    request_id: &str,
    wal: &ConcurrentWalManager,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    external_write_group_id: Option<TransactionId>,
    touched_tables: Option<&mut HashSet<String>>,
    execute: F,
) -> ConnectorResponse
where
    F: FnOnce(TransactionId, &mut RuntimeIndexStore) -> ConnectorResponse,
{

    if let (Some(write_group_id), Some(touched_tables)) = (external_write_group_id, touched_tables)
    {
        if touched_tables.insert(table.table_id.clone())
            && let Err(err) = append_payload_record_with_group(
                wal,
                &table.table_id,
                TransactionKind::WriteBegin,
                request_id.as_bytes().to_vec(),
                common::epoch_nanos!(),
                Some(write_group_id),
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("transaction write begin failed: {err}"),
                );
            }

        return execute(write_group_id, runtime_indexes);
    }

    let write_group_id = match append_payload_record_with_group(
        wal,
        &table.table_id,
        TransactionKind::WriteBegin,
        request_id.as_bytes().to_vec(),
        common::epoch_nanos!(),
        None,
    ) {
        Ok(group_id) => group_id,
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("statement write begin failed: {err}"),
            )
        }
    };

    let response = execute(write_group_id, runtime_indexes);

    if matches!(response.status, connector::ResponseStatus::Applied) {

        if let Err(err) = append_payload_record_with_group(
            wal,
            &table.table_id,
            TransactionKind::WriteCommit,
            Vec::new(),
            common::epoch_nanos!(),
            Some(write_group_id),
        ) {
            let _ = append_payload_record_with_group(
                wal,
                &table.table_id,
                TransactionKind::WriteAbort,
                Vec::new(),
                common::epoch_nanos!(),
                Some(write_group_id),
            );
            rebuild_runtime_indexes_for_table(table, wal, runtime_indexes);
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("statement write commit failed: {err}"),
            );
        }

        response

    } else {
        
        let _ = append_payload_record_with_group(
            wal,
            &table.table_id,
            TransactionKind::WriteAbort,
            Vec::new(),
            common::epoch_nanos!(),
            Some(write_group_id),
        );

        rebuild_runtime_indexes_for_table(table, wal, runtime_indexes);
        response
    
    }

}

pub(crate) fn commit_external_write_group(
    wal: &ConcurrentWalManager,
    table_ids: &HashSet<String>,
    group_id: TransactionId,
) -> Result<(), String> {
    for table_id in table_ids {
        append_payload_record_with_group(
            wal,
            table_id,
            TransactionKind::WriteCommit,
            Vec::new(),
            common::epoch_nanos!(),
            Some(group_id),
        )?;
    }

    Ok(())
}

pub(crate) fn abort_external_write_group(
    wal: &ConcurrentWalManager,
    catalogs: &HashMap<String, DatabaseCatalog>,
    runtime_indexes: &mut RuntimeIndexStore,
    table_ids: &HashSet<String>,
    group_id: TransactionId,
) {
    for table_id in table_ids {

        let _ = append_payload_record_with_group(
            wal,
            table_id,
            TransactionKind::WriteAbort,
            Vec::new(),
            common::epoch_nanos!(),
            Some(group_id),
        );

        if let Some(table) = catalogs.values().find_map(|catalog| catalog.table(table_id)) {
            rebuild_runtime_indexes_for_table(table, wal, runtime_indexes);
        }

    }
}

fn rebuild_runtime_indexes_for_table(
    table: &serverlib::DatabaseTable,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
) {
    let live_rows = load_live_rows(wal, &table.table_id, table.schema());

    for index in derived_indexes_for_table(table) {

        runtime_indexes.index_mut(&index.index_id.0).rebuild(
            live_rows
                .iter()
                .map(|(_, row_map)| index_value_tuple(index, row_map))
                .collect(),
        );

    }
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

    append_payload_record(wal, entity_wal_id, kind, payload, timestamp_epoch_ms)
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
        
        if let Err(err) = fs::remove_file(&path)
            && err.kind() != ErrorKind::NotFound {
                return Err(format!("cannot remove '{}': {err}", path.display()));
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

    write_bytes(
        entity_snapshot_path(node_data_dir, &normalized_entity_id),
        &file,
    )
    .map_err(|err| err.to_string())

}

fn remove_entity_snapshot_file(node_data_dir: &Path, entity_id: &str) -> Result<(), String> {
    
    let path = entity_snapshot_path(node_data_dir, entity_id);

    if let Err(err) = fs::remove_file(&path)
        && err.kind() != ErrorKind::NotFound {
            return Err(format!("cannot remove '{}': {err}", path.display()));
        }

    Ok(())
    
}

fn entity_snapshot_path(node_data_dir: &Path, entity_id: &str) -> PathBuf {
    let normalized_entity_id = common::normalize_identifier!(entity_id);
    let entity_stem = common::helpers::stable_id(&[&normalized_entity_id]);
    node_data_dir.join(FileKind::Entity.file_name(entity_stem))
}
