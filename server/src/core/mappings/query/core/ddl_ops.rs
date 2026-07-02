use super::*;
use super::wal_ops::append_payload_record;

pub(super) fn execute_alter_table_impl(
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

pub(super) fn execute_create_database_impl(
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

    let encryption_key_ref = parse_create_database_encryption_key_ref(&statement.sql, database_name);

    match DatabaseCatalog::create_new_database(database_name, node_data_dir) {

        Ok(mut catalog) => {
            if let Some(key_ref) = encryption_key_ref {
                if let Err(err) = catalog.configure_at_rest_encryption_key_ref(key_ref) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("create database encryption configuration failed: {err}"),
                    );
                }

                if let Err(err) = catalog.save_in_directory(node_data_dir) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("create database persistence failed: {err}"),
                    );
                }
            }

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

fn parse_create_database_encryption_key_ref(sql: &str, database_name: &str) -> Option<String> {
    let tokens = sql
        .split_whitespace()
        .map(|token| token.trim_matches(';'))
        .collect::<Vec<_>>();

    for (idx, token) in tokens.iter().enumerate() {
        let lower = token.to_ascii_lowercase();

        if lower == "--aes" {
            if let Some(next) = tokens.get(idx + 1) {
                let candidate = next.trim_matches(';');
                if !candidate.is_empty() && !candidate.starts_with("--") {
                    return Some(candidate.to_string());
                }
            }

            return Some(default_create_database_encryption_key_ref(database_name));
        }

        if let Some(value) = lower.strip_prefix("--aes=") {
            if value.is_empty() {
                return Some(default_create_database_encryption_key_ref(database_name));
            }

            return Some(value.to_string());
        }
    }

    None
}

fn default_create_database_encryption_key_ref(database_name: &str) -> String {
    let normalized_name = common::normalize_identifier!(database_name);
    format!("enc:{}:{}", normalized_name, common::helpers::utils::unique_id())
}

pub(super) fn execute_create_table_impl(
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

pub(super) fn execute_drop_directive_impl(
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

pub(crate) fn execute_create_view_impl(
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

    let dependencies = match serverlib::parse_create_view_dependencies_from_sql(&statement.sql) {
        
        Ok(dependencies) => dependencies,
        
        Err(err) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("create view dependency parse failed: {err}"),
            );
        }

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

            if let Err(err) = catalog.set_sql_definition(
                view_id,
                SqlObjectKind::View,
                statement.sql.clone(),
                dependencies.clone(),
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view definition apply failed: {err}"),
                );
            }

            if let Err(err) = append_sql_definition_upsert_with_wal(
                catalog,
                wal,
                &wal_id,
                &entity_wal_id,
                view_id,
                SqlObjectKind::View,
                &statement.sql,
                dependencies,
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

pub(super) fn execute_create_trigger_impl(
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
        Vec::new(),
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

pub(super) fn execute_create_stored_procedure_impl(
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
        Vec::new(),
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
        kind,
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
    dependencies: Vec<String>,
    created_at: u64,
    operation_label: &str,
) -> Result<(), String> {

    let payload = SqlDefinitionPayload {
        object_id: object_id.to_string(),
        object_kind,
        action: SqlDefinitionAction::Upsert,
        schema_epoch: catalog.schema_epoch(),
        sql: sql.to_string(),
        dependencies,
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
