use std::collections::HashMap;
use std::time::Instant;

use connector::{
    ConnectorResponse, ConnectorResult, DataQuery, FieldDef, FieldIndex, FieldType, QueryResult,
    QueryTimings, MutationResult,
};
use serverlib::engine::database::transaction::TransactionLog;
use serverlib::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseId, EntityMetadata, EntityMetadataPayload,
    SqlDefinitionAction, SqlDefinitionPayload, SqlObjectKind, SqlOperation, SqlRequest,
    TableSchema, TransactionId, TransactionKind, TransactionRecord, UserId,
};

pub(crate) fn handle_query_command(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
) -> ConnectorResponse {
    let request_start = Instant::now();
    let parse_start = Instant::now();
    match serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) {
        Ok(parsed) => {
            let parse_ms = parse_start.elapsed().as_millis() as u64;
            let response = execute_parsed_query(request_id, query, catalogs, wal, parsed);
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
        SqlOperation::Select => execute_select(request_id, query, catalogs, statement),
        SqlOperation::CreateView => execute_create_view(request_id, query, catalogs, wal, statement),
        SqlOperation::DropView => execute_drop_view(request_id, query, catalogs, wal, statement),
        SqlOperation::CreateTrigger => {
            execute_create_trigger(request_id, query, catalogs, wal, statement)
        }
        SqlOperation::DropTrigger => execute_drop_trigger(request_id, query, catalogs, wal, statement),
        SqlOperation::CreateStoredProcedure => {
            execute_create_stored_procedure(request_id, query, catalogs, wal, statement)
        }
        SqlOperation::DropStoredProcedure => {
            execute_drop_stored_procedure(request_id, query, catalogs, wal, statement)
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

fn execute_select(
    request_id: &str,
    query: &DataQuery,
    catalogs: &HashMap<String, DatabaseCatalog>,
    statement: &SqlRequest,
) -> ConnectorResponse {

    if statement.sql.to_ascii_lowercase().starts_with("show tables") {

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
                encoded_metadata,
                created_at,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view metadata WAL append failed: {err}"),
                );
            }

            let sql_payload = SqlDefinitionPayload {
                object_id: view_id.to_string(),
                object_kind: SqlObjectKind::View,
                action: SqlDefinitionAction::Upsert,
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
                encoded,
                created_at,
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("create view sql definition WAL append failed: {err}"),
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

fn execute_drop_view(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(view_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "drop view missing view identifier",
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let wal_id = catalog.database_id.0.clone();

    match catalog.drop_view(view_id) {
        Ok(()) => {
            let timestamp = common::epochabs!() as u64;
            let sql_payload = SqlDefinitionPayload {
                object_id: view_id.to_string(),
                object_kind: SqlObjectKind::View,
                action: SqlDefinitionAction::Drop,
                sql: String::new(),
                dependencies: Vec::new(),
            };

            let encoded = match sql_payload.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("drop view sql payload encode failed: {err}"),
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
                    format!("drop view sql definition WAL append failed: {err}"),
                );
            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
            )
        }
        Err(err) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop view failed: {err}"),
        ),
    }
}

fn execute_create_trigger(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
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
        encoded_metadata,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger metadata WAL append failed: {err}"),
        );
    }

    let sql_payload = SqlDefinitionPayload {
        object_id: trigger_id.to_string(),
        object_kind: SqlObjectKind::Trigger,
        action: SqlDefinitionAction::Upsert,
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
        encoded_sql,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create trigger sql definition WAL append failed: {err}"),
        );
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )
}

fn execute_drop_trigger(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(trigger_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "drop trigger missing trigger identifier",
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let wal_id = catalog.database_id.0.clone();

    match catalog.drop_trigger(trigger_id) {
        Ok(()) => {
            let timestamp = common::epochabs!() as u64;
            let sql_payload = SqlDefinitionPayload {
                object_id: trigger_id.to_string(),
                object_kind: SqlObjectKind::Trigger,
                action: SqlDefinitionAction::Drop,
                sql: String::new(),
                dependencies: Vec::new(),
            };

            let encoded = match sql_payload.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("drop trigger sql payload encode failed: {err}"),
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
                    format!("drop trigger sql definition WAL append failed: {err}"),
                );
            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
            )
        }
        Err(err) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop trigger failed: {err}"),
        ),
    }
}

fn execute_create_stored_procedure(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
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
        encoded_metadata,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure metadata WAL append failed: {err}"),
        );
    }

    let sql_payload = SqlDefinitionPayload {
        object_id: procedure_id.to_string(),
        object_kind: SqlObjectKind::StoredProcedure,
        action: SqlDefinitionAction::Upsert,
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
        encoded_sql,
        created_at,
    ) {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("create procedure sql definition WAL append failed: {err}"),
        );
    }

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
    )
}

fn execute_drop_stored_procedure(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    statement: &SqlRequest,
) -> ConnectorResponse {
    let Some(procedure_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "drop procedure missing identifier",
        );
    };

    let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let wal_id = catalog.database_id.0.clone();

    match catalog.drop_stored_procedure(procedure_id) {
        Ok(()) => {
            let timestamp = common::epochabs!() as u64;
            let sql_payload = SqlDefinitionPayload {
                object_id: procedure_id.to_string(),
                object_kind: SqlObjectKind::StoredProcedure,
                action: SqlDefinitionAction::Drop,
                sql: String::new(),
                dependencies: Vec::new(),
            };

            let encoded = match sql_payload.encode() {
                Ok(encoded) => encoded,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("drop procedure sql payload encode failed: {err}"),
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
                    format!("drop procedure sql definition WAL append failed: {err}"),
                );
            }

            ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
            )
        }
        Err(err) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("drop procedure failed: {err}"),
        ),
    }
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
