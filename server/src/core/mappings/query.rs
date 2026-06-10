use std::collections::HashMap;
use std::time::Instant;

use connector::{
    ConnectorResponse, ConnectorResult, DataQuery, FieldDef, FieldType, QueryResult,
    QueryTimings,
};
use serverlib::{DatabaseCatalog, DatabaseId, SqlRequest};

use super::dispatch_query_operation;

pub(crate) fn handle_query_command(
    request_id: &str,
    query: &DataQuery,
    catalogs: &HashMap<String, DatabaseCatalog>,
) -> ConnectorResponse {
    let request_start = Instant::now();
    let parse_start = Instant::now();
    match serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) {
        Ok(parsed) => {
            let parse_ms = parse_start.elapsed().as_millis() as u64;
            let response = execute_parsed_query(request_id, query, catalogs, parsed);
            with_query_timings(response, make_query_timings(request_start, parse_ms))
        }
        Err(err) => ConnectorResponse::rejected(request_id.to_string(), format!("sql parse failed: {err}")),
    }
}

fn execute_parsed_query(
    request_id: &str,
    query: &DataQuery,
    catalogs: &HashMap<String, DatabaseCatalog>,
    parsed: Vec<SqlRequest>,
) -> ConnectorResponse {

    if parsed.len() != 1 {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "multi-statement query execution is not wired yet",
        );
    }

    let statement = &parsed[0];
    
    dispatch_query_operation!(
        statement.operation,
        execute_select(request_id, query, catalogs, statement),
        ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "query operation '{:?}' execution is not wired yet",
                statement.operation
            ),
        )
    )
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
                    indexed: false,
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
