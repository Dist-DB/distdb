
use std::borrow::Cow;
use std::cmp::Ordering;
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
    static LAST_INSERT_ID_CONTEXT: RefCell<i64> = const { RefCell::new(0) };
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
        argument_bindings: HashMap::new(),
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
        
        SqlOperation::UnionQuery => Some(execute_union_query),
        
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

fn execute_union_query(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let (steps, order_by, limit, offset) =
        match serverlib::parse_union_select_read_plans_from_statement(&statement.sql) {
            Ok(parsed) => parsed,
            Err(err) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("union query execution failed: {err}"),
                )
            }
        };

    let Some(catalog) = resolve_catalog_mut(ctx.catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let mut set_stack = Vec::<serverlib::SelectExecutionResult>::new();

    for step in steps {
        match step {
            serverlib::SelectSetQueryStep::Branch(plan) => {
                let result = if !plan.ctes.is_empty() {
                    match execute_select_with_ctes(catalog, ctx.wal, ctx.runtime_indexes, &plan) {
                        Ok(result) => result,
                        Err(message) => {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("set query execution failed: {message}"),
                            )
                        }
                    }
                } else {
                    match execute_select_plan_result(catalog, ctx.wal, ctx.runtime_indexes, &plan) {
                        Ok(result) => result,
                        Err(message) => {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("set query execution failed: {message}"),
                            )
                        }
                    }
                };

                set_stack.push(result);
            }

            serverlib::SelectSetQueryStep::BoundaryOperation(boundary_operation) => {
                let Some(right_result) = set_stack.pop() else {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        "set query execution failed: missing right branch for set operation"
                            .to_string(),
                    );
                };

                let Some(mut left_result) = set_stack.pop() else {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        "set query execution failed: missing left branch for set operation"
                            .to_string(),
                    );
                };

                if left_result.columns.len() != right_result.columns.len() {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        "set query execution failed: all set-operation branches must return the same number of columns"
                            .to_string(),
                    );
                }

                if let Err(message) = reconcile_union_column_types(&mut left_result.columns, &right_result.columns) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("set query execution failed: {message}"),
                    );
                }

                let rows = apply_set_boundary_operation(
                    left_result.rows,
                    right_result.rows,
                    boundary_operation,
                    &left_result.columns,
                );

                left_result.rows = rows;
                set_stack.push(left_result);
            }
        }
    }

    let Some(set_result) = set_stack.pop() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "set query execution failed: no branch results were produced".to_string(),
        );
    };

    if !set_stack.is_empty() {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "set query execution failed: invalid set-operation evaluation state".to_string(),
        );
    }

    let columns = set_result.columns;
    let mut rows = set_result.rows;

    if !order_by.is_empty() {
        let mut order_indexes = Vec::with_capacity(order_by.len());
        const UNION_ORDER_BY_ORDINAL_PREFIX: &str = "__union_order_by_ordinal__";

        for item in &order_by {
            let index = if let Some(raw_ordinal) = item
                .field_name
                .strip_prefix(UNION_ORDER_BY_ORDINAL_PREFIX)
            {
                let ordinal = match raw_ordinal.parse::<usize>() {
                    Ok(value) => value,
                    Err(_) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!(
                                "union query execution failed: invalid ORDER BY ordinal '{}'",
                                raw_ordinal
                            ),
                        )
                    }
                };

                if ordinal == 0 || ordinal > columns.len() {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!(
                            "union query execution failed: ORDER BY ordinal {} is out of range for {} output columns",
                            ordinal,
                            columns.len()
                        ),
                    );
                }

                ordinal - 1

            } else {

                let Some(index) = columns
                    .iter()
                    .position(|column| column.field_name == item.field_name)
                else {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!(
                            "union query execution failed: ORDER BY column '{}' is not present in UNION output",
                            item.field_name
                        ),
                    );
                };

                index
            };

            order_indexes.push((index, item.descending));

        }

        rows.sort_by(|left, right| {

            for (index, descending) in &order_indexes {

                let ordering = compare_union_cell_values(
                    left.get(*index),
                    right.get(*index),
                    columns.get(*index),
                );

                if ordering != Ordering::Equal {
                    return if *descending {
                        ordering.reverse()
                    } else {
                        ordering
                    };
                }

            }

            Ordering::Equal
        });

    }

    rows = apply_union_row_window(rows, limit, offset);

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns: connector_field_defs(columns),
            rows,
            timings: empty_query_timings(),
        }),
    )
    
}

fn dedupe_union_rows_in_place(rows: &mut Vec<Vec<Vec<u8>>>) {
    let mut seen = HashSet::<Vec<Vec<u8>>>::new();
    rows.retain(|row| seen.insert(row.clone()));
}

fn dedupe_union_rows_with_columns(
    rows: &mut Vec<Vec<Vec<u8>>>,
    columns: &[serverlib::FieldDef],
) {
    let mut seen = HashSet::<Vec<Vec<u8>>>::new();
    rows.retain(|row| seen.insert(union_row_comparison_key(row, columns)));
}

fn apply_set_boundary_operation(
    mut left_rows: Vec<Vec<Vec<u8>>>,
    right_rows: Vec<Vec<Vec<u8>>>,
    operation: serverlib::SelectSetBoundaryOp,
    comparison_columns: &[serverlib::FieldDef],
) -> Vec<Vec<Vec<u8>>> {

    match operation {

        serverlib::SelectSetBoundaryOp::UnionAll => {
            left_rows.extend(right_rows);
            left_rows
        },

        serverlib::SelectSetBoundaryOp::UnionDistinct => {
            left_rows.extend(right_rows);
            dedupe_union_rows_with_columns(&mut left_rows, comparison_columns);
            left_rows
        },

        serverlib::SelectSetBoundaryOp::ExceptDistinct => {
            dedupe_union_rows_with_columns(&mut left_rows, comparison_columns);

            let mut right_seen = HashSet::<Vec<Vec<u8>>>::new();
            for row in right_rows {
                right_seen.insert(union_row_comparison_key(&row, comparison_columns));
            }

            left_rows.retain(|row| {
                let key = union_row_comparison_key(row, comparison_columns);
                !right_seen.contains(&key)
            });

            left_rows
        },

        serverlib::SelectSetBoundaryOp::IntersectDistinct => {
            dedupe_union_rows_with_columns(&mut left_rows, comparison_columns);

            let mut right_seen = HashSet::<Vec<Vec<u8>>>::new();
            for row in right_rows {
                right_seen.insert(union_row_comparison_key(&row, comparison_columns));
            }

            left_rows.retain(|row| {
                let key = union_row_comparison_key(row, comparison_columns);
                right_seen.contains(&key)
            });

            left_rows
        },

    }

}

fn reconcile_union_column_types(
    base_columns: &mut [serverlib::FieldDef],
    branch_columns: &[serverlib::FieldDef],
) -> Result<(), String> {

    for (index, (base_column, branch_column)) in base_columns
        .iter_mut()
        .zip(branch_columns.iter())
        .enumerate()
    {

        let resolved = resolve_union_column_type(&base_column.field_type, &branch_column.field_type)
            .ok_or_else(|| {
                format!(
                    "UNION column {} type mismatch: '{}' is not compatible with '{}'",
                    index + 1,
                    base_column.field_type.sql_variant_display_name(),
                    branch_column.field_type.sql_variant_display_name(),
                )
            })?;

        base_column.field_type = resolved;
        base_column.nullable = base_column.nullable || branch_column.nullable;

        reconcile_union_column_metadata(base_column, branch_column, index + 1)?;

    }

    Ok(())

}

fn resolve_union_column_type(
    left: &serverlib::FieldType,
    right: &serverlib::FieldType,
) -> Option<serverlib::FieldType> {
    use serverlib::FieldType;

    if left == right {
        return Some(left.clone());
    }

    match (left, right) {

        (FieldType::Float(left_bits), FieldType::Float(right_bits)) => {
            Some(FieldType::Float((*left_bits).max(*right_bits)))
        },

        (FieldType::Int(left_bits), FieldType::Int(right_bits)) => {
            Some(FieldType::Int((*left_bits).max(*right_bits)))
        },

        (FieldType::UInt(left_bits), FieldType::UInt(right_bits)) => {
            Some(FieldType::UInt((*left_bits).max(*right_bits)))
        },

        (FieldType::Int(left_bits), FieldType::UInt(right_bits))
        | (FieldType::UInt(right_bits), FieldType::Int(left_bits)) => {
            Some(resolve_mixed_signed_unsigned_int(*left_bits, *right_bits))
        },

        (FieldType::Float(left_bits), FieldType::Int(_))
        | (FieldType::Int(_), FieldType::Float(left_bits))
        | (FieldType::Float(left_bits), FieldType::UInt(_))
        | (FieldType::UInt(_), FieldType::Float(left_bits)) => {
            Some(FieldType::Float((*left_bits).max(64)))
        },

        (FieldType::Date, FieldType::Date)
        | (FieldType::DateTime, FieldType::DateTime)
        | (FieldType::Timestamp, FieldType::Timestamp) => Some(left.clone()),

        (FieldType::Date, FieldType::DateTime)
        | (FieldType::DateTime, FieldType::Date)
        | (FieldType::Date, FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::Date)
        | (FieldType::DateTime, FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::DateTime) => Some(FieldType::DateTime),

        (FieldType::StringFixed(left_len), FieldType::StringFixed(right_len)) => {
            Some(FieldType::StringFixed((*left_len).max(*right_len)))
        },

        (FieldType::StringFixed(_), FieldType::Text)
        | (FieldType::Text, FieldType::StringFixed(_))
        | (FieldType::Text, FieldType::Text) => Some(FieldType::Text),

        (FieldType::Enum(left_variants), FieldType::Enum(right_variants)) => {
            let left_max = max_enum_variant_len(left_variants);
            let right_max = max_enum_variant_len(right_variants);
            Some(FieldType::StringFixed(left_max.max(right_max).max(1)))
        },

        (FieldType::Enum(variants), FieldType::StringFixed(len))
        | (FieldType::StringFixed(len), FieldType::Enum(variants)) => {
            let enum_max = max_enum_variant_len(variants);
            Some(FieldType::StringFixed(enum_max.max(*len).max(1)))
        },

        (FieldType::Enum(_), FieldType::Text)
        | (FieldType::Text, FieldType::Enum(_)) => Some(FieldType::Text),

        (FieldType::Blob, FieldType::Blob) => Some(FieldType::Blob),
        (FieldType::Spatial, FieldType::Spatial) => Some(FieldType::Spatial),

        // First-pass MySQL-like coercion: mixing scalar/date/string-like families
        // yields textual result typing in UNION metadata.
        (FieldType::Int(_), FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Int(_))
        | (FieldType::Int(_), FieldType::Text)
        | (FieldType::Text, FieldType::Int(_))
        | (FieldType::UInt(_), FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::UInt(_))
        | (FieldType::UInt(_), FieldType::Text)
        | (FieldType::Text, FieldType::UInt(_))
        | (FieldType::Float(_), FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Float(_))
        | (FieldType::Float(_), FieldType::Text)
        | (FieldType::Text, FieldType::Float(_))
        | (FieldType::Date, FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Date)
        | (FieldType::Date, FieldType::Text)
        | (FieldType::Text, FieldType::Date)
        | (FieldType::DateTime, FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::DateTime)
        | (FieldType::DateTime, FieldType::Text)
        | (FieldType::Text, FieldType::DateTime)
        | (FieldType::Timestamp, FieldType::StringFixed(_))
        | (FieldType::StringFixed(_), FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::Text)
        | (FieldType::Text, FieldType::Timestamp)
        | (FieldType::Enum(_), FieldType::Int(_))
        | (FieldType::Int(_), FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::UInt(_))
        | (FieldType::UInt(_), FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::Float(_))
        | (FieldType::Float(_), FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::Date)
        | (FieldType::Date, FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::DateTime)
        | (FieldType::DateTime, FieldType::Enum(_))
        | (FieldType::Enum(_), FieldType::Timestamp)
        | (FieldType::Timestamp, FieldType::Enum(_)) => Some(FieldType::Text),

        _ => None,

    }

}

fn reconcile_union_column_metadata(
    base_column: &mut serverlib::FieldDef,
    branch_column: &serverlib::FieldDef,
    column_index: usize,
) -> Result<(), String> {

    let base_metadata = base_column.metadata.clone().unwrap_or_default();
    let branch_metadata = branch_column.metadata.clone().unwrap_or_default();

    let resolved_character_set = reconcile_union_metadata_value(
        base_metadata.character_set.as_deref(),
        branch_metadata.character_set.as_deref(),
        column_index,
        "character set",
    )?;

    let resolved_collation = reconcile_union_metadata_value(
        base_metadata.collation.as_deref(),
        branch_metadata.collation.as_deref(),
        column_index,
        "collation",
    )?;

    let resolved_visibility = if base_metadata.is_hidden() || branch_metadata.is_hidden() {
        common::schema::SystemFieldVisibility::Hidden
    } else {
        common::schema::SystemFieldVisibility::Visible
    };

    let resolved_metadata = common::schema::FieldMetadata {
        comment: base_metadata.comment.or(branch_metadata.comment),
        auto_increment: base_metadata.auto_increment || branch_metadata.auto_increment,
        original_sql_type: base_metadata.original_sql_type.or(branch_metadata.original_sql_type),
        character_set: resolved_character_set,
        collation: resolved_collation,
        system_visibility: resolved_visibility,
    };

    if resolved_metadata == common::schema::FieldMetadata::default() {
        base_column.metadata = None;
    } else {
        base_column.metadata = Some(resolved_metadata);
    }

    Ok(())

}

fn reconcile_union_metadata_value(
    base_value: Option<&str>,
    branch_value: Option<&str>,
    column_index: usize,
    label: &str,
) -> Result<Option<String>, String> {

    match (base_value, branch_value) {

        (Some(base), Some(branch)) if base.eq_ignore_ascii_case(branch) => {
            Ok(Some(base.to_string()))
        },

        (Some(base), None) => Ok(Some(base.to_string())),
        
        (None, Some(branch)) => Ok(Some(branch.to_string())),
        
        (None, None) => Ok(None),
        
        (Some(base), Some(branch)) => Err(format!(
            "UNION column {} {} mismatch: '{}' is not compatible with '{}'",
            column_index, label, base, branch
        )),

    }

}

fn compare_union_cell_values(
    left: Option<&Vec<u8>>,
    right: Option<&Vec<u8>>,
    column: Option<&serverlib::FieldDef>,
) -> Ordering {
    
    match (left, right) {

        (Some(left), Some(right)) => {
            let left_key = union_cell_compare_key(left, column);
            let right_key = union_cell_compare_key(right, column);
            left_key.cmp(&right_key)
        }

        (None, Some(_)) => Ordering::Less,

        (Some(_), None) => Ordering::Greater,

        (None, None) => Ordering::Equal,

    }

}

fn union_row_comparison_key(
    row: &[Vec<u8>],
    columns: &[serverlib::FieldDef],
) -> Vec<Vec<u8>> {
    row.iter()
        .enumerate()
        .map(|(index, cell)| union_cell_compare_key(cell, columns.get(index)))
        .collect()
}

fn union_cell_compare_key(cell: &[u8], column: Option<&serverlib::FieldDef>) -> Vec<u8> {
    let Some(column) = column else {
        return cell.to_vec();
    };

    if union_column_uses_case_insensitive_collation(column) {
        return String::from_utf8_lossy(cell).to_lowercase().into_bytes();
    }

    cell.to_vec()
}

fn union_column_uses_case_insensitive_collation(column: &serverlib::FieldDef) -> bool {
    let Some(collation) = column.metadata.as_ref().and_then(|metadata| metadata.collation.as_deref()) else {
        return false;
    };

    let normalized = collation.trim().to_ascii_lowercase();
    normalized.ends_with("_ci") || normalized.contains("_ci_")
}

fn resolve_mixed_signed_unsigned_int(left_signed_bits: u8, right_unsigned_bits: u8) -> serverlib::FieldType {
    // Keep UNION integer results in integer family instead of widening to float.
    // We conservatively promote mixed signed/unsigned values to signed 64-bit.
    if left_signed_bits >= right_unsigned_bits {
        serverlib::FieldType::Int(left_signed_bits.max(64))
    } else {
        serverlib::FieldType::Int(64)
    }
}

fn max_enum_variant_len(variants: &[String]) -> usize {
    variants
        .iter()
        .map(|variant| variant.len())
        .max()
        .unwrap_or(1)
}

fn apply_union_row_window(
    rows: Vec<Vec<Vec<u8>>>,
    limit: Option<usize>,
    offset: Option<usize>,
) -> Vec<Vec<Vec<u8>>> {
    let start = offset.unwrap_or(0).min(rows.len());
    let end = limit
        .map(|limit| start.saturating_add(limit).min(rows.len()))
        .unwrap_or(rows.len());

    rows.into_iter()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
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

    let cached_plan = (!is_explain)
        .then_some(statement.parsed_insert_plan.as_ref())
        .flatten();

    let parsed_plan = if cached_plan.is_none() {

        Some(match (!is_explain).then_some(statement.parsed_statement.as_ref()).flatten() {

            Some(parsed_statement) => match serverlib::parse_insert_rows_from_parsed_statement(parsed_statement) {
                Ok(plan) => plan,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("insert parse failed: {err}"),
                    );
                }
            },

            None => match serverlib::parse_insert_rows_from_statement(statement_sql) {
                Ok(plan) => plan,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("insert parse failed: {err}"),
                    );
                }
            },
            
        })

    } else {

        None

    };

    let plan = cached_plan.unwrap_or_else(|| {
        parsed_plan
            .as_ref()
            .expect("parsed plan should exist when cached insert plan is absent")
    });

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
            plan,
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
            .map(|field| field.field_name.as_str())
            .collect::<Vec<_>>()
    } else {
        plan.columns.iter().map(String::as_str).collect::<Vec<_>>()
    };

    let insert_column_fields = columns
        .iter()
        .map(|column| {
            schema
                .field(column)
                .ok_or_else(|| format!("insert failed: unknown column '{}'", column))
                .map(|field| (*column, field))
        })
        .collect::<Result<Vec<_>, _>>();

    let insert_column_fields = match insert_column_fields {
        Ok(fields) => fields,
        Err(message) => {
            return ConnectorResponse::rejected(request_id.to_string(), message);
        }
    };

    let missing_field_defaults = schema
        .fields
        .iter()
        .filter(|field| !columns.iter().any(|column| *column == field.field_name))
        .map(|field| {
            (
                field.field_name.as_str(),
                field.default_value.as_ref(),
                field.nullable,
            )
        })
        .collect::<Vec<_>>();

    let mut seen = HashSet::with_capacity(columns.len());

    for column in &columns {

        if !seen.insert(*column) {
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
            let mut pk_checks = 0u64;
            let mut staged_payloads = Vec::with_capacity(insert_rows.len());
            let track_runtime_indexes_for_insert = derived_indexes_for_table(table).next().is_some();
            let mut staged_row_maps = if track_runtime_indexes_for_insert {
                Vec::with_capacity(insert_rows.len())
            } else {
                Vec::new()
            };
            let mut staged_pk_keys = HashSet::<Vec<Vec<u8>>>::new();
            let primary_key_details = primary_key_index(table).map(|pk_index| {
                let pk_fields = if pk_index.field_names.is_empty() && !pk_index.field_name.is_empty() {
                    vec![pk_index.field_name.as_str()]
                } else {
                    pk_index.field_names.iter().map(|name| name.as_str()).collect()
                };

                (pk_index.index_id.0.as_str(), pk_fields)
            });

            for row in insert_rows.iter() {

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

                for ((column, field), value) in insert_column_fields.iter().zip(row.iter().cloned()) {

                    match value {

                        Some(value_bytes) => {
                            payload_row.insert((*column).to_string(), value_bytes);
                        },

                        None => {

                            if let Some(default) = &field.default_value {
                                payload_row.insert((*column).to_string(), default.clone());
                            } else if !field.nullable {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!("insert failed: column '{}' cannot be null", column),
                                );
                            }

                        }

                    }

                }

                for (field_name, default_value, nullable) in &missing_field_defaults {

                    if let Some(default) = default_value {
                        payload_row.insert((*field_name).to_string(), (*default).clone());
                        continue;
                    }

                    if !nullable {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!(
                                "insert failed: missing required column '{}'",
                                field_name
                            ),
                        );
                    }

                }

                if let Some((pk_index_id, pk_fields)) = primary_key_details.as_ref() {
                    
                    pk_checks = pk_checks.saturating_add(1);

                    let incoming_pk = pk_fields
                        .iter()
                        .map(|pk| payload_row.get(*pk).cloned().unwrap_or_default())
                        .collect::<Vec<_>>();

                    let pk_runtime = runtime_indexes.index(pk_index_id);

                    if pk_runtime
                        .map(|idx| idx.contains(&incoming_pk))
                        .unwrap_or(false)
                        || staged_pk_keys.contains(&incoming_pk)
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

                    staged_pk_keys.insert(incoming_pk);

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

                staged_payloads.push(encoded);

                if track_runtime_indexes_for_insert {
                    staged_row_maps.push(payload_row);
                }

                affected_rows = affected_rows.saturating_add(1);

            }

            if let Err(err) = append_row_payload_records_batch(
                wal,
                &plan.table_id,
                table,
                runtime_indexes,
                TransactionKind::Insert,
                staged_payloads,
                Some(staged_row_maps),
                common::epoch_nanos!(),
                Some(group_id),
            ) {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("insert WAL append failed: {err}"),
                );
            }

            log::debug!(
                "insert execution table={} rows={} pk_checks={} runtime_indexes={}",
                table.table_id,
                affected_rows,
                pk_checks,
                table.indexes.len(),
            );

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

fn materialize_insert_source_rows<'a>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    source: &'a serverlib::InsertRowsSource,
) -> Result<Cow<'a, [Vec<Option<Vec<u8>>>]>, String> {

    match source {

        serverlib::InsertRowsSource::Values(rows) => Ok(Cow::Borrowed(rows.as_slice())),

        serverlib::InsertRowsSource::Select(read_plan) => {

            let select_result = if !read_plan.joins.is_empty() {

                serverlib::execute_joined_select_plan(
                    catalog,
                    wal,
                    runtime_indexes,
                    &read_plan,
                    &mut |function| evaluate_inbuilt_sql_function(function),
                    &mut |row_map, condition| {
                        Ok(serverlib::row_matches_select_condition(
                            row_map,
                            condition,
                            catalog,
                            wal,
                            runtime_indexes,
                        ))
                    },
                    &mut |row_tuple, condition| {
                        Ok(serverlib::row_matches_select_condition(
                            row_tuple,
                            condition,
                            catalog,
                            wal,
                            runtime_indexes,
                        ))
                    },
                )

            } else if read_plan.table_id.is_empty() {
                
                serverlib::execute_projection_only_select_plan(&read_plan, &mut |function| {
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
                    &read_plan,
                    &access_plan,
                    &mut |function| evaluate_inbuilt_sql_function(function),
                    &mut |row_map, condition| {
                        Ok(serverlib::row_matches_select_condition(
                            row_map,
                            condition,
                            catalog,
                            wal,
                            runtime_indexes,
                        ))
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

            Ok(Cow::Owned(rows))

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

    let primary_key = primary_key_index(table);
    let primary_key_fields = primary_key.map(|pk_index| {
        if pk_index.field_names.is_empty() && !pk_index.field_name.is_empty() {
            vec![pk_index.field_name.as_str()]
        } else {
            pk_index
                .field_names
                .iter()
                .map(|name| name.as_str())
                .collect::<Vec<_>>()
        }
    });

    let mut pk_keys = if let Some(pk_index) = primary_key {
        current_live_rows
            .iter()
            .map(|(_, row_map)| index_value_tuple(pk_index, row_map))
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    let current_live_row_ids = current_live_rows
        .iter()
        .map(|(row_id, _)| *row_id)
        .collect::<HashSet<_>>();

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
                            format!("update delete payload encode failed: {err}"),
                        );
                    }
                };

                let old_pk = primary_key.map(|pk_index| index_value_tuple(pk_index, &row_map));

                let mut updated_row = row_map;

                for assignment in &plan.assignments {
                    match &assignment.value {
                        Some(value) => {
                            if let Some(slot) = updated_row.get_mut(&assignment.field_name) {
                                *slot = value.clone();
                            } else {
                                updated_row.insert(assignment.field_name.clone(), value.clone());
                            }
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

                if let Some(pk_index) = primary_key {

                    let old_pk = old_pk.expect("primary key tuple should exist when primary key index is present");
                    let new_pk = index_value_tuple(pk_index, &updated_row);

                    if old_pk != new_pk && pk_keys.contains(&new_pk) {
                        let pk_display = primary_key_fields
                            .as_ref()
                            .expect("primary key fields should exist when primary key index is present")
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

                if let Err(err) = append_row_payload_record_with_live_row_ids(
                    wal,
                    &plan.table_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Delete,
                    delete_payload,
                    common::epoch_nanos!(),
                    Some(TransactionId(row_id)),
                    Some(&current_live_row_ids),
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

    let current_live_row_ids = current_live_rows
        .iter()
        .map(|(row_id, _)| *row_id)
        .collect::<HashSet<_>>();

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

                if let Err(err) = append_row_payload_record_with_live_row_ids(
                    wal,
                    &plan.table_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Delete,
                    delete_payload,
                    common::epoch_nanos!(),
                    Some(TransactionId(row_id)),
                    Some(&current_live_row_ids),
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
            Ok(serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            ))
        },
    )
    .map(|rows| {
        rows.into_iter()
            .map(|row| (row.row_id, row.row_map))
            .collect()
    })

}

fn execute_select_plan_result(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
) -> Result<serverlib::SelectExecutionResult, String> {
    if !read_plan.joins.is_empty() {
        return serverlib::execute_joined_select_plan(
            catalog,
            wal,
            runtime_indexes,
            read_plan,
            &mut |function| evaluate_inbuilt_sql_function(function),
            &mut |row_map, condition| {
                Ok(serverlib::row_matches_select_condition(
                    row_map,
                    condition,
                    catalog,
                    wal,
                    runtime_indexes,
                ))
            },
            &mut |row_tuple, condition| {
                Ok(serverlib::row_matches_select_condition(
                    row_tuple,
                    condition,
                    catalog,
                    wal,
                    runtime_indexes,
                ))
            },
        );
    }

    if read_plan.table_id.is_empty() {
        return serverlib::execute_projection_only_select_plan(read_plan, &mut |function| {
            evaluate_inbuilt_sql_function(function)
        });
    }

    let table_id = read_plan.table_id.as_str();

    let schema = catalog
        .table_schema(table_id)
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;

    let table = catalog
        .table(table_id)
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;

    let mut index_filter_map = HashMap::new();
    let allow_index_short_circuit = read_plan
        .where_condition
        .as_ref()
        .map(|condition| collect_indexable_equality_filters(condition, &mut index_filter_map))
        .unwrap_or(true);

    let access_plan = plan_relation_access(table, allow_index_short_circuit, index_filter_map);

    serverlib::execute_relation_select_plan(
        wal,
        table,
        schema,
        runtime_indexes,
        read_plan,
        &access_plan,
        &mut |function| evaluate_inbuilt_sql_function(function),
        &mut |row_map, condition| {
            Ok(serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            ))
        },
    )
}

fn extract_view_select_sql(view_sql: &str) -> Result<String, String> {
    let trimmed = view_sql.trim();
    let lowered = trimmed.to_ascii_lowercase();

    if lowered.starts_with("select ") {
        return Ok(trimmed.to_string());
    }

    if let Some(as_index) = lowered.find(" as ") {
        let select_sql = trimmed[(as_index + 4)..].trim();
        if select_sql.to_ascii_lowercase().starts_with("select ") {
            return Ok(select_sql.to_string());
        }
    }

    Err("view execution failed: could not extract SELECT body from view definition".to_string())
}

fn execute_select_impl(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
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

    if statement_sql_lower.starts_with("describe ")
        || statement_sql_lower.starts_with("desc ")
        || statement_sql_lower.starts_with("show columns")
    {

        let Some(catalog) = resolve_catalog(catalogs, &query.database_id) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", query.database_id),
            );
        };

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

    if !read_plan.ctes.is_empty() {
        let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", query.database_id),
            );
        };

        let result = match execute_select_with_ctes(catalog, wal, runtime_indexes, &read_plan) {
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

    if !read_plan.joins.is_empty() {
        let Some(catalog) = resolve_catalog(catalogs, &query.database_id) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", query.database_id),
            );
        };
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
                    &read_plan,
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

    let view_sql = resolve_catalog(catalogs, &query.database_id)
        .and_then(|catalog| catalog.view(table_id))
        .map(|view| view.sql.clone());

    if let Some(view_sql) = view_sql {
        
        let view_select_sql = match extract_view_select_sql(&view_sql) {
            Ok(sql) => sql,
            Err(message) => {
                return ConnectorResponse::rejected(request_id.to_string(), message);
            }
        };

        let view_read_plan = match serverlib::parse_select_read_plan_from_statement(&view_select_sql) {
            Ok(plan) => plan,
            Err(err) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("view execution failed: {err}"),
                );
            }
        };

        let view_result = match resolve_catalog(catalogs, &query.database_id) {
            Some(catalog) => {
                match execute_select_plan_result(catalog, wal, runtime_indexes, &view_read_plan) {
                    Ok(result) => result,
                    Err(message) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("view execution failed: {message}"),
                        );
                    }
                }
            }
            None => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", query.database_id),
                );
            }
        };

        let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", query.database_id),
            );
        };

        let result = match execute_view_over_scoped_materialization(
            catalog,
            wal,
            runtime_indexes,
            table_id,
            &read_plan,
            view_result,
        ) {
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

    let Some(catalog) = resolve_catalog(catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

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
                &read_plan,
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
            Ok(serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            ))
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

fn execute_view_over_scoped_materialization(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    view_table_id: &str,
    read_plan: &serverlib::SelectReadPlan,
    view_result: serverlib::SelectExecutionResult,
) -> Result<serverlib::SelectExecutionResult, String> {
    let scoped_table_id = format!("__scoped_view_{}_{}", view_table_id, common::epoch_nanos!());

    let mut scoped_handle = serverlib::create_scoped_ephemeral_table(
        catalog,
        wal,
        scoped_table_id,
        TableSchema::new(view_result.columns.clone()),
    )
    .map_err(|message| format!("view execution failed: {message}"))?;

    let scoped_result = (|| -> Result<serverlib::SelectExecutionResult, String> {
        let scoped_table_id = scoped_handle.table_id().to_string();

        materialize_select_result_into_scoped_table(
            catalog,
            wal,
            runtime_indexes,
            &scoped_table_id,
            &view_result,
        )?;

        let scoped_read_plan = remap_select_read_plan_table(read_plan, &scoped_table_id);

        execute_select_plan_result(catalog, wal, runtime_indexes, &scoped_read_plan)
            .map_err(|message| format!("view execution failed: {message}"))
    })();

    serverlib::release_scoped_ephemeral_table(catalog, wal, &mut scoped_handle)
        .map_err(|err| format!("view execution failed: scoped release failed: {err}"))?;

    scoped_result
}

fn execute_select_with_ctes(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
) -> Result<serverlib::SelectExecutionResult, String> {
    let mut scoped_handles = Vec::with_capacity(read_plan.ctes.len());

    let execution_result = (|| -> Result<serverlib::SelectExecutionResult, String> {
        for cte in &read_plan.ctes {
            if catalog.table(&cte.table_id).is_some() || catalog.view(&cte.table_id).is_some() {
                return Err(format!(
                    "cte execution failed: cte '{}' conflicts with existing table/view",
                    cte.table_id
                ));
            }

            let cte_result = execute_select_plan_result(catalog, wal, runtime_indexes, &cte.read_plan)
                .map_err(|message| format!("cte execution failed: {message}"))?;

            let scoped_handle = serverlib::create_scoped_ephemeral_table(
                catalog,
                wal,
                cte.table_id.clone(),
                TableSchema::new(cte_result.columns.clone()),
            )
            .map_err(|message| format!("cte execution failed: {message}"))?;

            materialize_select_result_into_scoped_table(
                catalog,
                wal,
                runtime_indexes,
                scoped_handle.table_id(),
                &cte_result,
            )
            .map_err(|message| format!("cte execution failed: {message}"))?;

            scoped_handles.push(scoped_handle);
        }

        let mut main_plan = read_plan.clone();
        main_plan.ctes.clear();

        execute_select_plan_result(catalog, wal, runtime_indexes, &main_plan)
            .map_err(|message| format!("cte execution failed: {message}"))
    })();

    let mut release_error = None;
    for handle in scoped_handles.iter_mut().rev() {
        if let Err(err) = serverlib::release_scoped_ephemeral_table(catalog, wal, handle)
            && release_error.is_none()
        {
            release_error = Some(format!("cte execution failed: scoped release failed: {err}"));
        }
    }

    if let Some(err) = release_error {
        return Err(err);
    }

    execution_result
}

fn materialize_select_result_into_scoped_table(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    scoped_table_id: &str,
    view_result: &serverlib::SelectExecutionResult,
) -> Result<(), String> {
    let scoped_table = catalog
        .table(scoped_table_id)
        .ok_or_else(|| "view execution failed: scoped table not found".to_string())?;

    let scoped_schema = scoped_table.schema().clone();

    for row in &view_result.rows {
        let mut row_map = HashMap::with_capacity(view_result.columns.len());

        for (column_index, column) in view_result.columns.iter().enumerate() {
            let value = row.get(column_index).cloned().unwrap_or_else(|| b"NULL".to_vec());
            row_map.insert(column.field_name.clone(), value);
        }

        let encoded = encode_row_payload(&scoped_schema, &row_map)
            .map_err(|err| format!("view execution failed: scoped row encode failed: {err}"))?;

        append_row_payload_record(
            wal,
            scoped_table_id,
            scoped_table,
            runtime_indexes,
            TransactionKind::Insert,
            encoded,
            common::epoch_nanos!(),
            None,
            None,
        )
        .map_err(|err| format!("view execution failed: scoped row append failed: {err}"))?;
    }

    Ok(())
}

fn remap_select_read_plan_table(
    read_plan: &serverlib::SelectReadPlan,
    scoped_table_id: &str,
) -> serverlib::SelectReadPlan {
    let mut scoped_read_plan = read_plan.clone();
    let original_table_id = scoped_read_plan.table_id.clone();
    scoped_read_plan.table_id = scoped_table_id.to_string();

    for relation in &mut scoped_read_plan.relations {
        if relation.table_id == original_table_id {
            relation.table_id = scoped_table_id.to_string();
        }
    }

    scoped_read_plan
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
            Ok(serverlib::row_matches_select_condition(
                row_map,
                condition,
                catalog,
                wal,
                runtime_indexes,
            ))
        },
        &mut |row_tuple, condition| {
            Ok(serverlib::row_matches_select_condition(
                row_tuple,
                condition,
                catalog,
                wal,
                runtime_indexes,
            ))
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

    let last_id = wal.latest_transaction_id(wal_id);
    let next_id = TransactionId(last_id.map(|id| id.0 + 1).unwrap_or(1));
    let refid = last_id;
    
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
    append_row_payload_record_with_live_row_ids(
        wal,
        wal_id,
        table,
        runtime_indexes,
        kind,
        payload,
        timestamp_epoch_ms,
        refid,
        None,
        group_id,
    )
}

fn append_row_payload_record_with_live_row_ids(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payload: Vec<u8>,
    timestamp_epoch_ms: u64,
    refid: Option<TransactionId>,
    expected_live_row_ids: Option<&HashSet<u64>>,
    group_id: Option<TransactionId>,
) -> Result<(), String> {
    let mutation_start = Instant::now();

    if let Some(expected_refid) = refid {
        let live_row_check_start = Instant::now();
        let live_row_exists = if let Some(live_row_ids) = expected_live_row_ids {
            live_row_ids.contains(&expected_refid.0)
        } else {
            let live_row_ids = load_live_rows(wal, wal_id, table.schema())
                .into_iter()
                .map(|(row_id, _)| row_id)
                .collect::<HashSet<_>>();
            live_row_ids.contains(&expected_refid.0)
        };
        let live_row_check_ms = live_row_check_start.elapsed().as_millis() as u64;

        if !live_row_exists {
            return Err(format!(
                "row mutation references stale or missing live transaction id {}",
                expected_refid.0
            ));
        }

        if live_row_check_ms > 0 {
            log::info!(
                "mutation live-row check timing table={} kind={:?} live_row_check_ms={}",
                table.table_id,
                kind,
                live_row_check_ms,
            );
        }
    }

    let last_id = wal.latest_transaction_id(wal_id);
    let next_id = TransactionId(last_id.map(|id| id.0 + 1).unwrap_or(1));
    let refid = refid.or(last_id);

    let row_decode_start = Instant::now();
    let derived_indexes = derived_indexes_for_table(table).collect::<Vec<_>>();
    let track_runtime_indexes = !derived_indexes.is_empty();

    let row_map = if track_runtime_indexes {
        Some(
            decode_row_payload(table.schema(), &payload)
                .map_err(|err| format!("row payload decode failed: {err}"))?,
        )
    } else {
        None
    };
    let row_decode_ms = row_decode_start.elapsed().as_millis() as u64;

    let record = TransactionRecord {
        id: next_id,
        groupid: group_id,
        refid,
        timestamp_epoch_ms,
        actor: UserId::from_username("server"),
        kind,
        payload,
    };

    let wal_append_start = Instant::now();
    wal.append(wal_id, record).map_err(|e| e.to_string())?;
    let wal_append_ms = wal_append_start.elapsed().as_millis() as u64;

    let index_apply_start = Instant::now();

    if let Some(row_map) = row_map.as_ref() {
        runtime_indexes.apply_table_row_mutation(derived_indexes.iter().copied(), kind, row_map);
    }

    let index_apply_ms = index_apply_start.elapsed().as_millis() as u64;
    let total_ms = mutation_start.elapsed().as_millis() as u64;

    log::debug!(
        "mutation timing table={} kind={:?} track_runtime_indexes={} row_decode_ms={} wal_append_ms={} index_apply_ms={} total_ms={}",
        table.table_id,
        kind,
        track_runtime_indexes,
        row_decode_ms,
        wal_append_ms,
        index_apply_ms,
        total_ms,
    );

    Ok(())

}

pub(super) fn append_row_payload_records_batch(
    wal: &ConcurrentWalManager,
    wal_id: &str,
    table: &serverlib::DatabaseTable,
    runtime_indexes: &mut RuntimeIndexStore,
    kind: TransactionKind,
    payloads: Vec<Vec<u8>>,
    prepared_row_maps: Option<Vec<HashMap<String, Vec<u8>>>>,
    timestamp_epoch_ms: u64,
    group_id: Option<TransactionId>,
) -> Result<(), String> {
    if payloads.is_empty() {
        return Ok(());
    }

    let batch_start = Instant::now();
    let derived_indexes = derived_indexes_for_table(table).collect::<Vec<_>>();
    let track_runtime_indexes = !derived_indexes.is_empty();
    let payload_count = payloads.len();

    let last_id = wal.latest_transaction_id(wal_id);
    let mut next_id = last_id.map(|id| id.0.saturating_add(1)).unwrap_or(1);
    let mut refid = last_id;
    let actor = UserId::from_username("server");

    let mut records = Vec::with_capacity(payloads.len());
    let mut row_maps = prepared_row_maps.unwrap_or_default();
    let use_prepared_row_maps = track_runtime_indexes && row_maps.len() == payload_count;

    if track_runtime_indexes && !use_prepared_row_maps {
        row_maps = Vec::with_capacity(payload_count);
    }

    let materialize_start = Instant::now();

    for payload in payloads {
        if track_runtime_indexes && !use_prepared_row_maps {
            let row_map = decode_row_payload(table.schema(), &payload)
                .map_err(|err| format!("row payload decode failed: {err}"))?;
            row_maps.push(row_map);
        }

        let tx_id = TransactionId(next_id);
        next_id = next_id.saturating_add(1);

        records.push(TransactionRecord {
            id: tx_id,
            groupid: group_id,
            refid,
            timestamp_epoch_ms,
            actor: actor.clone(),
            kind,
            payload,
        });

        refid = Some(tx_id);
    }

    let materialize_us = materialize_start.elapsed().as_micros() as u64;

    let wal_append_start = Instant::now();
    wal.append_batch(wal_id, records)
        .map_err(|err| err.to_string())?;
    let wal_append_us = wal_append_start.elapsed().as_micros() as u64;

    let index_apply_start = Instant::now();

    if track_runtime_indexes {
        match kind {
            TransactionKind::Delete => {
                runtime_indexes.remove_table_rows_batch(&derived_indexes, &row_maps);
            }

            TransactionKind::Insert | TransactionKind::Update => {
                runtime_indexes.record_table_rows_batch(&derived_indexes, &row_maps);
            }

            _ => {}
        }
    }

    let index_apply_us = index_apply_start.elapsed().as_micros() as u64;
    let total_us = batch_start.elapsed().as_micros() as u64;

    if track_runtime_indexes && index_apply_us >= 5_000 {
        let mut index_stats = Vec::with_capacity(derived_indexes.len());

        for index in &derived_indexes {
            let index_id = &index.index_id.0;
            let stats = runtime_indexes
                .stats(index_id)
                .map(|(cardinality, capacity)| format!("{}:{}/{}", index_id, cardinality, capacity))
                .unwrap_or_else(|| format!("{}:missing", index_id));
            index_stats.push(stats);
        }

        log::warn!(
            "batch mutation index spike table={} kind={:?} rows={} index_apply_us={} index_stats=[{}]",
            table.table_id,
            kind,
            payload_count,
            index_apply_us,
            index_stats.join(", "),
        );
    }

    log::debug!(
        "batch mutation timing table={} kind={:?} rows={} track_runtime_indexes={} materialize_us={} wal_append_us={} index_apply_us={} total_us={}",
        table.table_id,
        kind,
        payload_count,
        track_runtime_indexes,
        materialize_us,
        wal_append_us,
        index_apply_us,
        total_us,
    );

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
            log::warn!(
                "runtime index rebuild triggered table={} reason=write_commit_failed error={}",
                table.table_id,
                err
            );
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

        log::warn!(
            "runtime index rebuild triggered table={} reason=statement_response_rejected",
            table.table_id
        );
        
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

        log::warn!(
            "runtime index rebuild triggered table={} reason=external_write_group_abort",
            table_id
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
