

mod ddl_ops;
mod mutation_ops;
mod select_ops;
mod set_ops;
mod wal_ops;


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
    RuntimeIndexStore, count_condition_predicates,
    collect_indexable_equality_filters_for_schema,
    decode_row_payload, encode_row_payload, index_value_tuple,
    load_live_rows_with_context,
    plan_relation_access, primary_key_index,
};

use super::catalogs::{resolve_catalog, resolve_catalog_mut};

use super::explain::{
    connector_field_defs, explain_inner_statement, explain_join_mutation_plan,
    explain_mutation_plan, explain_select_plan,
};

use super::timings::{empty_query_timings, make_query_timings, with_query_timings};

use mutation_ops::{execute_delete_impl, execute_insert_impl, execute_update_impl};
use ddl_ops::{
    execute_alter_table_impl, execute_create_database_impl, execute_create_stored_procedure_impl,
    execute_create_table_impl, execute_create_trigger_impl, execute_create_view_impl,
    execute_drop_directive_impl,
};
use set_ops::{
    apply_set_boundary_operation, apply_union_row_window, compare_union_cell_values,
    reconcile_union_column_types,
};
use select_ops::{execute_select_impl, execute_select_plan_result, execute_select_with_ctes};

use wal_ops::{
    append_row_payload_record_with_live_row_ids, append_row_payload_records_batch,
    payload_context_for_table, with_statement_write_batch,
};

pub(super) use wal_ops::append_row_payload_record;
pub(crate) use wal_ops::{abort_external_write_group, commit_external_write_group};





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

