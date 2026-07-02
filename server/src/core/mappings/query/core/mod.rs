

mod dispatch_ops;
mod ddl_ops;
mod mutation_ops;
mod select_ops;
mod set_ops;
mod wal_ops;


use std::borrow::Cow;
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
    ObjectStatus,
    TransactionRecord, UserId,
};

use serverlib::{
    RuntimeIndexStore, count_condition_predicates,
    collect_indexable_equality_filters_for_schema,
    collect_indexable_like_filter_for_schema,
    decode_row_payload, encode_row_payload, index_value_tuple,
    load_live_rows_with_context,
    plan_relation_access, primary_key_index,
};

use super::catalogs::{
    resolve_catalog,
    resolve_catalog_for_table_reference,
    resolve_catalog_for_table_reference_mut,
    resolve_catalog_mut,
};

use super::explain::{
    connector_field_defs, explain_inner_statement, explain_join_mutation_plan,
    explain_mutation_plan, explain_select_plan,
};

use super::timings::{empty_query_timings, make_query_timings, with_query_timings};

use dispatch_ops::execute_parsed_query;

use mutation_ops::{execute_delete_impl, execute_insert_impl, execute_update_impl};
use ddl_ops::{
    execute_alter_table_impl, execute_create_database_impl, execute_create_stored_procedure_impl,
    execute_create_table_impl, execute_create_trigger_impl,
    execute_drop_directive_impl,
};
pub(crate) use ddl_ops::execute_create_view_impl;
use set_ops::execute_union_query_impl;
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

