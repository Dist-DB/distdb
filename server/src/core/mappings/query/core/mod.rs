

mod dispatch_ops;
mod ddl_ops;
mod mutation_ops;

mod select_ops;
mod set_ops;
mod variables;
mod wal_ops;

pub(crate) use variables::SessionVariableOverrides;


use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::cell::RefCell;

use common::helpers::format::{FileKind, HEADER_SIZE, make_header};
use common::helpers::write_bytes;
use connector::{ConnectorResponse, ConnectorResult, MutationResult, QueryResult};
use serverlib::engine::database::inbuilt::{
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
    execute_alter_table_impl, execute_alter_view_impl, execute_create_database_impl,
    execute_create_olap_view_impl,
    execute_create_other_impl, execute_create_stored_procedure_impl,
    execute_create_table_impl, execute_create_trigger_impl,
    execute_truncate_table_impl,
    execute_drop_directive_impl,
};
pub(crate) use ddl_ops::execute_create_view_impl;
use set_ops::execute_union_query_impl;
use select_ops::{execute_select_impl, execute_select_plan_result, execute_select_with_ctes};

use wal_ops::{
    append_row_payload_record_with_live_row_ids_and_prepared_row_map,
    append_row_payload_records_batch,
    payload_context_for_table, with_statement_write_batch,
};

#[allow(unused_imports)]
pub(super) use wal_ops::append_row_payload_record;
pub(super) use wal_ops::append_row_payload_record_with_prepared_row_map;
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

pub(super) struct QueryExecutionSessionContext {
    session_id: String,
    connection_id: usize,
    session_user: Option<String>,
    session_variable_overrides: Option<SessionVariableOverrides>,
}

impl QueryExecutionSessionContext {

    pub(super) fn new(
        session_id: impl Into<String>,
        connection_id: usize,
        session_user: Option<String>,
        session_variable_overrides: Option<SessionVariableOverrides>,
    ) -> Self {
        Self {
            session_id: session_id.into(),
            connection_id,
            session_user,
            session_variable_overrides,
        }
    }

    pub(super) fn session_id(&self) -> &str {
        self.session_id.as_str()
    }

    pub(super) fn session_variable_overrides(
        &self,
    ) -> Option<&SessionVariableOverrides> {
        self.session_variable_overrides.as_ref()
    }

    pub(super) fn take_session_variable_overrides(&mut self) -> Option<SessionVariableOverrides> {
        self.session_variable_overrides.take()
    }

    pub(super) fn replace_session_variable_overrides(
        &mut self,
        overrides: Option<SessionVariableOverrides>,
    ) {
        self.session_variable_overrides = overrides;
    }

}

pub(crate) fn handle_query_command(
    request_id: &str,
    database_id: &str,
    sql: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
) -> ConnectorResponse {
    let mut session_variable_overrides = SessionVariableOverrides::new();

    handle_query_command_with_session_variables(
        request_id,
        database_id,
        sql,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        session_id,
        connection_id,
        session_user,
        &mut session_variable_overrides,
    )

}

pub(crate) fn handle_query_command_with_session_variables(
    request_id: &str,
    database_id: &str,
    sql: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
    session_variable_overrides: &mut SessionVariableOverrides,
) -> ConnectorResponse {

    let (parsed, parse_ms) = match parse_query_requests_with_timing(
        request_id,
        database_id,
        sql,
    ) {
        Ok(parsed_query) => parsed_query,
        Err(response) => return response,
    };

    let mut session_context = QueryExecutionSessionContext::new(
        session_id,
        connection_id,
        session_user,
        Some(std::mem::take(session_variable_overrides)),
    );

    let runtime_context = inbuilt_runtime_context_for_query(
        request_id,
        database_id,
        &session_context,
        catalogs,
        session_context.session_variable_overrides(),
    );

    let response = with_inbuilt_sql_runtime_context(&runtime_context, || {
        handle_query_command_internal_with_parsed(
            request_id,
            database_id,
            catalogs,
            wal,
            node_data_dir,
            runtime_indexes,
            parsed,
            parse_ms,
            None,
            None,
            &mut session_context,
        )
    });

    *session_variable_overrides = session_context
        .take_session_variable_overrides()
        .unwrap_or_default();

    response

}

pub(crate) fn handle_query_command_with_parsed(
    request_id: &str,
    database_id: &str,
    parsed: Vec<SqlRequest>,
    parse_ms: u64,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
) -> ConnectorResponse {
    let mut session_variable_overrides = SessionVariableOverrides::new();

    handle_query_command_with_parsed_and_session_variables(
        request_id,
        database_id,
        parsed,
        parse_ms,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        session_id,
        connection_id,
        session_user,
        &mut session_variable_overrides,
    )

}

pub(crate) fn handle_query_command_with_parsed_and_session_variables(
    request_id: &str,
    database_id: &str,
    parsed: Vec<SqlRequest>,
    parse_ms: u64,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
    session_variable_overrides: &mut SessionVariableOverrides,
) -> ConnectorResponse {

    let mut session_context = QueryExecutionSessionContext::new(
        session_id,
        connection_id,
        session_user,
        Some(std::mem::take(session_variable_overrides)),
    );

    let runtime_context = inbuilt_runtime_context_for_query(
        request_id,
        database_id,
        &session_context,
        catalogs,
        session_context.session_variable_overrides(),
    );

    let response = with_inbuilt_sql_runtime_context(&runtime_context, || {
        handle_query_command_internal_with_parsed(
            request_id,
            database_id,
            catalogs,
            wal,
            node_data_dir,
            runtime_indexes,
            parsed,
            parse_ms,
            None,
            None,
            &mut session_context,
        )
    });

    *session_variable_overrides = session_context
        .take_session_variable_overrides()
        .unwrap_or_default();

    response

}

pub(crate) fn handle_query_command_in_write_group(
    request_id: &str,
    database_id: &str,
    sql: &str,
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
    let mut session_variable_overrides = SessionVariableOverrides::new();

    handle_query_command_in_write_group_with_session_variables(
        request_id,
        database_id,
        sql,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        write_group_id,
        touched_tables,
        session_id,
        connection_id,
        session_user,
        &mut session_variable_overrides,
    )

}

pub(crate) fn handle_query_command_in_write_group_with_session_variables(
    request_id: &str,
    database_id: &str,
    sql: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    write_group_id: TransactionId,
    touched_tables: &mut HashSet<String>,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
    session_variable_overrides: &mut SessionVariableOverrides,
) -> ConnectorResponse {

    let (parsed, parse_ms) = match parse_query_requests_with_timing(
        request_id,
        database_id,
        sql,
    ) {
        Ok(parsed_query) => parsed_query,
        Err(response) => return response,
    };

    handle_query_command_in_write_group_with_parsed_and_session_variables(
        request_id,
        database_id,
        parsed,
        parse_ms,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        write_group_id,
        touched_tables,
        session_id,
        connection_id,
        session_user,
        session_variable_overrides,
    )

}

pub(crate) fn handle_query_command_in_write_group_with_parsed_and_session_variables(
    request_id: &str,
    database_id: &str,
    parsed: Vec<SqlRequest>,
    parse_ms: u64,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    write_group_id: TransactionId,
    touched_tables: &mut HashSet<String>,
    session_id: &str,
    connection_id: usize,
    session_user: Option<String>,
    session_variable_overrides: &mut SessionVariableOverrides,
) -> ConnectorResponse {

    let mut session_context = QueryExecutionSessionContext::new(
        session_id,
        connection_id,
        session_user,
        Some(std::mem::take(session_variable_overrides)),
    );

    let runtime_context = inbuilt_runtime_context_for_query(
        request_id,
        database_id,
        &session_context,
        catalogs,
        session_context.session_variable_overrides(),
    );

    let response = with_inbuilt_sql_runtime_context(&runtime_context, || {
        handle_query_command_internal_with_parsed(
            request_id,
            database_id,
            catalogs,
            wal,
            node_data_dir,
            runtime_indexes,
            parsed,
            parse_ms,
            Some(write_group_id),
            Some(touched_tables),
            &mut session_context,
        )
    });

    *session_variable_overrides = session_context
        .take_session_variable_overrides()
        .unwrap_or_default();

    response

}

fn inbuilt_runtime_context_for_query(
    _request_id: &str,
    database_id: &str,
    session_context: &QueryExecutionSessionContext,
    catalogs: &HashMap<String, DatabaseCatalog>,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> InbuiltSqlRuntimeContext {

    let system_user = std::env::var("USER")
        .ok()
        .map(|user| format!("{}@localhost", user))
        .unwrap_or_else(|| "root@localhost".to_string());

    let current_user = std::env::var("USER")
        .ok()
        .map(|user| format!("{}@localhost", user))
        .unwrap_or_else(|| "root@localhost".to_string());

    let session_user = session_context
        .session_user
        .clone()
        .unwrap_or_else(|| "root@localhost".to_string());

    let argument_bindings = resolve_catalog(catalogs, database_id)
        .map(|catalog| {
            variables::runtime_variable_bindings(
                catalog,
                session_variable_overrides,
                Some(session_user.as_str()),
            )
        })
        .unwrap_or_default();

    InbuiltSqlRuntimeContext {
        current_database: Some(database_id.to_string()),
        current_user: Some(current_user),
        session_user: Some(session_user),
        system_user: Some(system_user),
        connection_id: Some(session_context.connection_id as i64),
        last_insert_id: None,
        version: None,
        argument_bindings,
    }
    
}

#[expect(clippy::result_large_err, reason="we want to return a detailed error response for sql parse failures")]
fn parse_query_requests_with_timing(
    request_id: &str,
    database_id: &str,
    sql: &str,
) -> Result<(Vec<SqlRequest>, u64), ConnectorResponse> {

    let parse_start = Instant::now();

    match serverlib::parse_mysql8_sql_requests(sql, database_id) {
        Ok(parsed) => {
            let parse_ms = parse_start.elapsed().as_millis() as u64;
            Ok((parsed, parse_ms))
        }
        Err(err) => Err(ConnectorResponse::rejected(
            request_id.to_string(),
            format!("sql parse failed: {err}"),
        )),
    }

}

fn handle_query_command_internal_with_parsed(
    request_id: &str,
    database_id: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    parsed: Vec<SqlRequest>,
    parse_ms: u64,
    external_write_group_id: Option<TransactionId>,
    touched_tables: Option<&mut HashSet<String>>,
    session_context: &mut QueryExecutionSessionContext,
) -> ConnectorResponse {

    let request_start = Instant::now();

    if let Some(statement) = parsed.first() {
        log::debug!(
            "query directive parsed request_id={} database_id={} directive={:?} operation={:?} object_name={:?}",
            request_id,
            database_id,
            statement.directive,
            statement.operation,
            statement.object_name
        );
    }

    let response = execute_parsed_query(
        request_id,
        database_id,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        parsed,
        external_write_group_id,
        touched_tables,
        session_context,
    );

    with_query_timings(response, make_query_timings(request_start, parse_ms))

}

