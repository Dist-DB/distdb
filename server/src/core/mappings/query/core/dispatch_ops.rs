use super::*;

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

pub(super) fn execute_parsed_query(
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
    execute_union_query_impl(
        request_id,
        query,
        ctx.catalogs,
        ctx.wal,
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

