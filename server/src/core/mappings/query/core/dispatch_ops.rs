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

        SqlOperation::CallStoredProcedure => Some(execute_call_stored_procedure),
        
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

fn execute_call_stored_procedure(
    ctx: &mut QueryExecutionContext<'_>,
    request_id: &str,
    query: &DataQuery,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let Some(procedure_id) = statement.object_name.as_deref() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            "call procedure missing identifier",
        );
    };

    let Some(catalog) = resolve_catalog(ctx.catalogs, &query.database_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", query.database_id),
        );
    };

    let Some(procedure) = catalog.stored_procedure(procedure_id).cloned() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("stored procedure '{}' not found", procedure_id),
        );
    };

    let mut local_entities = serverlib::ProcedureLocalEntityScope::new(format!(
        "proc_{}_{}",
        common::normalize_identifier!(ctx.session_id),
        procedure.procedure_id,
    ));

    if let Some(parsed_call_statement) = statement.parsed_statement.as_ref() {
        let argument_bindings =
            match serverlib::bind_call_procedure_arguments(&procedure.sql, parsed_call_statement) {
                Ok(bindings) => bindings,
                Err(err) => {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("call procedure argument binding failed: {err}"),
                    );
                }
            };

        for (name, value) in argument_bindings {
            local_entities.set_argument(name, value);
        }
    }

    let provider = local_entities.materialize_value_bindings();

    let invocation_result = serverlib::execute_stored_procedure_invocation(
        &provider,
        &procedure,
        serverlib::EntityInvocationSource::DirectedUser,
        &mut |action_sql| {
            let parsed_action_sql = serverlib::parse_mysql8_sql_requests(action_sql, &query.database_id)
                .map_err(|err| format!("call action parse failed: {err}"))?;

            let mut last_response: Option<ConnectorResponse> = None;

            for parsed_statement in parsed_action_sql {

                if matches!(parsed_statement.operation, SqlOperation::CreateTable) {

                    let plan = serverlib::create_table_plan_from_statement(&parsed_statement.sql)
                        .map_err(|err| format!("call action create table parse failed: {err}"))?;

                    if plan.temporary {
                        let Some(catalog) = resolve_catalog_mut(ctx.catalogs, &query.database_id) else {
                            return Err(format!("database '{}' not found", query.database_id));
                        };

                        local_entities.create_temporary_table(
                            catalog,
                            ctx.wal,
                            plan.table_id,
                            plan.schema,
                        )?;

                        last_response = Some(ConnectorResponse::applied(
                            request_id.to_string(),
                            ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
                        ));
                        
                        continue;
                    
                    }

                }

                let rewritten_sql = rewrite_sql_with_call_aliases(
                    &parsed_statement.sql,
                    &local_entities,
                )?;
                let rewritten_parsed = serverlib::parse_mysql8_sql_requests(
                    &rewritten_sql,
                    &query.database_id,
                )
                .map_err(|err| format!("call action parse failed after alias rewrite: {err}"))?;

                if rewritten_parsed.len() != 1 {
                    return Err("call action rewrite produced unsupported multi-statement execution".to_string());
                }

                let action_query = DataQuery {
                    database_id: query.database_id.clone(),
                    sql: rewritten_sql,
                };

                let response = execute_parsed_query(
                    request_id,
                    &action_query,
                    ctx.catalogs,
                    ctx.wal,
                    ctx.node_data_dir,
                    ctx.runtime_indexes,
                    rewritten_parsed,
                    ctx.external_write_group_id,
                    None,
                    ctx.session_id,
                );

                if matches!(response.status, connector::ResponseStatus::Rejected) {
                    let message = match response.result {
                        ConnectorResult::Error(message) => message,
                        _ => "call action execution failed".to_string(),
                    };
                    return Err(message);
                }

                if matches!(parsed_statement.operation, SqlOperation::DropTable)
                    && let Some(dropped_name) = parsed_statement.object_name.as_deref()
                {
                    local_entities.mark_temporary_table_dropped(dropped_name);
                }

                last_response = Some(response);
            }

            Ok(last_response.unwrap_or_else(|| {
                ConnectorResponse::applied(
                    request_id.to_string(),
                    ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
                )
            }))
        },
    );

    let cleanup_result = match resolve_catalog_mut(ctx.catalogs, &query.database_id) {
        Some(catalog) => local_entities.cleanup(catalog, ctx.wal),
        None => Err(format!("database '{}' not found", query.database_id)),
    };

    match (invocation_result, cleanup_result) {
        (Ok(Some(response)), Ok(())) => response,

        (Ok(None), Ok(())) => ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
        ),

        (Err(err), Ok(())) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("call procedure failed: {err}"),
        ),

        (Ok(_), Err(cleanup_err)) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("call procedure cleanup failed: {cleanup_err}"),
        ),

        (Err(err), Err(cleanup_err)) => ConnectorResponse::rejected(
            request_id.to_string(),
            format!("call procedure failed: {err}; cleanup failed: {cleanup_err}"),
        ),
    }

}

fn rewrite_sql_with_call_aliases(
    sql: &str,
    local_entities: &serverlib::ProcedureLocalEntityScope,
) -> Result<String, String> {
    if !local_entities.has_temporary_tables() {
        return Ok(sql.to_string());
    }

    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    while i < chars.len() {
        let c = chars[i];

        if c == '\'' && !in_double_quote && !in_backtick {
            in_single_quote = !in_single_quote;
            out.push(c);
            i += 1;
            continue;
        }

        if c == '"' && !in_single_quote && !in_backtick {
            in_double_quote = !in_double_quote;
            out.push(c);
            i += 1;
            continue;
        }

        if c == '`' && !in_single_quote && !in_double_quote {
            in_backtick = !in_backtick;
            out.push(c);
            i += 1;
            continue;
        }

        if in_single_quote || in_double_quote || in_backtick {
            out.push(c);
            i += 1;
            continue;
        }

        if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            i += 1;
            while i < chars.len() {
                let next = chars[i];
                if next.is_ascii_alphanumeric() || next == '_' {
                    i += 1;
                } else {
                    break;
                }
            }

            let token = chars[start..i].iter().collect::<String>();
            if let Some(mapped) = local_entities.resolve_temporary_table_id_checked(token.as_str())? {
                out.push('`');
                out.push_str(mapped);
                out.push('`');
            } else {
                out.push_str(&token);
            }
            continue;
        }

        out.push(c);
        i += 1;
    }

    Ok(out)
}

