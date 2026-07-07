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

            let mut last_response: Option<ConnectorResponse> = None;
            let mut handler_runtime = LocalHandlerRuntime::default();
            let mut loop_runtime = LocalControlFlowRuntime::default();
            let control = execute_call_action_sql(
                action_sql,
                request_id,
                query,
                ctx,
                &mut local_entities,
                &mut last_response,
                false,
                &mut handler_runtime,
                &mut loop_runtime,
            )?;

            if !matches!(control, serverlib::LoopControlDirective::None) {
                return Err("loop control directive used outside a loop block".to_string());
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

fn split_sql_statements_for_call_action(sql: &str) -> Vec<String> {

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_backtick = false;

    for ch in sql.chars() {

        match ch {

            '\'' if !in_double_quote && !in_backtick => {
                in_single_quote = !in_single_quote;
                current.push(ch);
            },

            '"' if !in_single_quote && !in_backtick => {
                in_double_quote = !in_double_quote;
                current.push(ch);
            },

            '`' if !in_single_quote && !in_double_quote => {
                in_backtick = !in_backtick;
                current.push(ch);
            },

            ';' if !in_single_quote && !in_double_quote && !in_backtick => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(trimmed.to_string());
                }
                current.clear();
            },

            _ => current.push(ch),

        }

    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(trimmed.to_string());
    }

    coalesce_compound_blocks(statements)
}

fn coalesce_compound_blocks(statements: Vec<String>) -> Vec<String> {

    let mut merged = Vec::new();
    let mut idx = 0usize;

    while idx < statements.len() {

        let current = statements[idx].trim().to_string();
        let lowered = current.to_ascii_lowercase();

        let end_marker = if lowered.starts_with("while ")
            || parse_labeled_block_prefix(current.as_str(), "while").is_some()
        {
            Some("end while")
        } else if lowered.starts_with("repeat ")
            || parse_labeled_block_prefix(current.as_str(), "repeat").is_some()
        {
            Some("end repeat")
        } else if is_loop_block_statement(lowered.as_str()) {
            Some("end loop")
        } else if is_begin_block_statement(lowered.as_str()) {
            Some("end")
        } else {
            None
        };

        let Some(marker) = end_marker else {
            merged.push(current);
            idx += 1;
            continue;
        };

        let mut block_sql = current;
        while !block_sql.to_ascii_lowercase().contains(marker) && idx + 1 < statements.len() {
            idx += 1;
            block_sql.push_str("; ");
            block_sql.push_str(statements[idx].trim());
        }

        merged.push(block_sql);
        idx += 1;

    }

    merged

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

const MAX_CALL_LOOP_ITERATIONS: usize = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalHandlerActionKind {
    Continue,
    Exit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LocalHandlerCondition {
    SqlException,
}

#[derive(Clone, Debug)]
struct LocalDeclaredHandler {
    action_kind: LocalHandlerActionKind,
    condition: LocalHandlerCondition,
    action_sql: String,
}

#[derive(Default)]
struct LocalHandlerRuntime {
    handlers: Vec<LocalDeclaredHandler>,
}

impl LocalHandlerRuntime {
    
    fn push(&mut self, handler: LocalDeclaredHandler) {
        self.handlers.push(handler);
    }

    fn truncate(&mut self, len: usize) {
        self.handlers.truncate(len);
    }

    fn resolve_for_error(&self, _message: &str) -> Option<LocalDeclaredHandler> {
        self.handlers
            .iter()
            .rev()
            .find(|handler| matches!(handler.condition, LocalHandlerCondition::SqlException))
            .cloned()
    }

}

#[derive(Clone, Debug)]
enum LocalControlFlowFrame {
    Loop(Option<String>),
    Block(Option<String>),
}

#[derive(Default)]
struct LocalControlFlowRuntime {
    frames: Vec<LocalControlFlowFrame>,
}

impl LocalControlFlowRuntime {

    fn push_loop(&mut self, label: Option<String>) {
        self.frames.push(LocalControlFlowFrame::Loop(label));
    }

    fn push_block(&mut self, label: Option<String>) {
        self.frames.push(LocalControlFlowFrame::Block(label));
    }

    fn pop(&mut self) {
        let _ = self.frames.pop();
    }

    fn has_any_label(&self, target: &str) -> bool {
        self.frames.iter().rev().any(|frame| match frame {
            LocalControlFlowFrame::Loop(label) | LocalControlFlowFrame::Block(label) => label
                .as_deref()
                .map(|active| active.eq_ignore_ascii_case(target))
                .unwrap_or(false),
        })
    }

    fn has_loop_label(&self, target: &str) -> bool {
        self.frames.iter().rev().any(|frame| match frame {
            LocalControlFlowFrame::Loop(label) => label
                .as_deref()
                .map(|active| active.eq_ignore_ascii_case(target))
                .unwrap_or(false),
            LocalControlFlowFrame::Block(_) => false,
        })
    }

}

fn parse_labeled_block_prefix(raw_statement: &str, keyword: &str) -> Option<String> {

    let trimmed = raw_statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if lowered.starts_with(keyword) {
        return None;
    }

    let colon_index = trimmed.find(':')?;
    let label = trimmed[..colon_index].trim();
    if label.is_empty() || !label.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }

    let rest = trimmed[(colon_index + 1)..].trim_start();
    if rest.to_ascii_lowercase().starts_with(keyword) {
        return Some(label.to_string());
    }

    None

}

fn parse_loop_control_target(raw_statement: &str, directive: &str) -> Result<Option<String>, String> {

    let trimmed = raw_statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with(directive) {
        return Ok(None);
    }

    let rest = trimmed[directive.len()..].trim();
    if rest.is_empty() {
        return Ok(None);
    }

    if !rest.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(format!(
            "{} directive parse failed: invalid label '{}'",
            directive.to_ascii_uppercase(),
            rest,
        ));
    }

    Ok(Some(rest.to_string()))

}

fn is_loop_block_statement(lowered_raw: &str) -> bool {

    let trimmed = lowered_raw.trim_start();
    if trimmed.starts_with("loop") {
        return true;
    }

    let Some(colon_index) = trimmed.find(':') else {
        return false;
    };

    let label = trimmed[..colon_index].trim();
    if label.is_empty() || !label.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return false;
    }

    trimmed[(colon_index + 1)..].trim_start().starts_with("loop")

}

fn is_begin_block_statement(lowered_raw: &str) -> bool {

    let trimmed = lowered_raw.trim_start();
    if trimmed.starts_with("begin") {
        return true;
    }

    let Some(colon_index) = trimmed.find(':') else {
        return false;
    };

    let label = trimmed[..colon_index].trim();
    if label.is_empty() || !label.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return false;
    }

    trimmed[(colon_index + 1)..].trim_start().starts_with("begin")

}

fn parse_local_begin_block(action_sql: &str) -> Result<(Option<String>, String), String> {

    let normalized = action_sql.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    let block_label = parse_labeled_block_prefix(normalized, "begin");

    let begin_start = if lowered.starts_with("begin") {
        0
    } else {
        let Some(colon_index) = lowered.find(':') else {
            return Err("begin parse failed: statement must start with BEGIN or <label>: BEGIN".to_string());
        };

        let rest = lowered[(colon_index + 1)..].trim_start();
        if !rest.starts_with("begin") {
            return Err("begin parse failed: statement must start with BEGIN or <label>: BEGIN".to_string());
        }

        lowered.len() - rest.len()
    };

    let end_index = lowered
        .rfind("end")
        .ok_or_else(|| "begin parse failed: END is missing".to_string())?;

    if end_index <= begin_start {
        return Err("begin parse failed: block layout is invalid".to_string());
    }

    let body_sql = normalized[(begin_start + "begin".len())..end_index]
        .trim()
        .to_string();

    Ok((block_label, body_sql))

}

fn execute_call_action_sql(
    action_sql: &str,
    request_id: &str,
    query: &DataQuery,
    ctx: &mut QueryExecutionContext<'_>,
    local_entities: &mut serverlib::ProcedureLocalEntityScope,
    last_response: &mut Option<ConnectorResponse>,
    allow_loop_control: bool,
    handler_runtime: &mut LocalHandlerRuntime,
    loop_runtime: &mut LocalControlFlowRuntime,
) -> Result<serverlib::LoopControlDirective, String> {

    let scope_start = handler_runtime.handlers.len();

    for raw_statement in split_sql_statements_for_call_action(action_sql) {

        let statement_result = execute_call_action_statement(
            raw_statement.as_str(),
            request_id,
            query,
            ctx,
            local_entities,
            last_response,
            allow_loop_control,
            handler_runtime,
            loop_runtime,
        );

        let control = match statement_result {

            Ok(control) => control,
            
            Err(err) => {

                let Some(handler) = handler_runtime.resolve_for_error(err.as_str()) else {
                    handler_runtime.truncate(scope_start);
                    return Err(err);
                };

                let handler_control = execute_call_action_sql(
                    handler.action_sql.as_str(),
                    request_id,
                    query,
                    ctx,
                    local_entities,
                    last_response,
                    allow_loop_control,
                    handler_runtime,
                    loop_runtime,
                )?;

                if !matches!(handler_control, serverlib::LoopControlDirective::None) {
                    handler_runtime.truncate(scope_start);
                    return Ok(handler_control);
                }

                if matches!(handler.action_kind, LocalHandlerActionKind::Exit) {
                    handler_runtime.truncate(scope_start);
                    return Ok(serverlib::LoopControlDirective::None);
                }

                continue;

            }

        };

        if !matches!(control, serverlib::LoopControlDirective::None) {
            handler_runtime.truncate(scope_start);
            return Ok(control);
        }

    }

    handler_runtime.truncate(scope_start);
    
    Ok(serverlib::LoopControlDirective::None)

}

fn execute_call_action_statement(
    raw_statement: &str,
    request_id: &str,
    query: &DataQuery,
    ctx: &mut QueryExecutionContext<'_>,
    local_entities: &mut serverlib::ProcedureLocalEntityScope,
    last_response: &mut Option<ConnectorResponse>,
    allow_loop_control: bool,
    handler_runtime: &mut LocalHandlerRuntime,
    loop_runtime: &mut LocalControlFlowRuntime,
) -> Result<serverlib::LoopControlDirective, String> {

    let lowered_raw = raw_statement.trim().to_ascii_lowercase();

    if lowered_raw.starts_with("declare ") {

        if let Some(handler) = parse_local_handler_declare_statement(raw_statement)? {

            handler_runtime.push(handler);

            *last_response = Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
            ));

            return Ok(serverlib::LoopControlDirective::None);

        }

        apply_local_declare_statement(raw_statement, local_entities)?;
        
        *last_response = Some(ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
        ));
        
        return Ok(serverlib::LoopControlDirective::None);

    }

    if lowered_raw.starts_with("set ") {
        
        apply_local_set_statement(raw_statement, local_entities)?;
        
        *last_response = Some(ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Mutation(MutationResult { affected_rows: 0 }),
        ));
        
        return Ok(serverlib::LoopControlDirective::None);

    }

    if lowered_raw.starts_with("leave") {
        
        let target = parse_loop_control_target(raw_statement, "leave")?;

        if !allow_loop_control && target.is_none() {
            return Err("LEAVE directive is only valid inside a loop block or when a target label is provided".to_string());
        }

        if let Some(target_label) = target.as_deref()
            && !loop_runtime.has_any_label(target_label)
        {
            return Err(format!("LEAVE target label '{}' is not active", target_label));
        }

        return Ok(serverlib::LoopControlDirective::Leave(target));

    }

    if lowered_raw.starts_with("iterate") {

        if !allow_loop_control {
            return Err("ITERATE directive is only valid inside a loop block".to_string());
        }

        let target = parse_loop_control_target(raw_statement, "iterate")?;
        
        if let Some(target_label) = target.as_deref()
            && !loop_runtime.has_loop_label(target_label)
        {
            return Err(format!("ITERATE target label '{}' is not an active loop", target_label));
        }

        return Ok(serverlib::LoopControlDirective::Iterate(target));

    }

    let is_while_block = lowered_raw.starts_with("while ")
        || parse_labeled_block_prefix(raw_statement, "while").is_some();
    let is_repeat_block = lowered_raw.starts_with("repeat ")
        || parse_labeled_block_prefix(raw_statement, "repeat").is_some();

    if is_while_block || is_repeat_block {

        if is_while_block {

            let loop_label = parse_labeled_block_prefix(raw_statement, "while");
            
            loop_runtime.push_loop(loop_label);
            
            let loop_control = serverlib::execute_local_while_block(
                raw_statement,
                MAX_CALL_LOOP_ITERATIONS,
                local_entities,
                &mut |scope, condition_sql| evaluate_local_condition(condition_sql, scope),
                &mut |scope, body_sql| {
                    execute_call_action_sql(
                        body_sql,
                        request_id,
                        query,
                        ctx,
                        scope,
                        last_response,
                        true,
                        handler_runtime,
                        loop_runtime,
                    )
                },
            )?;
            
            loop_runtime.pop();

            if !matches!(loop_control, serverlib::LoopControlDirective::None) {
                return Ok(loop_control);
            }

            return Ok(serverlib::LoopControlDirective::None);
            
        }

        let loop_label = parse_labeled_block_prefix(raw_statement, "repeat");
        
        loop_runtime.push_loop(loop_label);

        let loop_control = serverlib::execute_local_repeat_block(
            raw_statement,
            MAX_CALL_LOOP_ITERATIONS,
            local_entities,
            &mut |scope, condition_sql| evaluate_local_condition(condition_sql, scope),
            &mut |scope, body_sql| {
                execute_call_action_sql(
                    body_sql,
                    request_id,
                    query,
                    ctx,
                    scope,
                    last_response,
                    true,
                    handler_runtime,
                    loop_runtime,
                )
            },
        )?;

        loop_runtime.pop();

        if !matches!(loop_control, serverlib::LoopControlDirective::None) {
            return Ok(loop_control);
        }

        return Ok(serverlib::LoopControlDirective::None);

    }

    if is_loop_block_statement(lowered_raw.as_str()) {

        let loop_label = parse_labeled_block_prefix(raw_statement, "loop");

        loop_runtime.push_loop(loop_label);

        let loop_control = serverlib::execute_local_loop_block(
            raw_statement,
            MAX_CALL_LOOP_ITERATIONS,
            local_entities,
            &mut |scope, body_sql| {
                execute_call_action_sql(
                    body_sql,
                    request_id,
                    query,
                    ctx,
                    scope,
                    last_response,
                    true,
                    handler_runtime,
                    loop_runtime,
                )
            },
        )?;

        loop_runtime.pop();

        if !matches!(loop_control, serverlib::LoopControlDirective::None) {
            return Ok(loop_control);
        }

        return Ok(serverlib::LoopControlDirective::None);

    }

    if is_begin_block_statement(lowered_raw.as_str()) {
        
        let (block_label, body_sql) = parse_local_begin_block(raw_statement)?;
        
        loop_runtime.push_block(block_label.clone());

        let block_control = execute_call_action_sql(
            body_sql.as_str(),
            request_id,
            query,
            ctx,
            local_entities,
            last_response,
            allow_loop_control,
            handler_runtime,
            loop_runtime,
        )?;

        loop_runtime.pop();

        match block_control {
            serverlib::LoopControlDirective::Leave(Some(target))
                if block_label
                    .as_deref()
                    .map(|label| label.eq_ignore_ascii_case(target.as_str()))
                    .unwrap_or(false) =>
            {
                return Ok(serverlib::LoopControlDirective::None);
            }
            other => return Ok(other),
        }
    }

    let parsed_action_sql = serverlib::parse_mysql8_sql_requests(raw_statement, &query.database_id)
        .map_err(|err| format!("call action parse failed: {err}"))?;

    if parsed_action_sql.len() != 1 {
        return Err("call action parse produced unsupported multi-statement execution".to_string());
    }

    let parsed_statement = parsed_action_sql
        .into_iter()
        .next()
        .ok_or_else(|| "call action parse produced no statements".to_string())?;

    if matches!(parsed_statement.operation, SqlOperation::CreateTable) {

        let plan = serverlib::create_table_plan_from_statement(&parsed_statement.sql)
            .map_err(|err| format!("call action create table parse failed: {err}"))?;

        if plan.temporary {

            let Some(catalog) = resolve_catalog_mut(ctx.catalogs, &query.database_id) else {
                return Err(format!("database '{}' not found", query.database_id));
            };

            local_entities.create_temporary_table(catalog, ctx.wal, plan.table_id, plan.schema)?;

            *last_response = Some(ConnectorResponse::applied(
                request_id.to_string(),
                ConnectorResult::Mutation(MutationResult { affected_rows: 1 }),
            ));

            return Ok(serverlib::LoopControlDirective::None);
            
        }

    }

    let rewritten_sql = rewrite_sql_with_call_aliases(&parsed_statement.sql, local_entities)?;
    let rewritten_parsed = serverlib::parse_mysql8_sql_requests(&rewritten_sql, &query.database_id)
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

    *last_response = Some(response);
    
    Ok(serverlib::LoopControlDirective::None)

}


fn evaluate_local_condition(
    condition_sql: &str,
    local_entities: &serverlib::ProcedureLocalEntityScope,
) -> Result<bool, String> {

    let wrapped = format!("select __loop_eval from __loop_eval where {condition_sql}");
    let plan = serverlib::parse_select_read_plan_from_statement(&wrapped)
        .map_err(|err| format!("loop condition parse failed: {err}"))?;

    let Some(condition) = plan.where_condition.as_ref() else {
        return Err("loop condition parse failed: WHERE condition is missing".to_string());
    };

    let provider = local_entities.materialize_value_bindings();

    Ok(serverlib::row_matches_condition_with(
        &provider,
        Some(condition),
        &mut |_, _| std::collections::HashSet::new(),
        &mut |_, _| false,
        &mut |_, _| None,
    ))

}

fn parse_local_handler_declare_statement(sql: &str) -> Result<Option<LocalDeclaredHandler>, String> {

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("declare ") {
        return Ok(None);
    }

    let body = trimmed["declare".len()..].trim();
    let lowered_body = body.to_ascii_lowercase();

    let (action_kind, after_prefix) = if lowered_body.starts_with("continue handler for ") {
        (
            LocalHandlerActionKind::Continue,
            body["continue handler for ".len()..].trim(),
        )
    } else if lowered_body.starts_with("exit handler for ") {
        (
            LocalHandlerActionKind::Exit,
            body["exit handler for ".len()..].trim(),
        )
    } else {
        return Ok(None);
    };

    let mut parts = after_prefix.splitn(2, char::is_whitespace);
    let condition_token = parts.next().unwrap_or("").trim().to_ascii_lowercase();
    let action_sql = parts.next().unwrap_or("").trim().to_string();

    if action_sql.is_empty() {
        return Err("declare handler parse failed: handler action statement is missing".to_string());
    }

    let condition = match condition_token.as_str() {
        "sqlexception" => LocalHandlerCondition::SqlException,
        "sqlwarning" | "not" => {
            return Err(
                "declare handler parse failed: only SQLEXCEPTION handlers are supported".to_string(),
            )
        }
        _ => {
            return Err(format!(
                "declare handler parse failed: unsupported handler condition '{}'",
                condition_token,
            ))
        }
    };

    Ok(Some(LocalDeclaredHandler {
        action_kind,
        condition,
        action_sql,
    }))

}

fn apply_local_declare_statement(
    sql: &str,
    local_entities: &mut serverlib::ProcedureLocalEntityScope,
) -> Result<(), String> {

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("declare ") {
        return Err("local declare parse failed: statement is not DECLARE".to_string());
    }

    let body = trimmed["declare".len()..].trim();
    let tokens = body.split_whitespace().collect::<Vec<_>>();

    let Some(name_token) = tokens.first() else {
        return Err("local declare parse failed: variable name is missing".to_string());
    };

    let variable_name = name_token.trim_matches('`').trim_matches('"');
    if variable_name.is_empty() {
        return Err("local declare parse failed: variable name is empty".to_string());
    }

    let default_index = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("default"));

    let value = if let Some(idx) = default_index {
        let rhs = tokens[(idx + 1)..].join(" ");
        parse_local_scalar_value(&rhs, local_entities)?
    } else {
        Vec::new()
    };

    local_entities.set_variable(variable_name, value);
    Ok(())

}

fn apply_local_set_statement(
    sql: &str,
    local_entities: &mut serverlib::ProcedureLocalEntityScope,
) -> Result<(), String> {

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("set ") {
        return Err("local set parse failed: statement is not SET".to_string());
    }

    let body = trimmed["set".len()..].trim();
    let Some(eq_index) = body.find('=') else {
        return Err("local set parse failed: '=' is missing".to_string());
    };

    let variable_name = body[..eq_index].trim().trim_matches('`').trim_matches('"');
    if variable_name.is_empty() {
        return Err("local set parse failed: variable name is empty".to_string());
    }

    let rhs = body[(eq_index + 1)..].trim();
    let value = parse_local_scalar_value(rhs, local_entities)?;
    local_entities.set_variable(variable_name, value);

    Ok(())

}

fn parse_local_scalar_value(
    rhs: &str,
    local_entities: &serverlib::ProcedureLocalEntityScope,
) -> Result<Vec<u8>, String> {

    let value = rhs.trim();
    if value.is_empty() {
        return Err("local assignment parse failed: value is empty".to_string());
    }

    if (value.starts_with('\'') && value.ends_with('\''))
        || (value.starts_with('"') && value.ends_with('"'))
    {
        if value.len() < 2 {
            return Err("local assignment parse failed: quoted value is malformed".to_string());
        }
        return Ok(value[1..(value.len() - 1)].as_bytes().to_vec());
    }

    let lowered = value.to_ascii_lowercase();
    if lowered == "true" || lowered == "false" {
        return Ok(lowered.into_bytes());
    }

    if value.chars().all(|ch| ch.is_ascii_digit() || ch == '-' || ch == '+')
        && value.chars().any(|ch| ch.is_ascii_digit())
    {
        return Ok(value.as_bytes().to_vec());
    }

    let ident = common::normalize_identifier!(value.trim_matches('`').trim_matches('"'));
    if let Some(local_value) = local_entities.resolve_value(&ident) {
        return Ok(local_value.clone());
    }

    Err(format!(
        "local assignment parse failed: unsupported value expression '{}'",
        value
    ))
    
}

