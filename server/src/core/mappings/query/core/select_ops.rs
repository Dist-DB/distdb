use super::*;
use super::variables::{
    effective_recursive_cte_execution_settings,
    normalize_variable_name,
    readable_variable_rows,
    SessionVariableOverrides,
};

pub(super) fn execute_select_plan_result(
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
            &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
                serverlib::execute_sql_function_with_lookup(
                    catalog,
                    wal,
                    runtime_indexes,
                    function,
                    lookup,
                )
            }),
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
        return serverlib::execute_projection_only_select_plan(read_plan, &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
            serverlib::execute_sql_function_with_lookup(
                catalog,
                wal,
                runtime_indexes,
                function,
                lookup,
            )
        }));
    }

    let table_id = read_plan.table_id.as_str();

    let schema = catalog
        .table_schema(table_id)
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;

    let table = catalog
        .table(table_id)
        .ok_or_else(|| format!("select failed: table '{}' not found", table_id))?;

    let mut scoped_table = table.clone();
    if let Some(stream_id) = catalog.entity_wal_stream_id(table_id) {
        scoped_table.entity_id = stream_id;
    }

    let mut index_filter_map = HashMap::new();
    let like_filter = read_plan
        .where_condition
        .as_ref()
        .and_then(|condition| {
            collect_indexable_like_filter_for_schema(schema, condition)
        });
        
    let allow_index_short_circuit = read_plan
        .where_condition
        .as_ref()
        .map(|condition| {
            collect_indexable_equality_filters_for_schema(
                schema,
                condition,
                &mut index_filter_map,
            )
        })
        .unwrap_or(true);

    let access_plan = plan_relation_access(
        &scoped_table,
        allow_index_short_circuit,
        index_filter_map,
        like_filter,
    );

    serverlib::execute_relation_select_plan(
        wal,
        &scoped_table,
        schema,
        runtime_indexes,
        read_plan,
        &access_plan,
        &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
            serverlib::execute_sql_function_with_lookup(
                catalog,
                wal,
                runtime_indexes,
                function,
                lookup,
            )
        }),
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

pub(super) fn execute_select_impl(
    request_id: &str,
    database_id: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    statement: &SqlRequest,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> ConnectorResponse {

    let statement_sql_lower = statement.sql.to_ascii_lowercase();

    if let Some(response) = handle_select_introspection_request(
        request_id,
        database_id,
        catalogs,
        wal,
        statement,
        &statement_sql_lower,
        session_variable_overrides,
    ) {
        return response;
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

    let resolved_object_name = statement
        .object_name
        .as_deref()
        .unwrap_or(&read_plan.table_id)
        .to_string();

    let (catalog, read_plan) = match resolve_catalog_and_read_plan_for_select(
        catalogs,
        database_id,
        &resolved_object_name,
        read_plan,
    ) {
        
        Ok(resolved) => resolved,
        
        Err(message) => {
            return ConnectorResponse::rejected(request_id.to_string(), message);
        }

    };

    execute_select_read_plan(
        request_id,
        database_id,
        catalog,
        wal,
        runtime_indexes,
        &read_plan,
        session_variable_overrides,
    )

}

fn handle_select_introspection_request(
    request_id: &str,
    database_id: &str,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    statement: &SqlRequest,
    statement_sql_lower: &str,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> Option<ConnectorResponse> {

    let statement_sql_lower = statement_sql_lower.trim().trim_end_matches(';').trim();

    if statement_sql_lower == "show user" || statement_sql_lower == "show user()" {

        let current_user = serverlib::inbuilt_sql_runtime_context()
            .current_user
            .or_else(|| {
                std::env::var("USER")
                    .ok()
                    .map(|user| format!("{}@localhost", user))
            })
            .unwrap_or_else(|| "distdb@localhost".to_string());

        let result = serverlib::SelectExecutionResult {
            columns: vec![serverlib::FieldDef {
                seqno: 1,
                field_name: "user".to_string(),
                field_type: serverlib::FieldType::Text,
                nullable: false,
                indexed: serverlib::FieldIndex::None,
                default_value: None,
                metadata: None,
            }],
            rows: vec![vec![current_user.into_bytes()]],
        };

        return Some(applied_query_response(request_id, result));

    }

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

        return Some(applied_query_response(request_id, result));

    }

    if statement_sql_lower.starts_with("show privileges") ||
        statement_sql_lower.starts_with("show priviledges")
    {

        let target_db = statement
            .object_name
            .as_deref()
            .unwrap_or(database_id);

        let Some(catalog) = resolve_catalog(catalogs, target_db) else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", target_db),
            ));
        };

        let result = serverlib::show_privileges_result(
            catalog.effective_account_acl_entries().map(|entry| {
                let mut privileges = entry.acl.iter().cloned().collect::<Vec<_>>();
                privileges.sort();

                let mut grantable_privileges = entry.grant_acl.iter().cloned().collect::<Vec<_>>();
                grantable_privileges.sort();

                (entry.user_id.0.clone(), privileges, grantable_privileges)
            }),
        );

        return Some(applied_query_response(request_id, result));

    }

    if statement_sql_lower.starts_with("show tables") ||
        parse_show_tables_target_database(&statement.sql).is_some()
    {

        let show_tables_target_db = parse_show_tables_target_database(&statement.sql);

        let target_db = show_tables_target_db
            .as_deref()
            .or_else(|| statement.object_name.as_deref())
            .unwrap_or(database_id);

        let Some(catalog) = resolve_catalog(catalogs, target_db) else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", target_db),
            ));
        };

        let result = serverlib::show_tables_result(catalog.table_ids().into_iter().map(|table_id| {
            let store_kind = catalog
                .table(&table_id)
                .map(|table| if table.is_temporary() { "memory" } else { "permanent" })
                .unwrap_or("permanent")
                .to_string();

            (table_id, store_kind)
        }));

        return Some(applied_query_response(request_id, result));

    }

    if statement_sql_lower.starts_with("show variables") ||
        statement_sql_lower.starts_with("show variable")
    {

        let is_show_variables = statement_sql_lower.starts_with("show variables");
        let is_show_variable = !is_show_variables && statement_sql_lower.starts_with("show variable");

        let target_db = if is_show_variable {
            database_id
        } else {
            statement
                .object_name
                .as_deref()
                .unwrap_or(database_id)
        };

        let Some(catalog) = resolve_catalog(catalogs, target_db) else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", target_db),
            ));
        };

        let session_user = serverlib::inbuilt_sql_runtime_context().session_user;
        let available = recursive_cte_show_variables(
            catalog,
            session_variable_overrides,
            session_user.as_deref(),
        );

        let variable_filter = match parse_show_variable_filter(statement, is_show_variable) {
            Ok(filter) => filter,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        let variable_rows = match filter_named_value_rows(available, &variable_filter) {
            Ok(rows) => rows,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        let result = serverlib::show_variables_result(variable_rows);
        return Some(applied_query_response(request_id, result));

    }

    if statement_sql_lower.starts_with("show index") ||
        statement_sql_lower.starts_with("show indexes") ||
        statement_sql_lower.starts_with("show keys")
    {

        let object_name = statement
            .object_name
            .clone()
            .or_else(|| parse_show_indexes_target_table(&statement.sql));

        let Some(object_name) = object_name else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                "show indexes missing table identifier",
            ));
        };

        let (catalog, normalized_table_id) = if database_id.trim().is_empty() {

            if let Some((database_name, object_id)) = object_name.rsplit_once('.') {

                let Some(catalog) = resolve_catalog(catalogs, database_name) else {
                    return Some(ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("database '{}' not found", database_name),
                    ));
                };

                (catalog, common::normalize_identifier!(object_id))

            } else {

                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", object_name),
                ));

            }

        } else if let Some((database_name, object_id)) = object_name.rsplit_once('.') {

            let Some(catalog) = resolve_catalog(catalogs, database_name) else {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", database_name),
                ));
            };

            (catalog, common::normalize_identifier!(object_id))

        } else {

            let Some(catalog) = resolve_catalog(catalogs, database_id) else {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", database_id),
                ));
            };

            (catalog, common::normalize_identifier!(object_name))

        };

        let Some(table) = catalog.table(&normalized_table_id) else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "table '{}' not found in database '{}'",
                    normalized_table_id,
                    catalog.database_id.0
                ),
            ));
        };

        let result = serverlib::show_indexes_result(table.indexes.values().map(|index| {
            (
                normalized_table_id.clone(),
                index.index_id.0.clone(),
                format!("{:?}", index.kind).to_ascii_lowercase(),
                format!("{:?}", index.origin).to_ascii_lowercase(),
                index.field_names.join(","),
            )
        }));

        return Some(applied_query_response(request_id, result));

    }

    if statement_sql_lower.starts_with("show slices") {

        let options = match parse_show_slices_options(&statement.sql) {
            Ok(options) => options,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        let object_name = statement
            .object_name
            .clone()
            .or_else(|| parse_show_slices_target_view(&statement.sql));

        let Some(object_name) = object_name else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                "show slices missing olap view identifier",
            ));
        };

        let (catalog, normalized_object_id, resolved_database_id) =
            match resolve_catalog_and_object_for_lookup(catalogs, database_id, &object_name)
            {
                Ok(resolved) => resolved,
                Err(message) => {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), message));
                }
            };

        let mut result = match build_show_slices_result(
            catalog,
            wal,
            &normalized_object_id,
            &resolved_database_id,
        ) {
            Ok(result) => result,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        if let Err(message) = apply_show_slices_options(&mut result, options) {
            return Some(ConnectorResponse::rejected(request_id.to_string(), message));
        }

        return Some(applied_query_response(request_id, result));
        
    }

    if statement_sql_lower.starts_with("debug ") {

        let (entity_type, entity_name) = match parse_debug_entity_request(&statement.sql) {
            Ok(parsed) => parsed,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        let (catalog, normalized_object_id, resolved_database_id) =
            match resolve_catalog_and_object_for_lookup(catalogs, database_id, &entity_name)
            {
                Ok(resolved) => resolved,
                Err(message) => {
                    return Some(ConnectorResponse::rejected(request_id.to_string(), message));
                }
            };

        let rows = match build_debug_rows(
            catalog,
            &entity_type,
            &normalized_object_id,
            &resolved_database_id,
        ) {
            Ok(rows) => rows,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        let result = debug_attribute_result(rows);

        return Some(applied_query_response(request_id, result));

    }

    if statement_sql_lower.starts_with("describe ") ||
        statement_sql_lower.starts_with("desc ") ||
        statement_sql_lower.starts_with("show columns")
    {

        let Some(object_name) = statement.object_name.as_deref() else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                "describe/show columns missing table identifier",
            ));
        };

        let (catalog, normalized_object_id) = if database_id.trim().is_empty() {

            if let Some((database_name, object_id)) = object_name.rsplit_once('.') {

                let Some(catalog) = resolve_catalog(catalogs, database_name) else {
                    return Some(ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("database '{}' not found", database_name),
                    ));
                };

                (catalog, common::normalize_identifier!(object_id))

            } else {

                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!(
                        "database '{}' not found",
                        if database_id.is_empty() {
                            object_name
                        } else {
                            database_id
                        }
                    ),
                ));

            }

        } else if let Some((database_name, object_id)) = object_name.rsplit_once('.') {

            let Some(catalog) = resolve_catalog(catalogs, database_name) else {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", database_name),
                ));
            };

            (catalog, common::normalize_identifier!(object_id))

        } else {

            let Some(catalog) = resolve_catalog(catalogs, database_id) else {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", database_id),
                ));
            };

            (catalog, common::normalize_identifier!(object_name))

        };

        let result = if let Some(schema) = catalog.table_schema(&normalized_object_id) {

            serverlib::describe_table_result(schema)

        } else if let Some(view) = catalog.view(&normalized_object_id) {

            serverlib::describe_sql_object_result("view", &view.view_id, &view.sql)

        } else if let Some(trigger) = catalog.trigger(&normalized_object_id) {

            serverlib::describe_sql_object_result("trigger", &trigger.trigger_id, &trigger.sql)

        } else if let Some(procedure) = catalog.stored_procedure(&normalized_object_id) {

            serverlib::describe_sql_object_result(
                "stored_procedure",
                &procedure.procedure_id,
                &procedure.sql,
            )

        } else {

            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                format!(
                    "object '{}' not found in database '{}'",
                    normalized_object_id,
                    if database_id.trim().is_empty() {
                        catalog.database_id.0.as_str()
                    } else {
                        database_id
                    }
                ),
            ));

        };

        return Some(applied_query_response(request_id, result));

    }

    None

}

fn parse_show_indexes_target_table(sql: &str) -> Option<String> {

    let tokens = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>();

    let target_idx = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("from") || token.eq_ignore_ascii_case("in"))?
        + 1;

    let raw = tokens.get(target_idx)?;
    let normalized = raw
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches(';')
        .to_string();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }

}

fn parse_show_tables_target_database(sql: &str) -> Option<String> {

    let trimmed = sql.trim().trim_end_matches(';').trim();
    let tokens = trimmed.split_whitespace().collect::<Vec<_>>();

    if tokens.len() != 2 || !tokens[0].eq_ignore_ascii_case("show") {
        return None;
    }

    let raw_target = tokens[1].trim();
    let lowered = raw_target.to_ascii_lowercase();

    if !lowered.ends_with(".tables") {
        return None;
    }

    let raw_database = &raw_target[..raw_target.len().saturating_sub(".tables".len())];
    let normalized = common::normalize_identifier!(
        raw_database.trim().trim_matches('`').trim_matches('"')
    );

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }

}

fn recursive_cte_show_variables(
    catalog: &DatabaseCatalog,
    session_variable_overrides: Option<&SessionVariableOverrides>,
    session_user: Option<&str>,
) -> Vec<(String, String)> {
    readable_variable_rows(catalog, session_variable_overrides, session_user)
}

#[derive(Debug, Clone)]
enum NamedValueFilter {
    All,
    ExactName(String),
    LikePattern {
        pattern: String,
        escape_char: Option<char>,
    },
}

fn filter_named_value_rows(
    rows: Vec<(String, String)>,
    filter: &NamedValueFilter,
) -> Result<Vec<(String, String)>, String> {

    match filter {

        NamedValueFilter::All => Ok(rows),

        NamedValueFilter::ExactName(name) => Ok(rows
            .into_iter()
            .filter(|(row_name, _)| row_name == name)
            .collect()),

        NamedValueFilter::LikePattern {
            pattern,
            escape_char,
        } => {
            if pattern.is_empty() {
                return Ok(Vec::new());
            }

            Ok(rows
                .into_iter()
                .filter(|(row_name, _)| {
                    serverlib::engine::sql::compare_like_value(
                        row_name.as_bytes(),
                        pattern.as_bytes(),
                        true,
                        *escape_char,
                    )
                })
                .collect())
        }

    }

}

fn parse_show_variable_filter(
    statement: &SqlRequest,
    is_show_variable: bool,
) -> Result<NamedValueFilter, String> {

    if is_show_variable {
        return parse_show_variable_name(statement)
            .filter(|name| !name.is_empty())
            .map(NamedValueFilter::ExactName)
            .ok_or_else(|| "show variable missing variable identifier".to_string());
    }

    parse_show_variables_like_pattern(statement)

}

fn parse_show_variables_like_pattern(statement: &SqlRequest) -> Result<NamedValueFilter, String> {

    let tokens = statement
        .sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>();

    let Some(like_idx) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("like")) else {
        return Ok(NamedValueFilter::All);
    };

    let Some(raw_pattern) = tokens.get(like_idx + 1) else {
        return Err("show variables LIKE requires a pattern".to_string());
    };

    let pattern = strip_sql_token_quotes(raw_pattern).to_string();
    if pattern.is_empty() {
        return Err("show variables LIKE requires a pattern".to_string());
    }

    let escape_char = if tokens
        .get(like_idx + 2)
        .is_some_and(|token| token.eq_ignore_ascii_case("escape"))
    {
        let Some(raw_escape) = tokens.get(like_idx + 3) else {
            return Err("show variables LIKE ESCAPE requires a single character".to_string());
        };

        let escape = strip_sql_token_quotes(raw_escape);
        let mut chars = escape.chars();
        let Some(first) = chars.next() else {
            return Err("show variables LIKE ESCAPE requires a single character".to_string());
        };
        if chars.next().is_some() {
            return Err("show variables LIKE ESCAPE requires a single character".to_string());
        }

        Some(first)
    } else {
        None
    };

    Ok(NamedValueFilter::LikePattern {
        pattern,
        escape_char,
    })

}

fn strip_sql_token_quotes(token: &str) -> &str {
    token
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches('\'')
}

fn parse_show_variable_name(statement: &SqlRequest) -> Option<String> {

    let trimmed = statement.sql.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if lowered.starts_with("show variable") {
        let suffix = trimmed["show variable".len()..].trim();
        if !suffix.is_empty() {
            return Some(normalize_variable_name(
                suffix.split_whitespace().next().unwrap_or(suffix),
            ));
        }
    }

    statement
        .object_name
        .as_deref()
        .map(normalize_variable_name)
        .filter(|name| !name.is_empty())

}

fn parse_show_slices_target_view(sql: &str) -> Option<String> {

    let tokens = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>();

    let target_idx = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("from"))?
        + 1;

    let raw = tokens.get(target_idx)?;
    let normalized = raw
        .trim()
        .trim_matches('`')
        .trim_matches('"')
        .trim_matches(';')
        .to_string();

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }

}

#[derive(Debug, Clone, Default)]
struct ShowSlicesOptions {
    filters: Vec<ShowSlicesFilter>,
    order_by: Option<(String, bool)>,
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct ShowSlicesFilter {
    column: String,
    operator: ShowSlicesFilterOperator,
    value: String,
}

#[derive(Debug, Clone, Copy)]
enum ShowSlicesFilterOperator {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

fn parse_show_slices_options(sql: &str) -> Result<ShowSlicesOptions, String> {

    let tokens = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>();

    let mut options = ShowSlicesOptions::default();

    let Some(from_idx) = tokens
        .iter()
        .position(|token| token.eq_ignore_ascii_case("from")) else {
            return Ok(options);
        };

    let mut idx = from_idx.saturating_add(2);

    while idx < tokens.len() {

        let token = tokens[idx];

        if token.eq_ignore_ascii_case("where") {

            idx += 1;

            if idx >= tokens.len() {
                return Err("show slices WHERE clause is malformed".to_string());
            }

            while idx < tokens.len() {

                if tokens[idx].eq_ignore_ascii_case("order")
                    || tokens[idx].eq_ignore_ascii_case("limit")
                {
                    break;
                }

                if idx + 2 >= tokens.len() {
                    return Err("show slices WHERE clause is malformed".to_string());
                }

                let column = tokens[idx]
                    .trim()
                    .trim_matches('`')
                    .trim_matches('"')
                    .trim_matches(',')
                    .trim_matches(';')
                    .to_string();

                let operator = parse_show_slices_filter_operator(tokens[idx + 1])?;

                let value = tokens[idx + 2]
                    .trim()
                    .trim_matches('"')
                    .trim_matches('`')
                    .trim_matches(',')
                    .trim_matches(';')
                    .trim_matches('\'')
                    .to_string();

                if column.is_empty() {
                    return Err("show slices WHERE clause is malformed".to_string());
                }

                options.filters.push(ShowSlicesFilter {
                    column: common::normalize_identifier!(&column),
                    operator,
                    value,
                });

                idx += 3;

                if idx < tokens.len() && tokens[idx].eq_ignore_ascii_case("and") {
                    idx += 1;
                }

            }

            continue;

        }

        if token.eq_ignore_ascii_case("order") {

            if idx + 2 >= tokens.len() || !tokens[idx + 1].eq_ignore_ascii_case("by") {
                return Err("show slices ORDER BY clause is malformed".to_string());
            }

            let order_column = tokens[idx + 2]
                .trim()
                .trim_matches('`')
                .trim_matches('"')
                .trim_matches(',')
                .trim_matches(';')
                .to_string();

            if order_column.is_empty() {
                return Err("show slices ORDER BY requires a column name".to_string());
            }

            let mut descending = false;
            let mut consumed = 3usize;

            if idx + 3 < tokens.len() {
                let direction = tokens[idx + 3];
                if direction.eq_ignore_ascii_case("desc") {
                    descending = true;
                    consumed = 4;
                } else if direction.eq_ignore_ascii_case("asc") {
                    consumed = 4;
                }
            }

            options.order_by = Some((common::normalize_identifier!(&order_column), descending));
            idx += consumed;
            continue;

        }

        if token.eq_ignore_ascii_case("limit") {

            let Some(raw_limit) = tokens.get(idx + 1) else {
                return Err("show slices LIMIT requires a numeric value".to_string());
            };

            let parsed_limit = raw_limit
                .trim_matches(',')
                .trim_matches(';')
                .parse::<usize>()
                .map_err(|_| "show slices LIMIT must be an unsigned integer".to_string())?;

            options.limit = Some(parsed_limit);
            idx += 2;
            continue;

        }

        idx += 1;

    }

    Ok(options)

}

fn apply_show_slices_options(
    result: &mut serverlib::SelectExecutionResult,
    options: ShowSlicesOptions,
) -> Result<(), String> {

    if !options.filters.is_empty() {

        let column_lookup = result
            .columns
            .iter()
            .enumerate()
            .map(|(idx, column)| (common::normalize_identifier!(&column.field_name), idx))
            .collect::<std::collections::HashMap<_, _>>();

        let mut resolved_filters = Vec::with_capacity(options.filters.len());

        for filter in options.filters {
            let Some(column_index) = column_lookup.get(&filter.column).copied() else {
                return Err(format!(
                    "show slices WHERE field '{}' was not found in result columns",
                    filter.column,
                ));
            };

            resolved_filters.push((column_index, filter));
        }

        result.rows.retain(|row| {
            resolved_filters.iter().all(|(column_index, filter)| {
                let row_value = row
                    .get(*column_index)
                    .map(|cell| String::from_utf8_lossy(cell).to_string())
                    .unwrap_or_default();

                show_slices_filter_matches(&row_value, filter)
            })
        });

    }

    if let Some((order_column, descending)) = options.order_by {

        let Some(order_index) = result
            .columns
            .iter()
            .position(|column| common::normalize_identifier!(&column.field_name) == order_column)
        else {
            return Err(format!(
                "show slices ORDER BY field '{}' was not found in result columns",
                order_column,
            ));
        };

        result.rows.sort_by(|left, right| {
            
            let left_value = left.get(order_index).map(|cell| String::from_utf8_lossy(cell).to_string()).unwrap_or_default();
            let right_value = right.get(order_index).map(|cell| String::from_utf8_lossy(cell).to_string()).unwrap_or_default();

            let ordering = compare_slice_order_values(&left_value, &right_value);

            if descending {
                ordering.reverse()
            } else {
                ordering
            }
        
        });

    }

    if let Some(limit) = options.limit {
        result.rows.truncate(limit);
    }

    Ok(())

}

fn parse_show_slices_filter_operator(token: &str) -> Result<ShowSlicesFilterOperator, String> {

    match token {

        "="     => Ok(ShowSlicesFilterOperator::Eq),
        
        "!=" | 
        "<>"    => Ok(ShowSlicesFilterOperator::NotEq),
        
        ">"     => Ok(ShowSlicesFilterOperator::Gt),
        
        ">="    => Ok(ShowSlicesFilterOperator::Gte),
        
        "<"     => Ok(ShowSlicesFilterOperator::Lt),
        
        "<="    => Ok(ShowSlicesFilterOperator::Lte),

        _       => Err("show slices WHERE clause uses an unsupported operator".to_string()),
    
    }

}

fn show_slices_filter_matches(row_value: &str, filter: &ShowSlicesFilter) -> bool {

    let row_is_null = row_value.eq_ignore_ascii_case("NULL");
    let filter_is_null = filter.value.eq_ignore_ascii_case("NULL");

    if row_is_null || filter_is_null {
        return match filter.operator {
            ShowSlicesFilterOperator::Eq => row_is_null == filter_is_null,
            ShowSlicesFilterOperator::NotEq => row_is_null != filter_is_null,
            _ => false,
        };
    }

    if let (Ok(left_num), Ok(right_num)) = (row_value.parse::<f64>(), filter.value.parse::<f64>()) {
        return match filter.operator {
            ShowSlicesFilterOperator::Eq    => left_num == right_num,
            ShowSlicesFilterOperator::NotEq => left_num != right_num,
            ShowSlicesFilterOperator::Gt    => left_num > right_num,
            ShowSlicesFilterOperator::Gte   => left_num >= right_num,
            ShowSlicesFilterOperator::Lt    => left_num < right_num,
            ShowSlicesFilterOperator::Lte   => left_num <= right_num,
        };
    }

    let left = row_value.to_ascii_lowercase();
    let right = filter.value.to_ascii_lowercase();

    match filter.operator {
        ShowSlicesFilterOperator::Eq    => left == right,
        ShowSlicesFilterOperator::NotEq => left != right,
        ShowSlicesFilterOperator::Gt    => left > right,
        ShowSlicesFilterOperator::Gte   => left >= right,
        ShowSlicesFilterOperator::Lt    => left < right,
        ShowSlicesFilterOperator::Lte   => left <= right,
    }

}

fn compare_slice_order_values(left: &str, right: &str) -> std::cmp::Ordering {

    let left_is_null = left.eq_ignore_ascii_case("NULL");
    let right_is_null = right.eq_ignore_ascii_case("NULL");

    if left_is_null && right_is_null {
        return std::cmp::Ordering::Equal;
    }

    if left_is_null {
        return std::cmp::Ordering::Less;
    }

    if right_is_null {
        return std::cmp::Ordering::Greater;
    }

    match (left.parse::<f64>(), right.parse::<f64>()) {
        (Ok(left_num), Ok(right_num)) => left_num
            .partial_cmp(&right_num)
            .unwrap_or(std::cmp::Ordering::Equal),
        _ => left.cmp(right),
    }

}

fn build_show_slices_result(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    normalized_view_id: &str,
    resolved_database_id: &str,
) -> Result<serverlib::SelectExecutionResult, String> {

    let Some(olap_view) = catalog.olap_view(normalized_view_id) else {
        return Err(format!(
            "olap view '{}' not found in database '{}'",
            normalized_view_id,
            resolved_database_id,
        ));
    };

    if olap_view.z_dimension_columns.is_empty() {
        return Err(format!(
            "olap view '{}' has no dimensions configured",
            normalized_view_id,
        ));
    }

    let Some(source_dependency) = olap_view.dependencies.first() else {
        return Err(format!(
            "olap view '{}' has no source table dependency",
            normalized_view_id,
        ));
    };

    let source_table_id = strip_matching_database_prefix(source_dependency, resolved_database_id);

    let Some(schema) = catalog.table_schema(&source_table_id) else {
        return Err(format!(
            "show slices failed: source table '{}' not found for olap view '{}'",
            source_table_id,
            normalized_view_id,
        ));
    };

    let stream_id = catalog
        .entity_wal_stream_id(&source_table_id)
        .unwrap_or_else(|| source_table_id.clone());

    let payload_context = payload_context_for_table(catalog, &source_table_id);

    let live_rows = load_live_rows_with_context(wal, &stream_id, schema, &payload_context)
        .map_err(|err| format!("show slices failed: cannot load source rows: {err}"))?;

    let dimension_set = olap_view
        .z_dimension_columns
        .iter()
        .cloned()
        .collect::<std::collections::HashSet<_>>();

    let projected_fields = extract_projected_field_names_from_olap_view_sql(&olap_view.sql)
        .unwrap_or_default();

    let projected_field_set = if projected_fields.is_empty() {
        None
    } else {
        Some(
            projected_fields
                .into_iter()
                .map(|field| common::normalize_identifier!(&field))
                .collect::<std::collections::HashSet<_>>(),
        )
    };

    let measure_fields = schema
        .fields
        .iter()
        .filter(|field| {
            !dimension_set.contains(&field.field_name)
                && !matches!(field.indexed, serverlib::FieldIndex::PrimaryKey)
                && projected_field_set
                    .as_ref()
                    .map(|projection| projection.contains(&field.field_name))
                    .unwrap_or(true)
                && matches!(
                    field.field_type,
                    serverlib::FieldType::Int(_) |
                    serverlib::FieldType::UInt(_) |
                    serverlib::FieldType::Float(_)
                )
        })
        .map(|field| field.field_name.clone())
        .collect::<Vec<_>>();

    let mut grouped_counts = HashMap::<Vec<String>, SliceAggregate>::new();

    for (_row_id, row_map) in live_rows {
        
        let key = olap_view
            .z_dimension_columns
            .iter()
            .map(|dimension| {

                row_map
                    .get(dimension)
                    .map(|value| {
                        let text = serverlib::display_stored_field_value(value)
                            .trim()
                            .to_string();
                        if text.is_empty() {
                            "NULL".to_string()
                        } else {
                            text
                        }
                    })
                    .unwrap_or_else(|| "NULL".to_string())
                    
            })
            .collect::<Vec<_>>();

        let aggregate = grouped_counts
            .entry(key)
            .or_insert_with(|| SliceAggregate::new(measure_fields.len()));

        aggregate.row_count += 1;

        for (idx, measure_field) in measure_fields.iter().enumerate() {
            if let Some(raw_value) = row_map.get(measure_field)
                && let Some(value) = parse_numeric_slice_value(raw_value) {
                    aggregate.measure_stats[idx].ingest(value);
                }
        }

    }

    let mut grouped_rows = grouped_counts.into_iter().collect::<Vec<_>>();
    grouped_rows.sort_by(|left, right| compare_slice_coordinates(&left.0, &right.0));

    let mut columns = olap_view
        .z_dimension_columns
        .iter()
        .enumerate()
        .map(|(idx, dimension)| serverlib::FieldDef {
            seqno: (idx + 1) as u32,
            field_name: dimension.clone(),
            field_type: serverlib::FieldType::Text,
            nullable: false,
            indexed: serverlib::FieldIndex::None,
            default_value: None,
            metadata: None,
        })
        .collect::<Vec<_>>();

    columns.push(serverlib::FieldDef {
        seqno: (columns.len() + 1) as u32,
        field_name: "row_count".to_string(),
        field_type: serverlib::FieldType::UInt(64),
        nullable: false,
        indexed: serverlib::FieldIndex::None,
        default_value: None,
        metadata: None,
    });

    for measure_field in &measure_fields {
        for suffix in ["sum", "min", "max", "avg"] {
            columns.push(serverlib::FieldDef {
                seqno: (columns.len() + 1) as u32,
                field_name: format!("{}_{}", suffix, measure_field),
                field_type: serverlib::FieldType::Float(64),
                nullable: false,
                indexed: serverlib::FieldIndex::None,
                default_value: None,
                metadata: None,
            });
        }
    }

    let rows = grouped_rows
        .into_iter()
        .map(|(coordinates, aggregate)| {
            let mut row = coordinates
                .into_iter()
                .map(|coordinate| coordinate.into_bytes())
                .collect::<Vec<_>>();

            row.push(aggregate.row_count.to_string().into_bytes());

            for stats in aggregate.measure_stats {
                row.push(render_optional_numeric_slice_value(stats.sum).into_bytes());
                row.push(render_optional_numeric_slice_value(stats.min).into_bytes());
                row.push(render_optional_numeric_slice_value(stats.max).into_bytes());
                row.push(render_optional_numeric_slice_value(stats.avg()).into_bytes());
            }

            row
        })
        .collect::<Vec<_>>();

    Ok(serverlib::SelectExecutionResult { columns, rows })

}

#[derive(Debug, Clone)]
struct SliceAggregate {
    row_count: usize,
    measure_stats: Vec<MeasureStats>,
}

impl SliceAggregate {
    fn new(measure_count: usize) -> Self {
        Self {
            row_count: 0,
            measure_stats: vec![MeasureStats::default(); measure_count],
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MeasureStats {
    count: usize,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
}

impl MeasureStats {

    fn ingest(&mut self, value: f64) {
        self.count += 1;

        self.sum = Some(self.sum.unwrap_or(0.0) + value);
        self.min = Some(self.min.map_or(value, |current| current.min(value)));
        self.max = Some(self.max.map_or(value, |current| current.max(value)));
    }

    fn avg(&self) -> Option<f64> {
        if self.count == 0 {
            None
        } else {
            self.sum.map(|sum| sum / self.count as f64)
        }
    }

}

fn parse_numeric_slice_value(raw: &[u8]) -> Option<f64> {

    let rendered = serverlib::display_stored_field_value(raw);
    let text = rendered.trim();
    
    if text.is_empty() || text.eq_ignore_ascii_case("null") {
        return None;
    }
    
    text.parse::<f64>().ok()

}

fn render_optional_numeric_slice_value(value: Option<f64>) -> String {

    let Some(value) = value else {
        return "NULL".to_string();
    };

    if value.fract() == 0.0 {
        (value as i128).to_string()
    } else {
        value.to_string()
    }

}

fn compare_slice_coordinates(left: &[String], right: &[String]) -> std::cmp::Ordering {

    for (left_value, right_value) in left.iter().zip(right.iter()) {

        let left_is_null = left_value.eq_ignore_ascii_case("NULL");
        let right_is_null = right_value.eq_ignore_ascii_case("NULL");

        let ordering = match (left_is_null, right_is_null) {
            (true, true) => std::cmp::Ordering::Equal,
            // Keep NULLs sorted first for stable, explicit introspection output.
            (true, false) => std::cmp::Ordering::Less,
            (false, true) => std::cmp::Ordering::Greater,
            (false, false) => left_value.cmp(right_value),
        };

        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }

    }

    left.len().cmp(&right.len())

}

fn extract_projected_field_names_from_olap_view_sql(olap_sql: &str) -> Option<Vec<String>> {
    let select_sql = extract_view_select_sql(olap_sql).ok()?;
    let projection = serverlib::parse_select_projection_from_statement(&select_sql).ok()?;
    projection
}

fn resolve_catalog_and_read_plan_for_select<'a>(
    catalogs: &'a mut HashMap<String, DatabaseCatalog>,
    requested_database_id: &str,
    resolved_object_name: &str,
    read_plan: serverlib::SelectReadPlan,
) -> Result<(&'a mut DatabaseCatalog, serverlib::SelectReadPlan), String> {

    if requested_database_id.trim().is_empty() {

        let Some((catalog, table_id)) = resolve_catalog_for_table_reference_mut(
            catalogs,
            requested_database_id,
            resolved_object_name,
        ) else {

            let database_name = resolved_object_name
                .rsplit_once('.')
                .map(|(database_name, _)| database_name)
                .unwrap_or(resolved_object_name);

            return Err(format!("database '{}' not found", database_name));

        };

        let mut normalized_read_plan = read_plan;
        normalized_read_plan.table_id = table_id;
        return Ok((catalog, normalized_read_plan));

    }

    if resolved_object_name.contains('.') {

        let Some((catalog, table_id)) = resolve_catalog_for_table_reference_mut(
            catalogs,
            requested_database_id,
            resolved_object_name,
        ) else {

            let database_name = resolved_object_name
                .rsplit_once('.')
                .map(|(database_name, _)| database_name)
                .unwrap_or(resolved_object_name);

            return Err(format!("database '{}' not found", database_name));

        };

        let mut normalized_read_plan = read_plan;
        normalized_read_plan.table_id = table_id;
        return Ok((catalog, normalized_read_plan));

    }

    let Some(catalog) = resolve_catalog_mut(catalogs, requested_database_id) else {
        return Err(format!("database '{}' not found", requested_database_id));
    };

    let mut normalized_read_plan = read_plan;

    normalize_select_read_plan_for_active_database(
        &mut normalized_read_plan,
        requested_database_id,
    );

    Ok((catalog, normalized_read_plan))

}

fn execute_select_read_plan(
    request_id: &str,
    database_id: &str,
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> ConnectorResponse {

    if read_plan.lock_mode != serverlib::SelectLockMode::None {

        if read_plan.is_explain {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                "select lock clause is not supported with EXPLAIN".to_string(),
            );
        }

        let lock_targets = match resolve_select_lock_targets(catalog, read_plan) {
            Ok(targets) => targets,
            Err(message) => {
                return ConnectorResponse::rejected(request_id.to_string(), message);
            }
        };

        if lock_targets.is_empty() {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                "select lock clause requires at least one table-backed relation target"
                    .to_string(),
            );
        }

        let acquired_targets = match acquire_select_lock_targets(catalog, &lock_targets) {
            Ok(acquired) => acquired,
            Err(message) => {
                return ConnectorResponse::rejected(request_id.to_string(), message);
            }
        };

        let response = execute_select_read_plan_without_lock(
            request_id,
            database_id,
            catalog,
            wal,
            runtime_indexes,
            read_plan,
            session_variable_overrides,
        );

        if let Err(message) = release_select_lock_targets(catalog, &acquired_targets) {
            return ConnectorResponse::rejected(request_id.to_string(), message);
        }

        return response;
    }

    execute_select_read_plan_without_lock(
        request_id,
        database_id,
        catalog,
        wal,
        runtime_indexes,
        read_plan,
        session_variable_overrides,
    )

}

fn execute_select_read_plan_without_lock(
    request_id: &str,
    database_id: &str,
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> ConnectorResponse {

    if !read_plan.ctes.is_empty() {

        let result = match execute_select_with_ctes(
            catalog,
            wal,
            runtime_indexes,
            read_plan,
            session_variable_overrides,
        ) {
            
            Ok(result) => result,
            
            Err(message) => {
                return ConnectorResponse::rejected(request_id.to_string(), message);
            }

        };

        return applied_query_response(request_id, result);

    }

    if !read_plan.joins.is_empty() {
        return execute_joined_select(request_id, database_id, catalog, wal, runtime_indexes, read_plan);
    }

    let table_id = read_plan.table_id.as_str();

    if table_id.is_empty() {

        if read_plan.lock_mode != serverlib::SelectLockMode::None {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                "select lock clause requires a direct table target".to_string(),
            );
        }

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
                    None,
                    runtime_indexes,
                    read_plan,
                ),
            );

        }

        let result =
            match serverlib::execute_projection_only_select_plan(read_plan, &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
                serverlib::execute_sql_function_with_lookup(
                    catalog,
                    wal,
                    runtime_indexes,
                    function,
                    lookup,
                )
            })) {

                Ok(result) => result,

                Err(message) => {
                    return ConnectorResponse::rejected(request_id.to_string(), message);
                }

            };

        return applied_query_response(request_id, result);

    }

    let view_sql = catalog
        .view(table_id)
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

        let view_result = match execute_select_plan_result(catalog, wal, runtime_indexes, &view_read_plan) {

            Ok(result) => result,

            Err(message) => {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("view execution failed: {message}"),
                );
            }

        };

        let result = match execute_view_over_scoped_materialization(
            catalog,
            wal,
            runtime_indexes,
            table_id,
            read_plan,
            view_result,
        ) {
            
            Ok(result) => result,

            Err(message) => {
                return ConnectorResponse::rejected(request_id.to_string(), message);
            }

        };

        return applied_query_response(request_id, result);

    }

    let Some(schema) = catalog.table_schema(table_id).cloned() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                table_id, database_id
            ),
        );
    };

    let Some(mut scoped_table) = catalog.table(table_id).cloned() else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                table_id, database_id
            ),
        );
    };

    if let Some(stream_id) = catalog.entity_wal_stream_id(table_id) {
        scoped_table.entity_id = stream_id;
    }

    let mut index_filter_map = HashMap::new();

    let like_filter = read_plan
        .where_condition
        .as_ref()
        .and_then(|condition| {
            collect_indexable_like_filter_for_schema(&schema, condition)
        });

    let allow_index_short_circuit = read_plan
        .where_condition
        .as_ref()
        .map(|condition| {
            collect_indexable_equality_filters_for_schema(
                &schema,
                condition,
                &mut index_filter_map,
            )
        })
        .unwrap_or(true);

    let access_plan = plan_relation_access(
        &scoped_table,
        allow_index_short_circuit,
        index_filter_map,
        like_filter,
    );

    let index_lookup = access_plan.runtime_index_lookup(&scoped_table);

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
                Some(&access_plan),
                index_lookup,
                runtime_indexes,
                read_plan,
            ),
        );
    }

    match serverlib::execute_relation_select_plan(
        wal,
        &scoped_table,
        &schema,
        runtime_indexes,
        read_plan,
        &access_plan,
        &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
            
            serverlib::execute_sql_function_with_lookup(
                catalog,
                wal,
                runtime_indexes,
                function,
                lookup,
            )

        }),
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
        Ok(result) => applied_query_response(request_id, result),
        Err(message) => ConnectorResponse::rejected(request_id.to_string(), message),
    }

}

fn resolve_select_lock_targets(
    catalog: &DatabaseCatalog,
    read_plan: &serverlib::SelectReadPlan,
) -> Result<Vec<String>, String> {

    let mut targets = HashSet::<String>::new();
    let mut visited_views = HashSet::<String>::new();

    collect_select_lock_targets(catalog, read_plan, &mut targets, &mut visited_views)?;

    let mut ordered = targets.into_iter().collect::<Vec<_>>();
    ordered.sort();
    
    Ok(ordered)

}

fn collect_select_lock_targets(
    catalog: &DatabaseCatalog,
    read_plan: &serverlib::SelectReadPlan,
    targets: &mut HashSet<String>,
    visited_views: &mut HashSet<String>,
) -> Result<(), String> {
    
    for cte in &read_plan.ctes {
        collect_select_lock_targets(catalog, &cte.read_plan, targets, visited_views)?;
        if let Some(recursive) = cte.recursive_read_plan.as_ref() {
            collect_select_lock_targets(catalog, recursive, targets, visited_views)?;
        }
    }

    collect_select_lock_target_relation(catalog, &read_plan.table_id, targets, visited_views)?;

    for relation in &read_plan.relations {
        collect_select_lock_target_relation(catalog, &relation.table_id, targets, visited_views)?;
    }

    for join in &read_plan.joins {
        collect_select_lock_target_relation(catalog, &join.relation.table_id, targets, visited_views)?;
    }

    Ok(())

}

fn collect_select_lock_target_relation(
    catalog: &DatabaseCatalog,
    relation_id: &str,
    targets: &mut HashSet<String>,
    visited_views: &mut HashSet<String>,
) -> Result<(), String> {

    if relation_id.trim().is_empty() {
        return Ok(());
    }

    let normalized = common::normalize_identifier!(relation_id);

    if catalog.table(&normalized).is_some() {
        targets.insert(normalized);
        return Ok(());
    }

    let Some(view_sql) = catalog.view(&normalized).map(|view| view.sql.clone()) else {
        return Ok(());
    };

    if !visited_views.insert(normalized.clone()) {
        return Ok(());
    }

    let view_select_sql = extract_view_select_sql(&view_sql)
        .map_err(|message| format!("select lock resolution failed: {message}"))?;

    let view_read_plan = serverlib::parse_select_read_plan_from_statement(&view_select_sql)
        .map_err(|err| {
            format!(
                "select lock resolution failed: view '{}' parse failed: {err}",
                normalized
            )
        })?;

    collect_select_lock_targets(catalog, &view_read_plan, targets, visited_views)

}

fn acquire_select_lock_targets(
    catalog: &mut DatabaseCatalog,
    lock_targets: &[String],
) -> Result<Vec<String>, String> {

    let mut acquired = Vec::new();

    for table_id in lock_targets {
        let already_locked = catalog
            .table(table_id)
            .is_some_and(|table| table.status() == ObjectStatus::Lock);

        if already_locked {
            continue;
        }

        if let Err(err) = catalog.begin_table_write(table_id) {
            let _ = release_select_lock_targets(catalog, &acquired);
            return Err(format!("table read lock failed: {err}"));
        }

        acquired.push(table_id.clone());
    }

    Ok(acquired)

}

fn release_select_lock_targets(
    catalog: &mut DatabaseCatalog,
    acquired_targets: &[String],
) -> Result<(), String> {

    for table_id in acquired_targets.iter().rev() {
        if let Err(err) = catalog.abort_table_write(table_id) {
            return Err(format!("table read lock release failed: {err}"));
        }
    }

    Ok(())

}

fn normalize_select_read_plan_for_active_database(
    read_plan: &mut serverlib::SelectReadPlan,
    active_database_id: &str,
) {

    read_plan.table_id = strip_matching_database_prefix(&read_plan.table_id, active_database_id);

    for relation in &mut read_plan.relations {
        relation.table_id = strip_matching_database_prefix(&relation.table_id, active_database_id);
    }

    for join in &mut read_plan.joins {
        
        join.relation.table_id = strip_matching_database_prefix(
            &join.relation.table_id,
            active_database_id,
        );

    }

}

fn parse_debug_entity_request(statement_sql: &str) -> Result<(String, String), String> {

    let tokens = statement_sql
        .split_whitespace()
        .map(|token| token.trim())
        .filter(|token| !token.is_empty())
        .collect::<Vec<_>>();

    if tokens.len() != 3 || !tokens[0].eq_ignore_ascii_case("debug") {
        return Err(
            "debug usage: debug <databaseentitytype> <entityname>".to_string(),
        );
    }

    let entity_type = tokens[1]
        .trim_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .to_ascii_lowercase();

    let entity_name = tokens[2]
        .trim_matches(';')
        .trim_matches('`')
        .trim_matches('"')
        .to_string();

    if entity_type.is_empty() || entity_name.is_empty() {
        return Err(
            "debug usage: debug <databaseentitytype> <entityname>".to_string(),
        );
    }

    Ok((entity_type, entity_name))

}

fn resolve_catalog_and_object_for_lookup<'a>(
    catalogs: &'a HashMap<String, DatabaseCatalog>,
    requested_database_id: &str,
    object_name: &str,
) -> Result<(&'a DatabaseCatalog, String, String), String> {

    if requested_database_id.trim().is_empty() {

        if let Some((database_name, object_id)) = object_name.rsplit_once('.') {

            let Some(catalog) = resolve_catalog(catalogs, database_name) else {
                return Err(format!("database '{}' not found", database_name));
            };

            let resolved_database_id = catalog.database_id.0.clone();
            return Ok((
                catalog,
                common::normalize_identifier!(object_id),
                resolved_database_id,
            ));

        }

        return Err(format!("database '{}' not found", object_name));
    }

    if let Some((database_name, object_id)) = object_name.rsplit_once('.') {

        let Some(catalog) = resolve_catalog(catalogs, database_name) else {
            return Err(format!("database '{}' not found", database_name));
        };

        let resolved_database_id = catalog.database_id.0.clone();
        return Ok((
            catalog,
            common::normalize_identifier!(object_id),
            resolved_database_id,
        ));
        
    }

    let Some(catalog) = resolve_catalog(catalogs, requested_database_id) else {
        return Err(format!("database '{}' not found", requested_database_id));
    };

    let resolved_database_id = catalog.database_id.0.clone();
    Ok((
        catalog,
        common::normalize_identifier!(object_name),
        resolved_database_id,
    ))

}

fn debug_attribute_result(rows: Vec<(String, String)>) -> serverlib::SelectExecutionResult {

    serverlib::SelectExecutionResult {
        columns: vec![
            serverlib::FieldDef {
                seqno: 1,
                field_name: "attribute".to_string(),
                field_type: serverlib::FieldType::Text,
                nullable: false,
                indexed: serverlib::FieldIndex::None,
                default_value: None,
                metadata: None,
            },
            serverlib::FieldDef {
                seqno: 2,
                field_name: "value".to_string(),
                field_type: serverlib::FieldType::Text,
                nullable: false,
                indexed: serverlib::FieldIndex::None,
                default_value: None,
                metadata: None,
            },
        ],
        rows: rows
            .into_iter()
            .map(|(attribute, value)| vec![attribute.into_bytes(), value.into_bytes()])
            .collect(),
    }

}

fn applied_query_response(
    request_id: &str,
    result: serverlib::SelectExecutionResult,
) -> ConnectorResponse {

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(QueryResult {
            columns: connector_field_defs(result.columns),
            rows: result.rows,
            timings: empty_query_timings(),
        }),
    )

}

fn build_debug_rows(
    catalog: &DatabaseCatalog,
    entity_type: &str,
    normalized_object_id: &str,
    resolved_database_id: &str,
) -> Result<Vec<(String, String)>, String> {

    match entity_type {

        "table" => build_table_debug_rows(catalog, normalized_object_id, resolved_database_id),

        "view" => build_view_debug_rows(catalog, normalized_object_id, resolved_database_id),

        "trigger" => build_trigger_debug_rows(catalog, normalized_object_id, resolved_database_id),

        "procedure" | "stored_procedure" | "function" | "stored_function" => {
            build_routine_debug_rows(
                catalog,
                entity_type,
                normalized_object_id,
                resolved_database_id,
            )
        }

        _ => Err(format!(
            "debug entity type '{}' is not supported; expected one of: table, view, trigger, procedure, function",
            entity_type,
        )),

    }

}

fn build_table_debug_rows(
    catalog: &DatabaseCatalog,
    normalized_object_id: &str,
    resolved_database_id: &str,
) -> Result<Vec<(String, String)>, String> {

    let Some(table) = catalog.table(normalized_object_id) else {
        return Err(format!(
            "table '{}' not found in database '{}'",
            normalized_object_id,
            resolved_database_id,
        ));
    };

    let field_summary = table
        .schema
        .fields
        .iter()
        .map(|field| {
            let sql_type = field
                .metadata
                .as_ref()
                .and_then(|meta| meta.original_sql_type.as_deref())
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| format!("{:?}", field.field_type));

            format!("{}:{}", field.field_name, sql_type)
        })
        .collect::<Vec<_>>()
        .join(", ");

    Ok(vec![
        ("entity_type".to_string(), "table".to_string()),
        ("entity_name".to_string(), table.table_id.clone()),
        ("database_id".to_string(), resolved_database_id.to_string()),
        ("entity_id".to_string(), table.entity_id.clone()),
        ("status".to_string(), table.status.to_string()),
        ("schema_revision".to_string(), table.schema_revision.to_string()),
        ("temporary".to_string(),
            if table.is_temporary() {
                "true".to_string()
            } else {
                "false".to_string()
            },
        ),
        ("field_count".to_string(), table.schema.fields.len().to_string()),
        ("index_count".to_string(), table.indexes.len().to_string()),
        ("fields".to_string(), field_summary),
    ])

}

fn build_view_debug_rows(
    catalog: &DatabaseCatalog,
    normalized_object_id: &str,
    resolved_database_id: &str,
) -> Result<Vec<(String, String)>, String> {

    let Some(view) = catalog.view(normalized_object_id) else {
        return Err(format!(
            "view '{}' not found in database '{}'",
            normalized_object_id,
            resolved_database_id,
        ));
    };

    Ok(vec![
        ("entity_type".to_string(), "view".to_string()),
        ("entity_name".to_string(), view.view_id.clone()),
        ("database_id".to_string(), resolved_database_id.to_string()),
        ("entity_id".to_string(), view.entity_id.clone()),
        ("dependency_count".to_string(), view.dependencies.len().to_string()),
        ("dependencies".to_string(), view.dependencies.join(",")),
        ("schema_field_count".to_string(), view.schema.fields.len().to_string()),
        ("sql".to_string(), view.sql.clone()),
    ])

}

fn build_trigger_debug_rows(
    catalog: &DatabaseCatalog,
    normalized_object_id: &str,
    resolved_database_id: &str,
) -> Result<Vec<(String, String)>, String> {

    let Some(trigger) = catalog.trigger(normalized_object_id) else {
        return Err(format!(
            "trigger '{}' not found in database '{}'",
            normalized_object_id,
            resolved_database_id,
        ));
    };

    let binding_summary = trigger
        .invocation_binding()
        .map(|binding| {
            format!(
                "table={} timing={:?} event={:?}",
                binding.table_id,
                binding.timing,
                binding.event
            )
        })
        .unwrap_or_else(|| "<none>".to_string());

    Ok(vec![
        ("entity_type".to_string(), "trigger".to_string()),
        ("entity_name".to_string(), trigger.trigger_id.clone()),
        ("database_id".to_string(), resolved_database_id.to_string()),
        ("entity_id".to_string(), trigger.entity_id.clone()),
        ("dependency_count".to_string(), trigger.dependencies.len().to_string()),
        ("dependencies".to_string(), trigger.dependencies.join(",")),
        ("invocation_binding".to_string(), binding_summary),
        ("sql".to_string(), trigger.sql.clone()),
    ])

}

fn build_routine_debug_rows(
    catalog: &DatabaseCatalog,
    entity_type: &str,
    normalized_object_id: &str,
    resolved_database_id: &str,
) -> Result<Vec<(String, String)>, String> {

    let Some(procedure) = catalog.stored_procedure(normalized_object_id) else {
        return Err(format!(
            "routine '{}' not found in database '{}'",
            normalized_object_id,
            resolved_database_id,
        ));
    };

    let routine_kind = if procedure
        .sql
        .trim()
        .to_ascii_lowercase()
        .starts_with("create function")
    {
        "stored_function"
    } else {
        "stored_procedure"
    };

    if (entity_type == "function" || entity_type == "stored_function")
        && routine_kind != "stored_function"
    {
        return Err(format!(
            "object '{}' is not a stored function",
            normalized_object_id,
        ));
    }

    if (entity_type == "procedure" || entity_type == "stored_procedure")
        && routine_kind != "stored_procedure"
    {
        return Err(format!(
            "object '{}' is not a stored procedure",
            normalized_object_id,
        ));
    }

    let procedure_dependencies = procedure.dependencies.join(",");

    let (
        cache_present,
        resource_count,
        result_set_count,
        resources,
        result_sets,
        procedure_variables,
        procedure_outputs,
    ) = if let Some(artifact) = procedure.compiled_artifact() {
        
        let resource_text =
            serverlib::format_sql_programatic_resource_manifest(&artifact.resources);

        let mut variable_entries = artifact
            .resources
            .iter()
            .filter(|entry| entry.kind == serverlib::StoredProcedureResourceKind::Variable)
            .map(|entry| format!("{}({:?})", entry.name, entry.direction))
            .collect::<Vec<_>>();
        
        variable_entries.sort();
        variable_entries.dedup();

        let mut output_entries = artifact
            .resources
            .iter()
            .filter(|entry| entry.direction == serverlib::StoredProcedureResourceDirection::Out)
            .map(|entry| format!("{:?}:{}", entry.kind, entry.name))
            .collect::<Vec<_>>();
        
        output_entries.sort();
        output_entries.dedup();

        let result_set_text = artifact
            .result_sets
            .iter()
            .enumerate()
            .map(|(index, shape)| {
                format!(
                    "#{} source={} wildcard={} columns={}",
                    index,
                    shape.source_sql,
                    shape.wildcard,
                    shape.columns.join(","),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        (
            "true".to_string(),
            artifact.resources.len().to_string(),
            artifact.result_sets.len().to_string(),
            resource_text,
            result_set_text,
            variable_entries.join(","),
            output_entries.join(","),
        )

    } else {
        
        (
            "false".to_string(),
            "0".to_string(),
            "0".to_string(),
            String::new(),
            String::new(),
            String::new(),
            String::new(),
        )

    };

    Ok(vec![
        ("entity_type".to_string(), routine_kind.to_string()),
        ("entity_name".to_string(), procedure.procedure_id.clone()),
        ("database_id".to_string(), resolved_database_id.to_string()),
        ("entity_id".to_string(), procedure.entity_id.clone()),
        ("dependency_count".to_string(), procedure.dependencies.len().to_string()),
        ("dependencies".to_string(), procedure_dependencies),
        ("procedure_dependencies".to_string(), procedure.dependencies.join(",")),
        ("cache_present".to_string(), cache_present),
        ("resource_count".to_string(), resource_count),
        ("result_set_count".to_string(), result_set_count),
        ("procedure_variables".to_string(), procedure_variables),
        ("procedure_outputs".to_string(), procedure_outputs),
        ("resources".to_string(), resources),
        ("result_sets".to_string(), result_sets),
        ("sql".to_string(), procedure.sql.clone()),
    ])

}

fn strip_matching_database_prefix(table_id: &str, active_database_id: &str) -> String {

    let normalized_table_id = common::normalize_identifier!(table_id);
    let normalized_active_database_id = common::normalize_identifier!(active_database_id);

    normalized_table_id
        .rsplit_once('.')
        .and_then(|(database_name, referenced_table_id)| {
            if common::normalize_identifier!(database_name) == normalized_active_database_id {
                Some(common::normalize_identifier!(referenced_table_id))
            } else {
                None
            }
        })
        .unwrap_or(normalized_table_id)

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

pub(super) fn execute_select_with_ctes(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
    session_variable_overrides: Option<&SessionVariableOverrides>,
) -> Result<serverlib::SelectExecutionResult, String> {

    let mut scoped_handles = Vec::with_capacity(read_plan.ctes.len());

    let execution_result = (|| -> Result<serverlib::SelectExecutionResult, String> {
        let recursive_cte_settings =
            effective_recursive_cte_execution_settings(catalog, session_variable_overrides);

        for cte in &read_plan.ctes {

            if catalog.table(&cte.table_id).is_some() || catalog.view(&cte.table_id).is_some() {
                return Err(format!(
                    "cte execution failed: cte '{}' conflicts with existing table/view",
                    cte.table_id
                ));
            }

            let seed_result = execute_select_plan_result(catalog, wal, runtime_indexes, &cte.read_plan)
                .map_err(|message| format!("cte execution failed: {message}"))?;

            let cte_schema = TableSchema::new(seed_result.columns.clone());

            let mut scoped_handle = serverlib::create_scoped_ephemeral_table(
                catalog,
                wal,
                cte.table_id.clone(),
                cte_schema.clone(),
            )
            .map_err(|message| format!("cte execution failed: {message}"))?;

            materialize_select_result_into_scoped_table(
                catalog,
                wal,
                runtime_indexes,
                scoped_handle.table_id(),
                &seed_result,
            )
            .map_err(|message| format!("cte execution failed: {message}"))?;

            if let Some(recursive_plan) = cte.recursive_read_plan.as_ref() {

                let mut accumulated = seed_result;
                if accumulated.rows.len() > recursive_cte_settings.max_rows {
                    return Err(format!(
                        "cte execution failed: recursive CTE '{}' exceeded max rows ({})",
                        cte.table_id,
                        recursive_cte_settings.max_rows,
                    ));
                }

                let mut frontier_rows = accumulated.rows.clone();
                let mut seen_rows = if cte.recursive_union_all {
                    HashSet::new()
                } else {
                    accumulated
                        .rows
                        .iter()
                        .cloned()
                        .collect::<HashSet<Vec<Vec<u8>>>>()
                };
                let mut seen_frontiers = HashSet::<Vec<Vec<Vec<u8>>>>::new();
                let iteration_started_at = std::time::Instant::now();

                let mut iterations = 0usize;

                loop {
                    if iterations >= recursive_cte_settings.max_iterations {
                        return Err(format!(
                            "cte execution failed: recursive CTE '{}' exceeded max iterations ({})",
                            cte.table_id,
                            recursive_cte_settings.max_iterations,
                        ));
                    }

                    if recursive_cte_settings.timeout_ms > 0
                        && iteration_started_at.elapsed().as_millis()
                            >= recursive_cte_settings.timeout_ms as u128
                    {
                        return Err(format!(
                            "cte execution failed: recursive CTE '{}' exceeded timeout ({} ms)",
                            cte.table_id,
                            recursive_cte_settings.timeout_ms,
                        ));
                    }

                    iterations += 1;

                    if frontier_rows.is_empty() {
                        break;
                    }

                    if cte.recursive_union_all
                        && recursive_cte_settings.detect_repeating_union_all_frontier
                        && !seen_frontiers.insert(frontier_rows.clone())
                    {
                        return Err(format!(
                            "cte execution failed: recursive CTE '{}' detected a repeating UNION ALL frontier",
                            cte.table_id,
                        ));
                    }

                    let frontier_result = serverlib::SelectExecutionResult {
                        columns: accumulated.columns.clone(),
                        rows: frontier_rows,
                    };

                    rematerialize_scoped_table_result(
                        catalog,
                        wal,
                        runtime_indexes,
                        &mut scoped_handle,
                        &cte_schema,
                        &frontier_result,
                    )
                    .map_err(|message| format!("cte execution failed: {message}"))?;

                    let recursive_result = execute_select_plan_result(
                        catalog,
                        wal,
                        runtime_indexes,
                        recursive_plan,
                    )
                    .map_err(|message| format!("cte execution failed: {message}"))?;

                    if recursive_result.columns.len() != accumulated.columns.len() {
                        return Err(format!(
                            "cte execution failed: recursive term for '{}' returned {} columns but expected {}",
                            cte.table_id,
                            recursive_result.columns.len(),
                            accumulated.columns.len(),
                        ));
                    }

                    let mut delta_rows = recursive_result
                        .rows
                        .into_iter()
                        .filter(|row| cte.recursive_union_all || seen_rows.insert(row.clone()))
                        .collect::<Vec<_>>();

                    if delta_rows.is_empty() {
                        break;
                    }

                    accumulated.rows.extend(delta_rows.iter().cloned());

                    if accumulated.rows.len() > recursive_cte_settings.max_rows {
                        return Err(format!(
                            "cte execution failed: recursive CTE '{}' exceeded max rows ({})",
                            cte.table_id,
                            recursive_cte_settings.max_rows,
                        ));
                    }

                    frontier_rows = std::mem::take(&mut delta_rows);
                }

                rematerialize_scoped_table_result(
                    catalog,
                    wal,
                    runtime_indexes,
                    &mut scoped_handle,
                    &cte_schema,
                    &accumulated,
                )
                .map_err(|message| format!("cte execution failed: {message}"))?;
            }

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

fn rematerialize_scoped_table_result(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    handle: &mut serverlib::ScopedEphemeralTableHandle,
    schema: &TableSchema,
    result: &serverlib::SelectExecutionResult,
) -> Result<(), String> {

    let logical_table_id = handle.table_id().to_string();

    serverlib::release_scoped_ephemeral_table(catalog, wal, handle)
        .map_err(|err| format!("scoped rematerialization release failed: {err}"))?;

    *handle = serverlib::create_scoped_ephemeral_table(
        catalog,
        wal,
        logical_table_id,
        schema.clone(),
    )
    .map_err(|message| format!("scoped rematerialization create failed: {message}"))?;

    materialize_select_result_into_scoped_table(
        catalog,
        wal,
        runtime_indexes,
        handle.table_id(),
        result,
    )

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
            catalog,
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
    _database_id: &str,
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
        &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
            serverlib::execute_sql_function_with_lookup(catalog, wal, runtime_indexes, function, lookup)
        }),
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

    applied_query_response(request_id, result)

}

