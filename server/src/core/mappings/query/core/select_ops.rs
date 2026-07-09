use super::*;

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
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    _node_data_dir: &Path,
    runtime_indexes: &mut RuntimeIndexStore,
    statement: &SqlRequest,
) -> ConnectorResponse {

    let statement_sql_lower = statement.sql.to_ascii_lowercase();

    if let Some(response) = handle_select_introspection_request(
        request_id,
        query,
        catalogs,
        statement,
        &statement_sql_lower,
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
        &query.database_id,
        &resolved_object_name,
        read_plan,
    ) {
        
        Ok(resolved) => resolved,
        
        Err(message) => {
            return ConnectorResponse::rejected(request_id.to_string(), message);
        }

    };

    execute_select_read_plan(request_id, query, catalog, wal, runtime_indexes, &read_plan)

}

fn handle_select_introspection_request(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    statement: &SqlRequest,
    statement_sql_lower: &str,
) -> Option<ConnectorResponse> {

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

    if statement_sql_lower.starts_with("show tables") {

        let target_db = statement
            .object_name
            .as_deref()
            .unwrap_or(&query.database_id);

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

    if statement_sql_lower.starts_with("debug ") {

        let (entity_type, entity_name) = match parse_debug_entity_request(&statement.sql) {
            Ok(parsed) => parsed,
            Err(message) => {
                return Some(ConnectorResponse::rejected(request_id.to_string(), message));
            }
        };

        let (catalog, normalized_object_id, resolved_database_id) =
            match resolve_catalog_and_object_for_lookup(catalogs, &query.database_id, &entity_name)
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

    if statement_sql_lower.starts_with("describe ")
        || statement_sql_lower.starts_with("desc ")
        || statement_sql_lower.starts_with("show columns")
    {

        let Some(object_name) = statement.object_name.as_deref() else {
            return Some(ConnectorResponse::rejected(
                request_id.to_string(),
                "describe/show columns missing table identifier",
            ));
        };

        let (catalog, normalized_object_id) = if query.database_id.trim().is_empty() {

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
                        if query.database_id.is_empty() {
                            object_name
                        } else {
                            &query.database_id
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

            let Some(catalog) = resolve_catalog(catalogs, &query.database_id) else {
                return Some(ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("database '{}' not found", query.database_id),
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
                    if query.database_id.trim().is_empty() {
                        catalog.database_id.0.as_str()
                    } else {
                        query.database_id.as_str()
                    }
                ),
            ));

        };

        return Some(applied_query_response(request_id, result));

    }

    None

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
    query: &DataQuery,
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &mut RuntimeIndexStore,
    read_plan: &serverlib::SelectReadPlan,
) -> ConnectorResponse {

    if !read_plan.ctes.is_empty() {

        let result = match execute_select_with_ctes(catalog, wal, runtime_indexes, read_plan) {
            
            Ok(result) => result,
            
            Err(message) => {
                return ConnectorResponse::rejected(request_id.to_string(), message);
            }

        };

        return applied_query_response(request_id, result);

    }

    if !read_plan.joins.is_empty() {
        return execute_joined_select(request_id, query, catalog, wal, runtime_indexes, read_plan);
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

    let result = match serverlib::execute_relation_select_plan(
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
    ) {
        Ok(result) => result,
        Err(message) => return ConnectorResponse::rejected(request_id.to_string(), message),
    };

    applied_query_response(request_id, result)

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

