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

        let Some(object_name) = statement.object_name.as_deref() else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                "describe/show columns missing table identifier",
            );
        };

        let Some((catalog, table_id)) = resolve_catalog_for_table_reference(
            catalogs,
            &query.database_id,
            object_name,
        ) else {
            let database_name = if query.database_id.trim().is_empty() {
                object_name
                    .rsplit_once('.')
                    .map(|(database_name, _)| database_name)
                    .unwrap_or(object_name)
            } else {
                &query.database_id
            };

            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", database_name),
            );
        };

        let Some(schema) = catalog.table_schema(&table_id) else {
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

    let resolved_object_name = statement
        .object_name
        .as_deref()
        .unwrap_or(&read_plan.table_id);

    let (catalog, read_plan) = if query.database_id.trim().is_empty() {

        let Some((catalog, table_id)) = resolve_catalog_for_table_reference_mut(
            
            catalogs,
            &query.database_id,
            resolved_object_name,

        ) else {

            let database_name = resolved_object_name
                .rsplit_once('.')
                .map(|(database_name, _)| database_name)
                .unwrap_or(resolved_object_name);

            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", database_name),
            );

        };

        let mut normalized_read_plan = read_plan.clone();
        normalized_read_plan.table_id = table_id;
        (catalog, normalized_read_plan)

    } else if resolved_object_name.contains('.') {

        let Some((catalog, table_id)) = resolve_catalog_for_table_reference_mut(
            catalogs,
            &query.database_id,
            resolved_object_name,
        ) else {
            let database_name = resolved_object_name
                .rsplit_once('.')
                .map(|(database_name, _)| database_name)
                .unwrap_or(resolved_object_name);

            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", database_name),
            );
        };

        let mut normalized_read_plan = read_plan;
        normalized_read_plan.table_id = table_id;
        (catalog, normalized_read_plan)

    } else {

        let Some(catalog) = resolve_catalog_mut(catalogs, &query.database_id) else {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("database '{}' not found", query.database_id),
            );
        };

        let mut normalized_read_plan = read_plan;
        normalize_select_read_plan_for_active_database(
            &mut normalized_read_plan,
            &query.database_id,
        );

        (catalog, normalized_read_plan)

    };

    if !read_plan.ctes.is_empty() {

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
                &read_plan,
            ),
        );
    }

    let result = match serverlib::execute_relation_select_plan(
        wal,
        &scoped_table,
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

