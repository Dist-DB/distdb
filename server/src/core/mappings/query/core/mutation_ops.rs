use super::*;
use crate::core::mappings::query::core::wal_ops::table_stream_id;

pub(super) fn execute_insert_impl(
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
        external_write_group_id.is_some(),
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

    let table_stream_id = table_stream_id(catalog, &plan.table_id);

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
        catalog,
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
            let mut existing_pk_keys = HashSet::<Vec<Vec<u8>>>::new();
            let primary_key_details = primary_key_index(table).map(|pk_index| {
                let pk_fields = if pk_index.field_names.is_empty() && !pk_index.field_name.is_empty() {
                    vec![pk_index.field_name.as_str()]
                } else {
                    pk_index.field_names.iter().map(|name| name.as_str()).collect()
                };

                let payload_context = payload_context_for_table(catalog, &plan.table_id);
                existing_pk_keys = load_live_rows_with_context(
                    wal,
                    &table_stream_id,
                    schema,
                    &payload_context,
                )
                .unwrap_or_default()
                .into_iter()
                .map(|(_, row_map)| index_value_tuple(pk_index, &row_map))
                .collect();

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

                let encoded = match encode_row_payload(schema, &payload_row) {
                    
                    Ok(encoded) => encoded,
                    
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert payload encode failed: {err}"),
                        );
                    }

                };

                let canonical_row = match decode_row_payload(schema, &encoded) {
                    Ok(row) => row,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert payload decode failed: {err}"),
                        );
                    }
                };

                if let Some((pk_index_id, pk_fields)) = primary_key_details.as_ref() {
                    pk_checks = pk_checks.saturating_add(1);

                    let incoming_pk = pk_fields
                        .iter()
                        .map(|pk| canonical_row.get(*pk).cloned().unwrap_or_default())
                        .collect::<Vec<_>>();

                    let pk_runtime = runtime_indexes.index_for_table(&table_stream_id, pk_index_id);

                    if pk_runtime
                        .map(|idx| idx.contains(&incoming_pk))
                        .unwrap_or(false)
                        || existing_pk_keys.contains(&incoming_pk)
                        || staged_pk_keys.contains(&incoming_pk)
                    {
                        let pk_display = pk_fields
                            .iter()
                            .zip(incoming_pk.iter())
                            .map(|(name, val)| format!("{}={}", name, serverlib::display_stored_field_value(val)))
                            .collect::<Vec<_>>()
                            .join(", ");

                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert failed: duplicate primary key ({})", pk_display),
                        );
                    }

                    staged_pk_keys.insert(incoming_pk.clone());
                    existing_pk_keys.insert(incoming_pk);
                }

                staged_payloads.push(encoded);

                if track_runtime_indexes_for_insert {
                    staged_row_maps.push(canonical_row);
                }

                affected_rows = affected_rows.saturating_add(1);

            }

            if let Err(err) = append_row_payload_records_batch(
                wal,
                &table_stream_id,
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

#[expect(clippy::type_complexity, reason="this function is complex due to the nature of insert execution and row materialization")]
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
                )

            } else if read_plan.table_id.is_empty() {
                
                serverlib::execute_projection_only_select_plan(read_plan, &mut |function| {
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

                let access_plan =
                    plan_relation_access(
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

pub(super) fn execute_update_impl(
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
        external_write_group_id.is_some(),
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

    let table_stream_id = table_stream_id(catalog, &plan.table_id);

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
        &table_stream_id,
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
        catalog,
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

                let insert_payload = match encode_row_payload(schema, &updated_row) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("update insert payload encode failed: {err}"),
                        );
                    }
                };

                let updated_row = match decode_row_payload(schema, &insert_payload) {
                    Ok(row) => row,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("update insert payload decode failed: {err}"),
                        );
                    }
                };

                if let Some(pk_index) = primary_key {

                    let old_pk = old_pk.expect("primary key tuple should exist when primary key index is present");
                    let new_pk = index_value_tuple(pk_index, &updated_row);

                    if old_pk != new_pk && pk_keys.contains(&new_pk) {
                        let pk_display = primary_key_fields
                            .as_ref()
                            .expect("primary key fields should exist when primary key index is present")
                            .iter()
                            .zip(new_pk.iter())
                            .map(|(name, val)| format!("{}={}", name, serverlib::display_stored_field_value(val)))
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
                    catalog,
                    wal,
                    &table_stream_id,
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

                if let Err(err) = append_row_payload_record(
                    catalog,
                    wal,
                    &table_stream_id,
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

pub(super) fn execute_delete_impl(
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
        external_write_group_id.is_some(),
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

    let table_stream_id = table_stream_id(catalog, &plan.table_id);

    let mutation_uses_joins = !plan.joins.is_empty();

    let current_live_rows = match load_mutation_rows(
        catalog,
        wal,
        runtime_indexes,
        schema,
        &plan.table_id,
        &table_stream_id,
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
        catalog,
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
                    catalog,
                    wal,
                    &table_stream_id,
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
    defer_finalize: bool,
    execute: F,
) -> ConnectorResponse
where
    F: FnOnce(&mut DatabaseCatalog) -> ConnectorResponse,
{
    
    let Some((catalog, table_id)) = resolve_catalog_for_table_reference_mut(
        catalogs,
        database_id,
        table_id,
    ) else {
        let database_name = if database_id.trim().is_empty() {
            table_id
                .rsplit_once('.')
                .map(|(database_name, _)| database_name)
                .unwrap_or(table_id)
        } else {
            database_id
        };

        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("database '{}' not found", database_name),
        );
    };

    let already_locked = catalog
        .table(&table_id)
        .is_some_and(|table| table.status() == ObjectStatus::Lock);

    if !already_locked
        && let Err(err) = catalog.begin_table_write(&table_id)
    {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!("table write lock failed: {err}"),
        );
    }

    let response = execute(catalog);

    if matches!(response.status, connector::ResponseStatus::Applied) {

        if defer_finalize {
            return response;
        }

        match catalog.finalize_table_write(&table_id) {

            Ok(()) => response,

            Err(err) => {
                let _ = catalog.abort_table_write(&table_id);
                ConnectorResponse::rejected(
                    request_id.to_string(),
                    format!("table write finalize failed: {err}"),
                )
            }
        
        }

    } else {
        if !already_locked {
            let _ = catalog.abort_table_write(&table_id);
        }
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
    table_stream_id: &str,
    relations: &[serverlib::SelectRelation],
    pushdown_conditions: &[Option<SelectCondition>],
    joins: &[serverlib::SelectJoin],
    where_condition: Option<&SelectCondition>,
) -> Result<Vec<(u64, HashMap<String, Vec<u8>>)>, String> {

    let payload_context = payload_context_for_table(catalog, table_id);

    if joins.is_empty() {
        return load_live_rows_with_context(wal, table_stream_id, schema, &payload_context);
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

