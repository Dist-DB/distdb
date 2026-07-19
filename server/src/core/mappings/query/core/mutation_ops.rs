use super::*;
use crate::core::mappings::query::core::wal_ops::table_stream_id;
use std::sync::Arc;

const ORDER_EXPR_LOWER_PREFIX: &str = serverlib::ORDER_EXPR_LOWER_PREFIX;
const ORDER_EXPR_UPPER_PREFIX: &str = serverlib::ORDER_EXPR_UPPER_PREFIX;
const ORDER_EXPR_ABS_PREFIX: &str = serverlib::ORDER_EXPR_ABS_PREFIX;
const ORDER_EXPR_LENGTH_PREFIX: &str = serverlib::ORDER_EXPR_LENGTH_PREFIX;
const ORDER_EXPR_REVERSE_PREFIX: &str = serverlib::ORDER_EXPR_REVERSE_PREFIX;
const ORDER_EXPR_TRIM_PREFIX: &str = serverlib::ORDER_EXPR_TRIM_PREFIX;
const ORDER_EXPR_LTRIM_PREFIX: &str = serverlib::ORDER_EXPR_LTRIM_PREFIX;
const ORDER_EXPR_RTRIM_PREFIX: &str = serverlib::ORDER_EXPR_RTRIM_PREFIX;
const ORDER_EXPR_CEIL_PREFIX: &str = serverlib::ORDER_EXPR_CEIL_PREFIX;
const ORDER_EXPR_FLOOR_PREFIX: &str = serverlib::ORDER_EXPR_FLOOR_PREFIX;
const ORDER_EXPR_ROUND_PREFIX: &str = serverlib::ORDER_EXPR_ROUND_PREFIX;
const ORDER_EXPR_ROUND_SCALE_PREFIX: &str = serverlib::ORDER_EXPR_ROUND_SCALE_PREFIX;

type MutationRowMap = Arc<HashMap<String, Vec<u8>>>;

trait ReturningRowRef {
    fn row_map_ref(&self) -> &HashMap<String, Vec<u8>>;
}

impl ReturningRowRef for HashMap<String, Vec<u8>> {
    fn row_map_ref(&self) -> &HashMap<String, Vec<u8>> {
        self
    }
}

impl ReturningRowRef for MutationRowMap {
    fn row_map_ref(&self) -> &HashMap<String, Vec<u8>> {
        self.as_ref()
    }
}

pub(super) fn execute_insert_impl(
    request_id: &str,
    database_id: &str,
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
            vec!["ignore".to_string(), plan.ignore.to_string()],
            vec!["replace_into".to_string(), plan.replace_into.to_string()],
            vec![
                "on_duplicate_update_count".to_string(),
                plan.on_duplicate_update.len().to_string(),
            ],
            vec!["column_count".to_string(), plan.columns.len().to_string()],
            vec![
                "returning_count".to_string(),
                plan.returning
                    .as_ref()
                    .map(|items| items.len().to_string())
                    .unwrap_or_else(|| "0".to_string()),
            ],
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
        database_id,
        &plan.table_id,
        external_write_group_id.is_some(),
        |catalog| {
        execute_insert_locked(
            request_id,
            database_id,
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
    database_id: &str,
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
                plan.table_id, database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, database_id
            ),
        );
    };

    let table_stream_id = table_stream_id(catalog, &plan.table_id);

    let is_default_values_insert = plan.columns.is_empty()
        && matches!(
            &plan.source,
            serverlib::InsertRowsSource::Values(rows)
                if !rows.is_empty() && rows.iter().all(|row| row.is_empty())
        );

    let columns = if plan.columns.is_empty() && !is_default_values_insert {
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
            let mut returning_rows: Vec<MutationRowMap> = if plan.returning.is_some() {
                Vec::with_capacity(insert_rows.len())
            } else {
                Vec::new()
            };
            let mut pk_checks = 0u64;
            let mut staged_payloads = Vec::with_capacity(insert_rows.len());
            let mut staged_rows = Vec::<MutationRowMap>::with_capacity(insert_rows.len());
            let track_runtime_indexes_for_insert = derived_indexes_for_table(table).next().is_some();
            let mut staged_pk_keys = HashSet::<Vec<Vec<u8>>>::with_capacity(insert_rows.len());
            let mut staged_pk_positions = HashMap::<Vec<Vec<u8>>, usize>::with_capacity(insert_rows.len());
            let mut existing_pk_keys = HashSet::<Vec<Vec<u8>>>::new();
            let mut existing_pk_rows = HashMap::<Vec<Vec<u8>>, (u64, MutationRowMap)>::new();
            let payload_context = payload_context_for_table(catalog, &plan.table_id);
            let current_live_rows = load_live_rows_with_context(
                wal,
                &table_stream_id,
                schema,
                &payload_context,
            )
            .unwrap_or_default()
            .into_iter()
            .map(|(row_id, row_map)| (row_id, Arc::new(row_map)))
            .collect::<Vec<_>>();
            let current_live_row_ids = current_live_rows
                .iter()
                .map(|(row_id, _)| *row_id)
                .collect::<HashSet<_>>();

            existing_pk_keys.reserve(current_live_rows.len());
            existing_pk_rows.reserve(current_live_rows.len());
            let primary_key_details = primary_key_index(table).map(|pk_index| {
                let pk_fields = if pk_index.field_names.is_empty() && !pk_index.field_name.is_empty() {
                    vec![pk_index.field_name.as_str()]
                } else {
                    pk_index.field_names.iter().map(|name| name.as_str()).collect()
                };

                for (row_id, row_map) in current_live_rows.iter().cloned() {
                    let key = index_value_tuple(pk_index, row_map.as_ref());
                    existing_pk_keys.insert(key.clone());
                    existing_pk_rows.insert(key, (row_id, row_map));
                }

                (pk_index.index_id.0.as_str(), pk_fields)
            });

            let unique_indexes = table
                .indexes
                .values()
                .filter(|index| !index.is_temporary() && index.is_unique_key())
                .collect::<Vec<_>>();

            let non_primary_unique_indexes = unique_indexes
                .iter()
                .copied()
                .filter(|index| !index.is_primary_key())
                .collect::<Vec<_>>();

            let mut existing_unique_rows = HashMap::<
                &str,
                HashMap<Vec<Vec<u8>>, (u64, MutationRowMap)>,
            >::with_capacity(unique_indexes.len());

            for index in &unique_indexes {
                let mut index_rows = HashMap::with_capacity(current_live_rows.len());
                for (row_id, row_map) in current_live_rows.iter().cloned() {
                    index_rows.insert(index_value_tuple(index, row_map.as_ref()), (row_id, row_map));
                }
                existing_unique_rows.insert(index.index_id.0.as_str(), index_rows);
            }

            let mut staged_unique_positions = unique_indexes
                .iter()
                .map(|index| {
                    (
                        index.index_id.0.as_str(),
                        HashMap::with_capacity(insert_rows.len()),
                    )
                })
                .collect::<HashMap<&str, HashMap<Vec<Vec<u8>>, usize>>>();

            let insert_requires_row_map = plan.returning.is_some()
                || track_runtime_indexes_for_insert
                || plan.replace_into
                || !plan.on_duplicate_update.is_empty()
                || primary_key_details.is_some()
                || !non_primary_unique_indexes.is_empty();

            if !plan.on_duplicate_update.is_empty()
                && unique_indexes.is_empty()
            {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    "insert failed: ON DUPLICATE KEY UPDATE requires a primary or unique key"
                        .to_string(),
                );
            }

            if plan.replace_into && unique_indexes.is_empty() {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    "insert failed: REPLACE INTO requires a primary or unique key in current implementation".to_string(),
                );
            }

            if let Some((_, pk_fields)) = primary_key_details.as_ref()
                && plan
                    .on_duplicate_update
                    .iter()
                    .any(|assignment| pk_fields.iter().any(|pk| *pk == assignment.field_name))
            {
                return ConnectorResponse::rejected(
                    request_id.to_string(),
                    "insert failed: ON DUPLICATE KEY UPDATE cannot modify primary key fields in current implementation".to_string(),
                );
            }

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

                if !insert_requires_row_map {
                    staged_payloads.push(encoded);
                    affected_rows = affected_rows.saturating_add(1);
                    continue;
                }

                let canonical_row = match decode_row_payload(schema, &encoded) {
                    Ok(row) => row,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert payload decode failed: {err}"),
                        );
                    }
                };

                if plan.replace_into {
                    let mut persisted_conflict = None;
                    let mut staged_conflict_position = None;

                    for index in &unique_indexes {
                        let index_id = index.index_id.0.as_str();
                        let key = index_value_tuple(index, &canonical_row);

                        if let Some(position) = staged_unique_positions
                            .get(index_id)
                            .and_then(|positions| positions.get(&key))
                            .copied()
                        {
                            staged_conflict_position = Some(position);
                            break;
                        }

                        if let Some((row_id, row)) = existing_unique_rows
                            .get(index_id)
                            .and_then(|rows| rows.get(&key))
                            .cloned()
                        {
                            persisted_conflict = Some((row_id, row));
                            break;
                        }
                    }

                    if let Some((row_id, existing_row)) = persisted_conflict {
                        let delete_payload = match encode_row_payload(schema, existing_row.as_ref()) {
                            Ok(encoded) => encoded,
                            Err(err) => {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!("insert replace payload encode failed: {err}"),
                                );
                            }
                        };

                        if let Err(err) = append_row_payload_record_with_live_row_ids_and_prepared_row_map(
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
                            Some(existing_row.as_ref()),
                            Some(group_id),
                        ) {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("insert replace delete WAL append failed: {err}"),
                            );
                        }

                        if let Err(err) = append_row_payload_record_with_prepared_row_map(
                            catalog,
                            wal,
                            &table_stream_id,
                            table,
                            runtime_indexes,
                            TransactionKind::Insert,
                            encoded,
                            Some(&canonical_row),
                            common::epoch_nanos!(),
                            None,
                            Some(group_id),
                        ) {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("insert replace insert WAL append failed: {err}"),
                            );
                        }

                        let canonical_row = Arc::new(canonical_row);

                        for index in &unique_indexes {
                            let old_key = index_value_tuple(index, existing_row.as_ref());
                            let new_key = index_value_tuple(index, canonical_row.as_ref());
                            if let Some(rows) = existing_unique_rows.get_mut(index.index_id.0.as_str()) {
                                rows.remove(&old_key);
                                rows.insert(new_key, (row_id, Arc::clone(&canonical_row)));
                            }
                        }

                        if plan.returning.is_some() {
                            returning_rows.push(Arc::clone(&canonical_row));
                        }

                        affected_rows = affected_rows.saturating_add(1);
                        continue;
                    }

                    if let Some(position) = staged_conflict_position {
                        let previous_row = Arc::clone(&staged_rows[position]);

                        let canonical_row = Arc::new(canonical_row);

                        staged_payloads[position] = encoded;
                        staged_rows[position] = Arc::clone(&canonical_row);

                        for index in &unique_indexes {
                            let old_key = index_value_tuple(index, previous_row.as_ref());
                            let new_key = index_value_tuple(index, canonical_row.as_ref());
                            let positions = staged_unique_positions
                                .get_mut(index.index_id.0.as_str())
                                .expect("staged unique index positions should be initialized");
                            positions.remove(&old_key);
                            positions.insert(new_key, position);
                        }

                        if plan.returning.is_some() {
                            returning_rows.push(Arc::clone(&canonical_row));
                        }

                        affected_rows = affected_rows.saturating_add(1);
                        continue;
                    }
                }

                if !plan.on_duplicate_update.is_empty() {
                    let mut persisted_conflict = None;
                    let mut staged_conflict_position = None;

                    for index in &unique_indexes {
                        let index_id = index.index_id.0.as_str();
                        let key = index_value_tuple(index, &canonical_row);

                        if let Some(position) = staged_unique_positions
                            .get(index_id)
                            .and_then(|positions| positions.get(&key))
                            .copied()
                        {
                            staged_conflict_position = Some(position);
                            break;
                        }

                        if let Some((row_id, row)) = existing_unique_rows
                            .get(index_id)
                            .and_then(|rows| rows.get(&key))
                            .cloned()
                        {
                            persisted_conflict = Some((row_id, row));
                            break;
                        }
                    }

                    if let Some((row_id, existing_row)) = persisted_conflict {
                        let delete_payload = match encode_row_payload(schema, existing_row.as_ref()) {
                            Ok(encoded) => encoded,
                            Err(err) => {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!("insert duplicate update payload encode failed: {err}"),
                                );
                            }
                        };

                        let mut updated_row = existing_row.as_ref().clone();

                        if let Err(message) = apply_insert_on_duplicate_assignments(
                            &mut updated_row,
                            &plan.on_duplicate_update,
                            &canonical_row,
                        ) {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("insert duplicate update failed: {message}"),
                            );
                        }

                        for field in &schema.fields {
                            if !updated_row.contains_key(&field.field_name) && !field.nullable {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!(
                                        "insert duplicate update failed: column '{}' cannot be null",
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
                                    format!("insert duplicate update payload encode failed: {err}"),
                                );
                            }
                        };

                        let updated_row = match decode_row_payload(schema, &insert_payload) {
                            Ok(row) => row,
                            Err(err) => {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!("insert duplicate update payload decode failed: {err}"),
                                );
                            }
                        };

                        let updated_row = Arc::new(updated_row);

                        if let Err(err) = append_row_payload_record_with_live_row_ids_and_prepared_row_map(
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
                            Some(existing_row.as_ref()),
                            Some(group_id),
                        ) {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("insert duplicate update delete WAL append failed: {err}"),
                            );
                        }

                        if let Err(err) = append_row_payload_record_with_prepared_row_map(
                            catalog,
                            wal,
                            &table_stream_id,
                            table,
                            runtime_indexes,
                            TransactionKind::Insert,
                            insert_payload,
                            Some(updated_row.as_ref()),
                            common::epoch_nanos!(),
                            None,
                            Some(group_id),
                        ) {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("insert duplicate update insert WAL append failed: {err}"),
                            );
                        }

                        for index in &unique_indexes {
                            let old_key = index_value_tuple(index, existing_row.as_ref());
                            let new_key = index_value_tuple(index, updated_row.as_ref());
                            if let Some(rows) = existing_unique_rows.get_mut(index.index_id.0.as_str()) {
                                rows.remove(&old_key);
                                rows.insert(new_key, (row_id, Arc::clone(&updated_row)));
                            }
                        }

                        if let Some((_, pk_fields)) = primary_key_details.as_ref() {
                            let old_pk = pk_fields
                                .iter()
                                .map(|pk| existing_row.get(*pk).cloned().unwrap_or_default())
                                .collect::<Vec<_>>();
                            let new_pk = pk_fields
                                .iter()
                                .map(|pk| updated_row.get(*pk).cloned().unwrap_or_default())
                                .collect::<Vec<_>>();

                            existing_pk_keys.remove(&old_pk);
                            existing_pk_keys.insert(new_pk.clone());
                            existing_pk_rows.remove(&old_pk);
                            existing_pk_rows.insert(new_pk, (row_id, Arc::clone(&updated_row)));
                        }

                        if plan.returning.is_some() {
                            returning_rows.push(Arc::clone(&updated_row));
                        }

                        affected_rows = affected_rows.saturating_add(1);
                        continue;
                    }

                    if let Some(position) = staged_conflict_position {
                        let previous_row = Arc::clone(&staged_rows[position]);
                        let mut updated_row = previous_row.as_ref().clone();

                        if let Err(message) = apply_insert_on_duplicate_assignments(
                            &mut updated_row,
                            &plan.on_duplicate_update,
                            &canonical_row,
                        ) {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("insert duplicate update failed: {message}"),
                            );
                        }

                        for field in &schema.fields {
                            if !updated_row.contains_key(&field.field_name) && !field.nullable {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!(
                                        "insert duplicate update failed: column '{}' cannot be null",
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
                                    format!("insert duplicate update payload encode failed: {err}"),
                                );
                            }
                        };

                        let updated_row = match decode_row_payload(schema, &insert_payload) {
                            Ok(row) => row,
                            Err(err) => {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    format!("insert duplicate update payload decode failed: {err}"),
                                );
                            }
                        };

                        let updated_row = Arc::new(updated_row);

                        staged_payloads[position] = insert_payload;
                        staged_rows[position] = Arc::clone(&updated_row);

                        for index in &unique_indexes {
                            let old_key = index_value_tuple(index, previous_row.as_ref());
                            let new_key = index_value_tuple(index, updated_row.as_ref());
                            let positions = staged_unique_positions
                                .get_mut(index.index_id.0.as_str())
                                .expect("staged unique index positions should be initialized");
                            positions.remove(&old_key);
                            positions.insert(new_key, position);
                        }

                        if let Some((_, pk_fields)) = primary_key_details.as_ref() {
                            let old_pk = pk_fields
                                .iter()
                                .map(|pk| previous_row.get(*pk).cloned().unwrap_or_default())
                                .collect::<Vec<_>>();
                            let new_pk = pk_fields
                                .iter()
                                .map(|pk| updated_row.get(*pk).cloned().unwrap_or_default())
                                .collect::<Vec<_>>();

                            staged_pk_keys.remove(&old_pk);
                            staged_pk_keys.insert(new_pk.clone());
                            staged_pk_positions.remove(&old_pk);
                            staged_pk_positions.insert(new_pk, position);
                        }

                        if plan.returning.is_some() {
                            returning_rows.push(Arc::clone(&updated_row));
                        }

                        affected_rows = affected_rows.saturating_add(1);
                        continue;
                    }
                }

                if !plan.replace_into && plan.on_duplicate_update.is_empty() {

                    let mut non_primary_unique_conflict: Option<String> = None;
                    let mut has_non_primary_unique_conflict = false;

                    for index in &non_primary_unique_indexes {

                        let index_id = index.index_id.0.as_str();
                        let key = index_value_tuple(index, &canonical_row);

                        let has_staged_conflict = staged_unique_positions
                            .get(index_id)
                            .and_then(|positions| positions.get(&key))
                            .is_some();

                        let has_persisted_conflict = existing_unique_rows
                            .get(index_id)
                            .and_then(|rows| rows.get(&key))
                            .is_some();

                        if !has_staged_conflict && !has_persisted_conflict {
                            continue;
                        }

                        has_non_primary_unique_conflict = true;

                        if !plan.ignore {
                            let unique_display = index
                                .field_names
                                .iter()
                                .zip(key.iter())
                                .map(|(name, val)| {
                                    format!(
                                        "{}={}",
                                        name,
                                        serverlib::display_stored_field_value(val)
                                    )
                                })
                                .collect::<Vec<_>>()
                                .join(", ");

                            non_primary_unique_conflict = Some(unique_display);
                        }

                        break;

                    }

                    if has_non_primary_unique_conflict {

                        if plan.ignore {
                            continue;
                        }

                        let unique_display = non_primary_unique_conflict
                            .unwrap_or_else(|| "unknown".to_string());

                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("insert failed: duplicate unique key ({})", unique_display),
                        );

                    }
                    
                }

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
                        if !plan.on_duplicate_update.is_empty() {

                            if let Some((row_id, existing_row)) = existing_pk_rows.get(&incoming_pk).cloned() {

                                let delete_payload = match encode_row_payload(schema, existing_row.as_ref()) {
                                    Ok(encoded) => encoded,
                                    Err(err) => {
                                        return ConnectorResponse::rejected(
                                            request_id.to_string(),
                                            format!("insert duplicate update payload encode failed: {err}"),
                                        );
                                    }
                                };

                                let mut updated_row = existing_row.as_ref().clone();
                                
                                if let Err(message) = apply_insert_on_duplicate_assignments(
                                    &mut updated_row,
                                    &plan.on_duplicate_update,
                                    &canonical_row,
                                ) {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!("insert duplicate update failed: {message}"),
                                    );
                                }

                                for field in &schema.fields {
                                    if !updated_row.contains_key(&field.field_name) && !field.nullable {
                                        return ConnectorResponse::rejected(
                                            request_id.to_string(),
                                            format!(
                                                "insert duplicate update failed: column '{}' cannot be null",
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
                                            format!("insert duplicate update payload encode failed: {err}"),
                                        );
                                    }
                                };

                                let updated_row = match decode_row_payload(schema, &insert_payload) {
                                    Ok(row) => row,
                                    Err(err) => {
                                        return ConnectorResponse::rejected(
                                            request_id.to_string(),
                                            format!("insert duplicate update payload decode failed: {err}"),
                                        );
                                    }
                                };

                                let updated_row = Arc::new(updated_row);

                                if let Err(err) = append_row_payload_record_with_live_row_ids_and_prepared_row_map(
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
                                    Some(existing_row.as_ref()),
                                    Some(group_id),
                                ) {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!("insert duplicate update delete WAL append failed: {err}"),
                                    );
                                }

                                if let Err(err) = append_row_payload_record_with_prepared_row_map(
                                    catalog,
                                    wal,
                                    &table_stream_id,
                                    table,
                                    runtime_indexes,
                                    TransactionKind::Insert,
                                    insert_payload,
                                    Some(updated_row.as_ref()),
                                    common::epoch_nanos!(),
                                    None,
                                    Some(group_id),
                                ) {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!("insert duplicate update insert WAL append failed: {err}"),
                                    );
                                }

                                if plan.returning.is_some() {
                                    returning_rows.push(Arc::clone(&updated_row));
                                }

                                existing_pk_rows.insert(incoming_pk.clone(), (row_id, updated_row));
                                affected_rows = affected_rows.saturating_add(1);
                                continue;
                            }

                            if let Some(position) = staged_pk_positions.get(&incoming_pk).copied() {

                                let mut updated_row = staged_rows[position].as_ref().clone();
                                if let Err(message) = apply_insert_on_duplicate_assignments(
                                    &mut updated_row,
                                    &plan.on_duplicate_update,
                                    &canonical_row,
                                ) {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!("insert duplicate update failed: {message}"),
                                    );
                                }

                                for field in &schema.fields {
                                    if !updated_row.contains_key(&field.field_name) && !field.nullable {
                                        return ConnectorResponse::rejected(
                                            request_id.to_string(),
                                            format!(
                                                "insert duplicate update failed: column '{}' cannot be null",
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
                                            format!("insert duplicate update payload encode failed: {err}"),
                                        );
                                    }
                                };

                                let updated_row = match decode_row_payload(schema, &insert_payload) {
                                    Ok(row) => row,
                                    Err(err) => {
                                        return ConnectorResponse::rejected(
                                            request_id.to_string(),
                                            format!("insert duplicate update payload decode failed: {err}"),
                                        );
                                    }
                                };

                                let updated_row = Arc::new(updated_row);

                                staged_payloads[position] = insert_payload;
                                staged_rows[position] = Arc::clone(&updated_row);

                                if plan.returning.is_some() {
                                    returning_rows.push(Arc::clone(&updated_row));
                                }

                                affected_rows = affected_rows.saturating_add(1);
                                continue;
                            }

                            {
                                return ConnectorResponse::rejected(
                                    request_id.to_string(),
                                    "insert failed: ON DUPLICATE KEY UPDATE could not resolve duplicate key source".to_string(),
                                );
                            }
                        }

                        if plan.ignore {
                            continue;
                        }

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

                if let Some((_, pk_fields)) = primary_key_details.as_ref() {
                    let incoming_pk = pk_fields
                        .iter()
                        .map(|pk| canonical_row.get(*pk).cloned().unwrap_or_default())
                        .collect::<Vec<_>>();
                    let position = staged_payloads.len().saturating_sub(1);
                    staged_pk_positions.insert(incoming_pk, position);
                }

                let position = staged_payloads.len().saturating_sub(1);
                for index in &unique_indexes {
                    let key = index_value_tuple(index, &canonical_row);
                    staged_unique_positions
                        .get_mut(index.index_id.0.as_str())
                        .expect("staged unique index positions should be initialized")
                        .insert(key, position);
                }

                let canonical_row = Arc::new(canonical_row);

                if plan.returning.is_some() {
                    returning_rows.push(Arc::clone(&canonical_row));
                }

                staged_rows.push(canonical_row);

                affected_rows = affected_rows.saturating_add(1);

            }

            let staged_row_maps = track_runtime_indexes_for_insert.then_some(staged_rows);

            if let Err(err) = append_row_payload_records_batch(
                catalog,
                wal,
                &table_stream_id,
                table,
                runtime_indexes,
                TransactionKind::Insert,
                staged_payloads,
                staged_row_maps,
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

            mutation_response_for_result(
                request_id,
                "insert",
                schema,
                affected_rows,
                plan.returning.as_ref(),
                &returning_rows,
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
                )

            } else if read_plan.table_id.is_empty() {
                
                serverlib::execute_projection_only_select_plan(read_plan, &mut serverlib::with_lookup_sql_function_evaluator(|function, lookup| {
                    serverlib::execute_sql_function_with_lookup(
                        catalog,
                        wal,
                        runtime_indexes,
                        function,
                        lookup,
                    )
                }))

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
    database_id: &str,
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
        database_id,
        &plan.table_id,
        external_write_group_id.is_some(),
        |catalog| {
        execute_update_locked(
            request_id,
            database_id,
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
    database_id: &str,
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
                plan.table_id, database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, database_id
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

    let mut current_live_rows = match load_mutation_rows(
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

    if !plan.order_by.is_empty() {
        apply_delete_order_by_items(&mut current_live_rows, &plan.order_by);
    }

    if let Some(limit) = plan.limit {
        current_live_rows.truncate(limit);
    }

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
            .map(|(_, row_map)| index_value_tuple(pk_index, row_map.as_ref()))
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    let update_requires_canonical_row = primary_key.is_some()
        || plan.returning.is_some()
        || derived_indexes_for_table(table).next().is_some();

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
            let mut returning_rows = if plan.returning.is_some() {
                Vec::with_capacity(current_live_rows.len())
            } else {
                Vec::new()
            };

            for (row_id, row_map) in current_live_rows {

                if !mutation_uses_joins
                    && !serverlib::row_matches_select_condition(
                        row_map.as_ref(),
                        plan.where_condition.as_ref(),
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                {
                    continue;
                }

                let delete_payload = match encode_row_payload(schema, row_map.as_ref()) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("update delete payload encode failed: {err}"),
                        );
                    }
                };

                let old_pk = primary_key.map(|pk_index| index_value_tuple(pk_index, row_map.as_ref()));

                let mut updated_row = row_map.as_ref().clone();

                for assignment in &plan.assignments {
                    let resolved_value = match &assignment.value {
                        serverlib::UpdateAssignmentValue::Literal(value) => value.clone(),

                        serverlib::UpdateAssignmentValue::FunctionExpression(expression_sql) => {
                            match evaluate_mutation_function_expression(
                                expression_sql,
                                &updated_row,
                                None,
                            ) {
                                Ok(value) => value,
                                Err(message) => {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!("update failed: {message}"),
                                    );
                                }
                            }
                        }

                        serverlib::UpdateAssignmentValue::ExistingColumn(column_name) => {
                            match updated_row.get(column_name).cloned() {
                                Some(value) => Some(value),
                                None => {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!(
                                            "update failed: assignment column '{}' was not found in current row",
                                            column_name
                                        ),
                                    );
                                }
                            }
                        }

                        serverlib::UpdateAssignmentValue::Arithmetic { left, op, right } => {
                            match evaluate_update_assignment_arithmetic(&updated_row, left, *op, right) {
                                Ok(value) => Some(value),
                                Err(message) => {
                                    return ConnectorResponse::rejected(
                                        request_id.to_string(),
                                        format!("update failed: {message}"),
                                    );
                                }
                            }
                        }
                    };

                    match resolved_value {
                        Some(value) => {
                            if let Some(slot) = updated_row.get_mut(&assignment.field_name) {
                                *slot = value;
                            } else {
                                updated_row.insert(assignment.field_name.clone(), value);
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

                let canonical_row = if update_requires_canonical_row {
                    Some(match decode_row_payload(schema, &insert_payload) {
                        Ok(row) => row,
                        Err(err) => {
                            return ConnectorResponse::rejected(
                                request_id.to_string(),
                                format!("update insert payload decode failed: {err}"),
                            );
                        }
                    })
                } else {
                    None
                };

                if let Some(pk_index) = primary_key {
                    let updated_row = canonical_row.as_ref().expect(
                        "canonical row should be present when primary key validation is required",
                    );

                    let old_pk = old_pk.expect("primary key tuple should exist when primary key index is present");
                    let new_pk = index_value_tuple(pk_index, updated_row);

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

                if let Err(err) = append_row_payload_record_with_live_row_ids_and_prepared_row_map(
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
                    Some(row_map.as_ref()),
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("update delete WAL append failed: {err}"),
                    );
                }

                if let Err(err) = append_row_payload_record_with_prepared_row_map(
                    catalog,
                    wal,
                    &table_stream_id,
                    table,
                    runtime_indexes,
                    TransactionKind::Insert,
                    insert_payload,
                    canonical_row.as_ref(),
                    common::epoch_nanos!(),
                    None,
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("update insert WAL append failed: {err}"),
                    );
                }

                if plan.returning.is_some() {
                    returning_rows.push(canonical_row.expect(
                        "canonical row should be present when RETURNING is requested",
                    ));
                }

                affected_rows = affected_rows.saturating_add(1);

            }

            mutation_response_for_result(
                request_id,
                "update",
                schema,
                affected_rows,
                plan.returning.as_ref(),
                &returning_rows,
            )

        },
    )

}

pub(super) fn execute_delete_impl(
    request_id: &str,
    database_id: &str,
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
        database_id,
        &plan.table_id,
        external_write_group_id.is_some(),
        |catalog| {
        execute_delete_locked(
            request_id,
            database_id,
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
    database_id: &str,
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
                plan.table_id, database_id
            ),
        );
    };

    let Some(table) = catalog.table(&plan.table_id) else {
        return ConnectorResponse::rejected(
            request_id.to_string(),
            format!(
                "table '{}' not found in database '{}'",
                plan.table_id, database_id
            ),
        );
    };

    let table_stream_id = table_stream_id(catalog, &plan.table_id);

    let mutation_uses_joins = !plan.joins.is_empty();

    let mut current_live_rows = match load_mutation_rows(
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

    if !plan.order_by.is_empty() {
        apply_delete_order_by_items(&mut current_live_rows, &plan.order_by);
    }

    if let Some(limit) = plan.limit {
        current_live_rows.truncate(limit);
    }

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
            let mut returning_rows = if plan.returning.is_some() {
                Vec::with_capacity(current_live_rows.len())
            } else {
                Vec::new()
            };

            for (row_id, row_map) in current_live_rows {

                if !mutation_uses_joins
                    && !serverlib::row_matches_select_condition(
                        row_map.as_ref(),
                        plan.where_condition.as_ref(),
                        catalog,
                        wal,
                        runtime_indexes,
                    )
                {
                    continue;
                }

                let delete_payload = match encode_row_payload(schema, row_map.as_ref()) {
                    Ok(encoded) => encoded,
                    Err(err) => {
                        return ConnectorResponse::rejected(
                            request_id.to_string(),
                            format!("delete payload encode failed: {err}"),
                        );
                    }
                };

                if let Err(err) = append_row_payload_record_with_live_row_ids_and_prepared_row_map(
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
                    Some(row_map.as_ref()),
                    Some(group_id),
                ) {
                    return ConnectorResponse::rejected(
                        request_id.to_string(),
                        format!("delete WAL append failed: {err}"),
                    );
                }

                if plan.returning.is_some() {
                    returning_rows.push(row_map);
                }

                affected_rows = affected_rows.saturating_add(1);
            
            }

            mutation_response_for_result(
                request_id,
                "delete",
                schema,
                affected_rows,
                plan.returning.as_ref(),
                &returning_rows,
            )

        },
    )

}

fn evaluate_update_assignment_arithmetic(
    row: &HashMap<String, Vec<u8>>,
    left: &serverlib::UpdateAssignmentOperand,
    op: serverlib::UpdateArithmeticOp,
    right: &serverlib::UpdateAssignmentOperand,
) -> Result<Vec<u8>, String> {
    let left_value = resolve_update_assignment_operand(row, left)?;
    let right_value = resolve_update_assignment_operand(row, right)?;

    let result = match op {
        serverlib::UpdateArithmeticOp::Add => left_value + right_value,
        serverlib::UpdateArithmeticOp::Subtract => left_value - right_value,
        serverlib::UpdateArithmeticOp::Multiply => left_value * right_value,
        serverlib::UpdateArithmeticOp::Divide => {
            if right_value == 0.0 {
                return Err("arithmetic division by zero is not supported".to_string());
            }
            left_value / right_value
        }
        serverlib::UpdateArithmeticOp::Modulo => {
            if right_value == 0.0 {
                return Err("arithmetic modulo by zero is not supported".to_string());
            }
            left_value % right_value
        }
    };

    Ok(format_update_arithmetic_result(result).into_bytes())
}

fn resolve_update_assignment_operand(
    row: &HashMap<String, Vec<u8>>,
    operand: &serverlib::UpdateAssignmentOperand,
) -> Result<f64, String> {
    match operand {
        serverlib::UpdateAssignmentOperand::Unary { op, operand } => {
            let value = resolve_update_assignment_operand(row, operand)?;
            match op {
                serverlib::UnaryArithmeticOp::Plus => Ok(value),
                serverlib::UnaryArithmeticOp::Minus => Ok(-value),
            }
        }

        serverlib::UpdateAssignmentOperand::Arithmetic { left, op, right } => {
            let left_value = resolve_update_assignment_operand(row, left)?;
            let right_value = resolve_update_assignment_operand(row, right)?;

            match op {
                serverlib::UpdateArithmeticOp::Add => Ok(left_value + right_value),
                serverlib::UpdateArithmeticOp::Subtract => Ok(left_value - right_value),
                serverlib::UpdateArithmeticOp::Multiply => Ok(left_value * right_value),
                serverlib::UpdateArithmeticOp::Divide => {
                    if right_value == 0.0 {
                        return Err("arithmetic division by zero is not supported".to_string());
                    }
                    Ok(left_value / right_value)
                }
                serverlib::UpdateArithmeticOp::Modulo => {
                    if right_value == 0.0 {
                        return Err("arithmetic modulo by zero is not supported".to_string());
                    }
                    Ok(left_value % right_value)
                }
            }
        }

        serverlib::UpdateAssignmentOperand::Literal(value) => {
            let Some(value) = value.as_ref() else {
                return Err("arithmetic operand cannot be NULL".to_string());
            };
            parse_update_numeric_value(value)
        }

        serverlib::UpdateAssignmentOperand::FunctionExpression(expression_sql) => {
            let Some(value) = evaluate_mutation_function_expression(expression_sql, row, None)? else {
                return Err("arithmetic operand cannot be NULL".to_string());
            };
            parse_update_numeric_value(&value)
        }

        serverlib::UpdateAssignmentOperand::ExistingColumn(column_name) => {
            let Some(value) = row.get(column_name) else {
                return Err(format!(
                    "arithmetic assignment column '{}' was not found in current row",
                    column_name
                ));
            };
            parse_update_numeric_value(value)
        }
    }
}

fn parse_update_numeric_value(value: &[u8]) -> Result<f64, String> {
    let text = serverlib::display_stored_field_value(value);
    let text = text.trim().to_string();
    text.parse::<f64>()
        .map_err(|_| format!("arithmetic operand '{}' is not numeric", text))
}

fn format_update_arithmetic_result(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn apply_delete_order_by_items(
    rows: &mut Vec<(u64, MutationRowMap)>,
    order_by_items: &[serverlib::SelectOrderByItem],
) {

    rows.sort_by(|left, right| {
        for item in order_by_items {
            let left_value = resolve_mutation_order_value(left.1.as_ref(), &item.field_name);
            let right_value = resolve_mutation_order_value(right.1.as_ref(), &item.field_name);

            let ordering = compare_delete_order_values(&left_value, &right_value);

            if ordering != std::cmp::Ordering::Equal {
                return if item.descending {
                    ordering.reverse()
                } else {
                    ordering
                };
            }
        }

        std::cmp::Ordering::Equal
    });

}

fn resolve_mutation_order_value(row: &HashMap<String, Vec<u8>>, field_name: &str) -> String {
    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_LOWER_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).to_ascii_lowercase())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_UPPER_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).to_ascii_uppercase())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_ABS_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value))
            .and_then(|value| value.trim().parse::<f64>().ok().map(|parsed| parsed.abs().to_string()))
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_LENGTH_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).chars().count().to_string())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_REVERSE_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).chars().rev().collect::<String>())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_TRIM_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).trim().to_string())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_LTRIM_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).trim_start().to_string())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_RTRIM_PREFIX) {
        return row
            .get(column)
            .map(|value| serverlib::display_stored_field_value(value).trim_end().to_string())
            .unwrap_or_else(|| "NULL".to_string());
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_CEIL_PREFIX) {
        return resolve_mutation_inbuilt_order_expression(
            row,
            &format!("ceil({column})"),
        );
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_FLOOR_PREFIX) {
        return resolve_mutation_inbuilt_order_expression(
            row,
            &format!("floor({column})"),
        );
    }

    if let Some(column) = field_name.strip_prefix(ORDER_EXPR_ROUND_PREFIX) {
        return resolve_mutation_inbuilt_order_expression(
            row,
            &format!("round({column})"),
        );
    }

    if let Some(encoded) = field_name.strip_prefix(ORDER_EXPR_ROUND_SCALE_PREFIX) {
        let Some((scale_text, column)) = encoded.split_once(':') else {
            return "NULL".to_string();
        };

        let Ok(scale) = scale_text.parse::<i32>() else {
            return "NULL".to_string();
        };

        return resolve_mutation_inbuilt_order_expression(
            row,
            &format!("round({column},{scale})"),
        );
    }

    row.get(field_name)
        .map(|value| serverlib::display_stored_field_value(value))
        .unwrap_or_else(|| "NULL".to_string())
}

fn resolve_mutation_inbuilt_order_expression(
    row: &HashMap<String, Vec<u8>>,
    expression_sql: &str,
) -> String {
    evaluate_mutation_function_expression(expression_sql, row, None)
        .ok()
        .flatten()
        .map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        .unwrap_or_else(|| "NULL".to_string())
}

fn evaluate_mutation_function_expression(
    expression_sql: &str,
    primary_row: &HashMap<String, Vec<u8>>,
    secondary_row: Option<&HashMap<String, Vec<u8>>>,
) -> Result<Option<Vec<u8>>, String> {
    let mut lookup = |field_name: &str| -> Option<Vec<u8>> {
        let normalized = common::normalize_identifier!(field_name);

        primary_row
            .get(&normalized)
            .cloned()
            .or_else(|| primary_row.get(field_name).cloned())
            .or_else(|| {
                field_name.split_once('.').and_then(|(_, column)| {
                    let normalized_column = common::normalize_identifier!(column);
                    primary_row
                        .get(&normalized_column)
                        .cloned()
                        .or_else(|| primary_row.get(column).cloned())
                })
            })
            .or_else(|| {
                secondary_row.and_then(|row| {
                    row.get(&normalized)
                        .cloned()
                        .or_else(|| row.get(field_name).cloned())
                        .or_else(|| {
                            field_name.split_once('.').and_then(|(_, column)| {
                                let normalized_column = common::normalize_identifier!(column);
                                row.get(&normalized_column)
                                    .cloned()
                                    .or_else(|| row.get(column).cloned())
                            })
                        })
                })
            })
    };

    let mut nested = serverlib::engine::sql::evaluate_inbuilt_sql_function_with_lookup;

    serverlib::engine::sql::evaluate_expression_sql_to_bytes(
        expression_sql,
        &mut lookup,
        &mut nested,
    )
    .map(Some)
}

fn compare_delete_order_values(left: &str, right: &str) -> std::cmp::Ordering {

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
) -> Result<Vec<(u64, MutationRowMap)>, String> {

    let payload_context = payload_context_for_table(catalog, table_id);

    if joins.is_empty() {
        return load_live_rows_with_context(wal, table_stream_id, schema, &payload_context)
            .map(|rows| {
                rows.into_iter()
                    .map(|(row_id, row_map)| (row_id, Arc::new(row_map)))
                    .collect()
            });
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

fn apply_insert_on_duplicate_assignments(
    target_row: &mut HashMap<String, Vec<u8>>,
    assignments: &[serverlib::InsertOnDuplicateAssignment],
    incoming_row: &HashMap<String, Vec<u8>>,
) -> Result<(), String> {

    for assignment in assignments {

        let resolved_value = match &assignment.value {
            serverlib::InsertOnDuplicateAssignmentValue::Literal(value) => value.clone(),

            serverlib::InsertOnDuplicateAssignmentValue::FunctionExpression(expression_sql) => {
                evaluate_mutation_function_expression(
                    expression_sql,
                    target_row,
                    Some(incoming_row),
                )?
            }

            serverlib::InsertOnDuplicateAssignmentValue::IncomingColumn(column_name) => {
                Some(incoming_row.get(column_name).cloned().ok_or_else(|| {
                    format!(
                        "ON DUPLICATE KEY UPDATE VALUES({}) references unknown incoming column",
                        column_name
                    )
                })?)
            }

            serverlib::InsertOnDuplicateAssignmentValue::ExistingColumn(column_name) => {
                Some(target_row.get(column_name).cloned().ok_or_else(|| {
                    format!(
                        "ON DUPLICATE KEY UPDATE column '{}' references unknown existing column",
                        column_name
                    )
                })?)
            }

            serverlib::InsertOnDuplicateAssignmentValue::Arithmetic { left, op, right } => {
                Some(evaluate_insert_on_duplicate_arithmetic(
                    target_row,
                    incoming_row,
                    left,
                    *op,
                    right,
                )?)
            }
        };

        match resolved_value {
            Some(value) => {
                if let Some(slot) = target_row.get_mut(&assignment.field_name) {
                    *slot = value;
                } else {
                    target_row.insert(assignment.field_name.clone(), value);
                }
            }
            None => {
                target_row.remove(&assignment.field_name);
            }
        }
    }

    Ok(())

}

fn evaluate_insert_on_duplicate_arithmetic(
    target_row: &HashMap<String, Vec<u8>>,
    incoming_row: &HashMap<String, Vec<u8>>,
    left: &serverlib::InsertOnDuplicateAssignmentOperand,
    op: serverlib::InsertOnDuplicateArithmeticOp,
    right: &serverlib::InsertOnDuplicateAssignmentOperand,
) -> Result<Vec<u8>, String> {
    let left_value = resolve_insert_on_duplicate_operand(target_row, incoming_row, left)?;
    let right_value = resolve_insert_on_duplicate_operand(target_row, incoming_row, right)?;

    let result = match op {
        serverlib::InsertOnDuplicateArithmeticOp::Add => left_value + right_value,
        serverlib::InsertOnDuplicateArithmeticOp::Subtract => left_value - right_value,
        serverlib::InsertOnDuplicateArithmeticOp::Multiply => left_value * right_value,
        serverlib::InsertOnDuplicateArithmeticOp::Divide => {
            if right_value == 0.0 {
                return Err("ON DUPLICATE KEY UPDATE arithmetic division by zero is not supported".to_string());
            }
            left_value / right_value
        }
        serverlib::InsertOnDuplicateArithmeticOp::Modulo => {
            if right_value == 0.0 {
                return Err("ON DUPLICATE KEY UPDATE arithmetic modulo by zero is not supported".to_string());
            }
            left_value % right_value
        }
    };

    Ok(format_update_arithmetic_result(result).into_bytes())
}

fn resolve_insert_on_duplicate_operand(
    target_row: &HashMap<String, Vec<u8>>,
    incoming_row: &HashMap<String, Vec<u8>>,
    operand: &serverlib::InsertOnDuplicateAssignmentOperand,
) -> Result<f64, String> {
    match operand {
        serverlib::InsertOnDuplicateAssignmentOperand::Unary { op, operand } => {
            let value = resolve_insert_on_duplicate_operand(target_row, incoming_row, operand)?;
            match op {
                serverlib::UnaryArithmeticOp::Plus => Ok(value),
                serverlib::UnaryArithmeticOp::Minus => Ok(-value),
            }
        }

        serverlib::InsertOnDuplicateAssignmentOperand::Arithmetic { left, op, right } => {
            let left_value = resolve_insert_on_duplicate_operand(target_row, incoming_row, left)?;
            let right_value = resolve_insert_on_duplicate_operand(target_row, incoming_row, right)?;

            match op {
                serverlib::InsertOnDuplicateArithmeticOp::Add => Ok(left_value + right_value),
                serverlib::InsertOnDuplicateArithmeticOp::Subtract => Ok(left_value - right_value),
                serverlib::InsertOnDuplicateArithmeticOp::Multiply => Ok(left_value * right_value),
                serverlib::InsertOnDuplicateArithmeticOp::Divide => {
                    if right_value == 0.0 {
                        return Err(
                            "ON DUPLICATE KEY UPDATE arithmetic division by zero is not supported"
                                .to_string(),
                        );
                    }
                    Ok(left_value / right_value)
                }
                serverlib::InsertOnDuplicateArithmeticOp::Modulo => {
                    if right_value == 0.0 {
                        return Err(
                            "ON DUPLICATE KEY UPDATE arithmetic modulo by zero is not supported"
                                .to_string(),
                        );
                    }
                    Ok(left_value % right_value)
                }
            }
        }

        serverlib::InsertOnDuplicateAssignmentOperand::Literal(value) => {
            let Some(value) = value.as_ref() else {
                return Err("ON DUPLICATE KEY UPDATE arithmetic operand cannot be NULL".to_string());
            };
            parse_update_numeric_value(value)
        }

        serverlib::InsertOnDuplicateAssignmentOperand::FunctionExpression(expression_sql) => {
            let Some(value) = evaluate_mutation_function_expression(
                expression_sql,
                target_row,
                Some(incoming_row),
            )? else {
                return Err("ON DUPLICATE KEY UPDATE arithmetic operand cannot be NULL".to_string());
            };
            parse_update_numeric_value(&value)
        }

        serverlib::InsertOnDuplicateAssignmentOperand::IncomingColumn(column_name) => {
            let Some(value) = incoming_row.get(column_name) else {
                return Err(format!(
                    "ON DUPLICATE KEY UPDATE VALUES({}) references unknown incoming column",
                    column_name
                ));
            };
            parse_update_numeric_value(value)
        }

        serverlib::InsertOnDuplicateAssignmentOperand::ExistingColumn(column_name) => {
            let Some(value) = target_row.get(column_name) else {
                return Err(format!(
                    "ON DUPLICATE KEY UPDATE column '{}' references unknown existing column",
                    column_name
                ));
            };
            parse_update_numeric_value(value)
        }
    }
}

fn mutation_response_for_result<R>(
    request_id: &str,
    operation: &str,
    schema: &TableSchema,
    affected_rows: u64,
    returning: Option<&serverlib::MutationReturningPlan>,
    rows: &[R],
) -> ConnectorResponse
where
    R: ReturningRowRef,
{

    let Some(returning) = returning else {
        return ConnectorResponse::applied(
            request_id.to_string(),
            ConnectorResult::Mutation(MutationResult { affected_rows }),
        );
    };

    let query_result = match query_result_from_returning(schema, returning, rows) {
        Ok(result) => result,
        Err(message) => {
            return ConnectorResponse::rejected(
                request_id.to_string(),
                format!("{operation} returning failed: {message}"),
            );
        }
    };

    ConnectorResponse::applied(
        request_id.to_string(),
        ConnectorResult::Query(query_result),
    )

}

fn query_result_from_returning<R>(
    schema: &TableSchema,
    returning: &serverlib::MutationReturningPlan,
    rows: &[R],
) -> Result<QueryResult, String>
where
    R: ReturningRowRef,
{

    let mut projected = Vec::<(String, String)>::new();

    for item in returning {
        match item {
            serverlib::MutationReturningItem::Wildcard => {
                projected.extend(
                    schema
                        .fields
                        .iter()
                        .map(|field| (field.field_name.clone(), field.field_name.clone())),
                );
            }

            serverlib::MutationReturningItem::Column {
                field_name,
                output_name,
            } => projected.push((field_name.clone(), output_name.clone())),
        }
    }

    let mut columns = Vec::with_capacity(projected.len());

    for (idx, (field_name, output_name)) in projected.iter().enumerate() {
        let Some(field) = schema.field(field_name) else {
            return Err(format!("unknown column '{field_name}'"));
        };

        columns.push(connector::FieldDef {
            seqno: (idx + 1) as u32,
            field_name: output_name.clone(),
            field_type: field.field_type.clone(),
            nullable: field.nullable,
            indexed: connector::FieldIndex::None,
            default_value: None,
            metadata: field.metadata.clone(),
        });
    }

    let rows = rows
        .iter()
        .map(|row_map| {
            let row_map = row_map.row_map_ref();
            projected
                .iter()
                .map(|(field_name, _)| {
                    row_map
                        .get(field_name)
                        .map(|value| serverlib::display_stored_field_value(value).into_bytes())
                        .unwrap_or_else(|| b"NULL".to_vec())
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    Ok(QueryResult {
        columns,
        rows,
        timings: empty_query_timings(),
    })

}

