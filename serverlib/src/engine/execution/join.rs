use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::engine::database::schema::migration::{convert_value_to_field_type, TypeConversionPolicy};
use crate::{
    ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore, SelectCondition, SelectJoin,
    SelectJoinKind, SelectRelation,
    render_stored_field_value,
};

use super::access::{
    build_relation_probe_index, collect_indexable_equality_filters_for_schema,
    collect_indexable_in_list_filter_for_schema,
    collect_indexable_like_filter_for_schema, collect_indexable_range_filters_for_schema,
    field_has_single_column_index, load_live_rows_by_equality, materialize_relation_rows,
    plan_relation_access,
    EqualityProbeSource,
};
use super::{
    join_condition_field_names, join_condition_matches_provider, relation_qualifier,
    JoinedRowCandidateProvider,
    JoinedRowTuple, MaterializedRelationRow, row_matches_condition_with_result,
};

const INNER_JOIN_KEY_PROBE_MAX_LEFT_ROWS: usize = 4096;
const INNER_JOIN_KEY_PROBE_MAX_DISTINCT_KEYS: usize = 8192;

pub fn build_joined_row_tuples<F>(
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    relations: &[SelectRelation],
    pushdown_conditions: &[Option<SelectCondition>],
    joins: &[SelectJoin],
    row_matches: &mut F,
) -> Result<Vec<JoinedRowTuple>, String>
where
    F: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> Result<bool, String>,
{

    let Some(primary_relation) = relations.first() else {
        return Ok(Vec::new());
    };

    let Some(primary_table) = catalog
        .table_handle(&primary_relation.table_id)
        .and_then(|handle| handle.table_snapshot()) else {
        return Err(format!(
            "select join failed: table '{}' not found",
            primary_relation.table_id
        ));
    };
    let primary_schema = &primary_table.schema;

    let scoped_primary_table_owned = catalog.entity_wal_stream_id(&primary_relation.table_id).map(|stream_id| {
        let mut table_with_stream = primary_table.clone();
        table_with_stream.entity_id = stream_id;
        table_with_stream
    });
    let scoped_primary_table = scoped_primary_table_owned.as_ref().unwrap_or(&primary_table);

    let primary_condition = pushdown_conditions.first().and_then(|condition| condition.as_ref());
    let mut primary_filter_map = HashMap::new();

    let primary_like_filter = primary_condition
        .as_ref()
        .and_then(|condition| collect_indexable_like_filter_for_schema(primary_schema, condition));
    let primary_in_list_filter = primary_condition
        .as_ref()
        .and_then(|condition| collect_indexable_in_list_filter_for_schema(primary_schema, condition));
    let primary_range_filters = primary_condition
        .as_ref()
        .map(|condition| collect_indexable_range_filters_for_schema(primary_schema, condition))
        .unwrap_or_default();

    let primary_allow_index_short_circuit = primary_condition
        .as_ref()
        .map(|condition| {

            collect_indexable_equality_filters_for_schema(
                primary_schema,
                condition,
                &mut primary_filter_map,
            )

        })
        .unwrap_or(true);

    let primary_access_plan = plan_relation_access(
        scoped_primary_table,
        primary_allow_index_short_circuit,
        primary_filter_map,
        primary_in_list_filter,
        primary_range_filters,
        primary_like_filter,
    );

    let mut joined_rows = materialize_relation_rows(
        wal,
        scoped_primary_table,
        primary_schema,
        runtime_indexes,
        &primary_access_plan,
    )
    .into_iter()
    .try_fold(Vec::new(), |mut acc, (row_id, row_map)| {

        if row_matches(&row_map, primary_condition)? {
            acc.push(JoinedRowTuple::from_relation_row(
                primary_relation,
                MaterializedRelationRow {
                    row_id,
                    row_map: Arc::new(row_map),
                },
            ));
        }

        Ok::<_, String>(acc)

    })?;

    for (join_index, join) in joins.iter().enumerate() {

        let Some(right_table) = catalog
            .table_handle(&join.relation.table_id)
            .and_then(|handle| handle.table_snapshot()) else {
            return Err(format!(
                "select join failed: table '{}' not found",
                join.relation.table_id
            ));
        };

        let right_schema = &right_table.schema;

        let scoped_right_table_owned = catalog.entity_wal_stream_id(&join.relation.table_id).map(|stream_id| {
            let mut table_with_stream = right_table.clone();
            table_with_stream.entity_id = stream_id;
            table_with_stream
        });

        let scoped_right_table = scoped_right_table_owned.as_ref().unwrap_or(&right_table);

        let right_condition = pushdown_conditions
            .get(join_index + 1)
            .and_then(|condition| condition.as_ref());

        let mut right_filter_map = HashMap::new();

        let right_like_filter = right_condition
            .as_ref()
            .and_then(|condition| collect_indexable_like_filter_for_schema(right_schema, condition));

        let right_in_list_filter = right_condition
            .as_ref()
            .and_then(|condition| collect_indexable_in_list_filter_for_schema(right_schema, condition));

        let right_range_filters = right_condition
            .as_ref()
            .map(|condition| collect_indexable_range_filters_for_schema(right_schema, condition))
            .unwrap_or_default();
        
        let right_allow_index_short_circuit = right_condition
            .as_ref()
            .map(|condition| {

                collect_indexable_equality_filters_for_schema(
                    right_schema,
                    condition,
                    &mut right_filter_map,
                )

            })
            .unwrap_or(true);

        let right_access_plan = plan_relation_access(
            scoped_right_table,
            right_allow_index_short_circuit,
            right_filter_map,
            right_in_list_filter,
            right_range_filters,
            right_like_filter,
        );

        let simple_join = join_condition_field_names(join).and_then(
            |(left_join_field_name, right_join_field_name)| {
                normalize_simple_join_field_orientation(
                    left_join_field_name,
                    right_join_field_name,
                    &join.relation,
                )
            },
        );

        let right_field_name = simple_join
            .map(|(_, right_join_field_name)| join_field_column_name(right_join_field_name));

        let mut materialize_right_rows = || {
            
            materialize_relation_rows(
                wal,
                scoped_right_table,
                right_schema,
                runtime_indexes,
                &right_access_plan,
            )
            .into_iter()
            .try_fold(Vec::new(), |mut acc, (row_id, row_map)| {

                if row_matches(&row_map, right_condition)? {
                    acc.push(MaterializedRelationRow {
                        row_id,
                        row_map: Arc::new(row_map),
                    });
                }

                Ok::<_, String>(acc)

            })

        };

        let right_rows = if matches!(join.kind, SelectJoinKind::Inner)
            && let Some((left_join_field_name, _)) = simple_join
            && let Some(right_join_field_name) = right_field_name
            && field_has_single_column_index(scoped_right_table, right_join_field_name)
            && joined_rows.len() <= INNER_JOIN_KEY_PROBE_MAX_LEFT_ROWS
        {

            let mut left_join_keys = HashSet::new();

            for left_row in &joined_rows {
                if let Some(left_value) = left_row.value(left_join_field_name) {
                    left_join_keys.insert(left_value.to_vec());

                    if left_join_keys.len() > INNER_JOIN_KEY_PROBE_MAX_DISTINCT_KEYS {
                        break;
                    }
                }
            }

            if !left_join_keys.is_empty() && left_join_keys.len() <= INNER_JOIN_KEY_PROBE_MAX_DISTINCT_KEYS {
                
                let use_table_stream_id = scoped_right_table.entity_id.is_empty()
                    || (wal.data_dir_path().is_none()
                        && wal
                            .latest_transaction_id_if_loaded(&scoped_right_table.entity_id)
                            .is_none()
                        && wal
                            .latest_transaction_id_if_loaded(&scoped_right_table.table_id)
                            .is_some());

                let right_table_stream_id = if use_table_stream_id {
                    scoped_right_table.table_id.as_str()
                } else {
                    scoped_right_table.entity_id.as_str()
                };

                let mut seen_right_row_ids = HashSet::new();
                let mut materialized = Vec::new();

                for lookup_key in left_join_keys {
                    
                    let rendered_lookup_key = render_stored_field_value(&lookup_key);
                    let mut probe_keys = vec![lookup_key];

                    if rendered_lookup_key != probe_keys[0] {
                        probe_keys.push(rendered_lookup_key);
                    }

                    if let Some(field) = right_schema.field(right_join_field_name)
                        && let Ok(normalized_probe_key) = convert_value_to_field_type(
                            &probe_keys[0],
                            &field.field_type,
                            TypeConversionPolicy::Safe,
                        )
                        && !probe_keys.contains(&normalized_probe_key)
                    {
                        probe_keys.push(normalized_probe_key);
                    }

                    for probe_key in probe_keys {

                        for (row_id, row_map) in load_live_rows_by_equality(
                            wal,
                            right_table_stream_id,
                            &scoped_right_table.table_id,
                            right_schema,
                            right_join_field_name,
                            &probe_key,
                        ) {
                            if !seen_right_row_ids.insert(row_id) {
                                continue;
                            }

                            if row_matches(&row_map, right_condition)? {
                                materialized.push(MaterializedRelationRow {
                                    row_id,
                                    row_map: Arc::new(row_map),
                                });
                            }
                        }
                    
                    }

                }

                log::debug!(
                    "select join right-side key probe relation={} key_field={} distinct_keys={} rows={} strategy=inner_probe",
                    join.relation.table_id,
                    right_join_field_name,
                    seen_right_row_ids.len(),
                    materialized.len(),
                );

                materialized
            } else {
                materialize_right_rows()?
            }
        } else {
            materialize_right_rows()?
        };

        if matches!(join.kind, SelectJoinKind::Cross) {
            
            let mut next_rows = Vec::new();

            for left_row in joined_rows {
                for right_row in &right_rows {
                    next_rows.push(left_row.append(&join.relation, right_row));
                }
            }

            joined_rows = next_rows;
            continue;

        }

        let probe_source = right_access_plan.equality_probe_source().unwrap_or_else(|| {
            right_field_name
                .map(|field_name| {
                    if field_has_single_column_index(&right_table, field_name) {
                        EqualityProbeSource::ExistingIndex
                    } else {
                        EqualityProbeSource::TemporaryIndex
                    }
                })
                .unwrap_or(EqualityProbeSource::TemporaryIndex)
        });

        let right_probe_index = right_field_name
            .map(|right_field_name| build_relation_probe_index(&right_rows, right_field_name));

        let right_probe_index_rendered = right_field_name
            .map(|right_field_name| {
                build_rendered_relation_probe_index(&right_rows, right_field_name)
            });

        log::debug!(
            "select join relation={} field={} strategy= {}",
            join.relation.table_id,
            right_field_name.unwrap_or("<predicate>"),
            match probe_source {
                EqualityProbeSource::ExistingIndex => "existing_index",
                EqualityProbeSource::TemporaryIndex => "temporary_index",
            },
        );

        let left_relations = &relations[..=join_index];
        let mut matched_right_ids = HashSet::new();
        let mut next_rows = Vec::new();

        for left_row in joined_rows {

            let mut matched_left = false;

            if let Some((left_join_field_name, _right_join_field_name)) = simple_join {

                let Some(left_value) = left_row.value(left_join_field_name) else {
                    continue;
                };

                let direct_matches = right_probe_index
                    .as_ref()
                    .and_then(|index| index.get(left_value));

                let rendered_left_value;
                let rendered_matches = if direct_matches.is_none() {
                    rendered_left_value = render_stored_field_value(left_value);
                    right_probe_index_rendered
                        .as_ref()
                        .and_then(|index| index.get(&rendered_left_value))
                } else {
                    None
                };

                if let Some(matches) = direct_matches.or(rendered_matches) {
                    
                    for right_row_index in matches {
                        
                        let Some(right_row) = right_rows.get(*right_row_index) else {
                            continue;
                        };

                        let provider = JoinedRowCandidateProvider {
                            left: &left_row,
                            right_relation: &join.relation,
                            right_row,
                        };

                        if join_condition_matches_provider(&provider, &join.on_condition) {
                            matched_left = true;
                            matched_right_ids.insert(right_row.row_id);
                            next_rows.push(left_row.append(&join.relation, right_row));
                        }

                    }
                    
                }

            } else {

                for right_row in &right_rows {
                    
                    let provider = JoinedRowCandidateProvider {
                        left: &left_row,
                        right_relation: &join.relation,
                        right_row,
                    };

                    if row_matches_condition_with_result(
                        &provider,
                        Some(&join.on_condition),
                        &mut |_, _| Ok(HashSet::new()),
                        &mut |_, _| Ok(false),
                        &mut |_, _| Ok(None),
                    )? {
                        matched_left = true;
                        matched_right_ids.insert(right_row.row_id);
                        next_rows.push(left_row.append(&join.relation, right_row));
                    }
                
                }

            }

            if !matched_left && matches!(join.kind, SelectJoinKind::Left | SelectJoinKind::Full) {
                next_rows.push(left_row.append_missing_relation(&join.relation));
            }

        }

        if matches!(join.kind, SelectJoinKind::Right | SelectJoinKind::Full) {

            for right_row in &right_rows {

                if matched_right_ids.contains(&right_row.row_id) {
                    continue;
                }

                next_rows.push(
                    JoinedRowTuple::from_missing_relations(left_relations)
                        .append(&join.relation, right_row),
                );

            }

        }

        joined_rows = next_rows;

    }

    Ok(joined_rows)
    
}

fn normalize_simple_join_field_orientation<'a>(
    left_field_name: &'a str,
    right_field_name: &'a str,
    right_relation: &SelectRelation,
) -> Option<(&'a str, &'a str)> {
    let right_qualifier = relation_qualifier(right_relation);

    let left_qualifier = left_field_name.split_once('.').map(|(qualifier, _)| qualifier);
    let right_qualifier_in_predicate =
        right_field_name.split_once('.').map(|(qualifier, _)| qualifier);

    let left_is_right_relation = left_qualifier.is_some_and(|qualifier| qualifier == right_qualifier);
    let right_is_right_relation = right_qualifier_in_predicate
        .is_some_and(|qualifier| qualifier == right_qualifier);

    match (left_is_right_relation, right_is_right_relation) {
        (false, true) => Some((left_field_name, right_field_name)),
        (true, false) => Some((right_field_name, left_field_name)),
        _ => None,
    }
}

fn build_rendered_relation_probe_index(
    right_rows: &[MaterializedRelationRow],
    right_field_name: &str,
) -> HashMap<Vec<u8>, Vec<usize>> {
    let mut index: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();

    for (row_index, row) in right_rows.iter().enumerate() {
        if let Some(value) = row.row_map.get(right_field_name) {
            index
                .entry(render_stored_field_value(value))
                .or_default()
                .push(row_index);
        }
    }

    index
}

fn join_field_column_name(field_name: &str) -> &str {

    field_name
        .split_once('.')
        .map(|(_, column_name)| column_name)
        .unwrap_or(field_name)
        
}


#[cfg(test)]
#[path = "join_test.rs"]
mod tests;
