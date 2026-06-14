use std::collections::HashMap;
use std::collections::HashSet;

use crate::{
    ConcurrentWalManager, DatabaseCatalog, RuntimeIndexStore, SelectCondition, SelectJoin,
    SelectJoinKind, SelectRelation,
};

use super::access::{
    build_relation_probe_index, collect_indexable_equality_filters, field_has_single_column_index,
    materialize_relation_rows, plan_relation_access, EqualityProbeSource,
};
use super::{
    join_condition_field_names, join_condition_matches_provider, JoinedRowCandidateProvider,
    JoinedRowTuple, MaterializedRelationRow,
};

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
    F: FnMut(&HashMap<String, Vec<u8>>, Option<&SelectCondition>) -> bool,
{

    let Some(primary_relation) = relations.first() else {
        return Ok(Vec::new());
    };

    let Some(primary_schema) = catalog.table_schema(&primary_relation.table_id) else {
        return Err(format!(
            "select join failed: table '{}' not found",
            primary_relation.table_id
        ));
    };

    let Some(primary_table) = catalog.table(&primary_relation.table_id) else {
        return Err(format!(
            "select join failed: table '{}' not found",
            primary_relation.table_id
        ));
    };

    let primary_condition = pushdown_conditions.first().and_then(|condition| condition.as_ref());
    let mut primary_filter_map = HashMap::new();
    let primary_allow_index_short_circuit = primary_condition
        .as_ref()
        .map(|condition| collect_indexable_equality_filters(condition, &mut primary_filter_map))
        .unwrap_or(true);

    let primary_access_plan = plan_relation_access(
        primary_table,
        primary_allow_index_short_circuit,
        primary_filter_map,
    );

    let mut joined_rows = materialize_relation_rows(
        wal,
        primary_table,
        primary_schema,
        runtime_indexes,
        &primary_access_plan,
    )
    .into_iter()
    .filter(|(_, row_map)| row_matches(row_map, primary_condition))
    .map(|(row_id, row_map)| {
        JoinedRowTuple::from_relation_row(
            primary_relation,
            MaterializedRelationRow { row_id, row_map },
        )
    })
    .collect::<Vec<_>>();

    for (join_index, join) in joins.iter().enumerate() {

        let Some(right_schema) = catalog.table_schema(&join.relation.table_id) else {
            return Err(format!(
                "select join failed: table '{}' not found",
                join.relation.table_id
            ));
        };

        let Some(right_table) = catalog.table(&join.relation.table_id) else {
            return Err(format!(
                "select join failed: table '{}' not found",
                join.relation.table_id
            ));
        };

        let Some((left_join_field_name, right_join_field_name)) = join_condition_field_names(join)
        else {
            return Err("select join failed: unsupported join ON condition".to_string());
        };

        let right_field_name = join_field_column_name(right_join_field_name);
        let right_condition = pushdown_conditions
            .get(join_index + 1)
            .and_then(|condition| condition.as_ref());
        let mut right_filter_map = HashMap::new();
        let right_allow_index_short_circuit = right_condition
            .as_ref()
            .map(|condition| collect_indexable_equality_filters(condition, &mut right_filter_map))
            .unwrap_or(true);

        let right_access_plan = plan_relation_access(
            right_table,
            right_allow_index_short_circuit,
            right_filter_map,
        );
        let probe_source = right_access_plan.equality_probe_source().unwrap_or_else(|| {
            if field_has_single_column_index(right_table, right_field_name) {
                EqualityProbeSource::ExistingIndex
            } else {
                EqualityProbeSource::TemporaryIndex
            }
        });

        let right_rows = materialize_relation_rows(
            wal,
            right_table,
            right_schema,
            runtime_indexes,
            &right_access_plan,
        )
        .into_iter()
        .filter(|(_, row_map)| row_matches(row_map, right_condition))
        .map(|(row_id, row_map)| MaterializedRelationRow { row_id, row_map })
        .collect::<Vec<_>>();

        let right_probe_index = build_relation_probe_index(&right_rows, right_field_name);

        log::debug!(
            "select join relation={} field={} strategy= {}",
            join.relation.table_id,
            right_field_name,
            match probe_source {
                EqualityProbeSource::ExistingIndex => "existing_index",
                EqualityProbeSource::TemporaryIndex => "temporary_index",
            },
        );

        let left_relations = &relations[..=join_index];
        let mut matched_right_ids = HashSet::new();
        let mut next_rows = Vec::new();

        for left_row in joined_rows {

            let Some(left_value) = left_row.value(left_join_field_name) else {
                continue;
            };

            if let Some(matches) = right_probe_index.get(left_value) {
                for right_row in matches {
                    let provider = JoinedRowCandidateProvider {
                        left: &left_row,
                        right_relation: &join.relation,
                        right_row,
                    };

                    if join_condition_matches_provider(&provider, &join.on_condition) {
                        matched_right_ids.insert(right_row.row_id);
                        next_rows.push(left_row.append(&join.relation, right_row));
                    }
                }
            } else if matches!(join.kind, SelectJoinKind::Left | SelectJoinKind::Full) {
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

fn join_field_column_name(field_name: &str) -> &str {
    field_name
        .split_once('.')
        .map(|(_, column_name)| column_name)
        .unwrap_or(field_name)
}


#[cfg(test)]
#[path = "join_test.rs"]
mod tests;
