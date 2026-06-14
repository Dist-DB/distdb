use std::collections::{HashMap, HashSet};

use crate::engine::database::transaction::TransactionLog;
use crate::engine::database::runtime_index::derived_indexes_for_table;
use crate::{
    decode_row_payload, ConcurrentWalManager, DatabaseIndex, DatabaseTable, RuntimeIndexStore,
    SelectComparisonOp, SelectCondition, SelectPredicate, TableSchema, TransactionKind,
};

use super::MaterializedRelationRow;

#[derive(Debug, Clone)]
pub enum RelationAccessStrategy {
    FullScan,
    RuntimeIndexLookup {
        index_id: String,
        lookup_key: Vec<Vec<u8>>,
    },
    EqualityProbe {
        field_name: String,
        lookup_value: Vec<u8>,
        source: EqualityProbeSource,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum EqualityProbeSource {
    ExistingIndex,
    TemporaryIndex,
}

#[derive(Debug, Clone)]
pub struct RelationAccessPlan {
    pub strategy: RelationAccessStrategy,
}

impl RelationAccessPlan {
    
    pub fn runtime_index_lookup<'a>(
        &'a self,
        table: &'a DatabaseTable,
    ) -> Option<(&'a DatabaseIndex, Vec<Vec<u8>>)> {
        let RelationAccessStrategy::RuntimeIndexLookup {
            index_id,
            lookup_key,
        } = &self.strategy else {
            return None;
        };

        table.indexes
            .values()
            .find(|index| index.index_id.0 == *index_id)
            .map(|index| (index, lookup_key.clone()))
    }

    pub fn equality_probe_source(&self) -> Option<EqualityProbeSource> {
        let RelationAccessStrategy::EqualityProbe { source, .. } = self.strategy else {
            return None;
        };

        Some(source)
    }

}

pub fn field_has_single_column_index(table: &DatabaseTable, field_name: &str) -> bool {
    table.indexes.values().any(|index| {
        let field_names = if index.field_names.is_empty() && !index.field_name.is_empty() {
            vec![index.field_name.clone()]
        } else {
            index.field_names.clone()
        };

        field_names.len() == 1 && field_names[0] == field_name
    })
}

pub fn build_relation_probe_index(
    rows: &[MaterializedRelationRow],
    field_name: &str,
) -> HashMap<Vec<u8>, Vec<MaterializedRelationRow>> {

    let mut probe_index = HashMap::new();

    for row in rows {
        if let Some(value) = row.row_map.get(field_name) {
            probe_index
                .entry(value.clone())
                .or_insert_with(Vec::new)
                .push(row.clone());
        }
    }

    probe_index
}

pub fn load_live_rows(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let wal_records = wal.since(table_id, None);
    let mut live_rows = HashMap::new();
    let mut row_order = Vec::new();
    let mut committed_groups = HashSet::new();
    let mut aborted_groups = HashSet::new();

    for record in &wal_records {
        match record.kind {
            TransactionKind::WriteCommit => {
                if let Some(group_id) = record.groupid {
                    committed_groups.insert(group_id.0);
                }
            }
            TransactionKind::WriteAbort => {
                if let Some(group_id) = record.groupid {
                    aborted_groups.insert(group_id.0);
                }
            }
            _ => {}
        }
    }

    for record in &wal_records {
        if let Some(group_id) = record.groupid {
            let group_id = group_id.0;
            if aborted_groups.contains(&group_id) {
                continue;
            }

            if !committed_groups.contains(&group_id)
                && !matches!(record.kind, TransactionKind::WriteCommit | TransactionKind::WriteAbort)
            {
                continue;
            }
        }

        match record.kind {

            TransactionKind::Ignore => {}

            TransactionKind::Insert | TransactionKind::Update => {
                match decode_row_payload(schema, &record.payload) {
                Ok(row_map) => {
                    row_order.push(record.id.0);
                    live_rows.insert(record.id.0, row_map);
                }
                Err(_) => continue,
                }
            },

            TransactionKind::Delete => {
                if let Some(refid) = record.refid {
                    live_rows.remove(&refid.0);
                }
            },

            _ => {}

        }
    }

    row_order
        .into_iter()
        .filter_map(|id| live_rows.remove(&id).map(|row_map| (id, row_map)))
        .collect()

}

pub fn plan_relation_access(
    table: &DatabaseTable,
    allow_index_short_circuit: bool,
    index_filter_map: HashMap<String, Vec<u8>>,
) -> RelationAccessPlan {

    if allow_index_short_circuit {
        if let Some((index, lookup_key)) = choose_index_lookup(table, &index_filter_map) {
            return RelationAccessPlan {
                strategy: RelationAccessStrategy::RuntimeIndexLookup {
                    index_id: index.index_id.0.clone(),
                    lookup_key,
                },
            };
        }
    }

    if index_filter_map.len() == 1 {

        let (field_name, lookup_value) = index_filter_map
            .into_iter()
            .next()
            .expect("single entry should exist");

        let source = if field_has_single_column_index(table, &field_name) {
            EqualityProbeSource::ExistingIndex
        } else {
            EqualityProbeSource::TemporaryIndex
        };

        return RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name,
                lookup_value,
                source,
            },
        };
    }

    RelationAccessPlan {
        strategy: RelationAccessStrategy::FullScan,
    }

}

pub fn materialize_relation_rows(
    wal: &ConcurrentWalManager,
    table: &DatabaseTable,
    schema: &TableSchema,
    runtime_indexes: &RuntimeIndexStore,
    access_plan: &RelationAccessPlan,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {
    
    match &access_plan.strategy {

        RelationAccessStrategy::RuntimeIndexLookup {
            index_id,
            lookup_key,
        } => {
            if let Some(state) = runtime_indexes.index(index_id) {
                if state.cardinality() > 0 && !state.contains(lookup_key) {
                    return Vec::new();
                }
            }

            load_live_rows(wal, &table.table_id, schema)
        },

        RelationAccessStrategy::EqualityProbe {
            field_name,
            lookup_value,
            source,
        } => {
            log::debug!(
                "relation access table={} field={} strategy={}",
                table.table_id,
                field_name,
                match source {
                    EqualityProbeSource::ExistingIndex => "existing_index",
                    EqualityProbeSource::TemporaryIndex => "temporary_index",
                }
            );

            let live_rows = load_live_rows(wal, &table.table_id, schema);
            let materialized_rows = live_rows
                .into_iter()
                .map(|(row_id, row_map)| MaterializedRelationRow { row_id, row_map })
                .collect::<Vec<_>>();
            let probe_index = build_relation_probe_index(&materialized_rows, field_name);

            probe_index
                .get(lookup_value)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|row| (row.row_id, row.row_map))
                .collect()
        },

        RelationAccessStrategy::FullScan => load_live_rows(wal, &table.table_id, schema),
    
    }

}

pub fn collect_indexable_equality_filters(
    condition: &SelectCondition,
    filters: &mut HashMap<String, Vec<u8>>,
) -> bool {

    match condition {
        SelectCondition::And(children) => children
            .iter()
            .all(|child| collect_indexable_equality_filters(child, filters)),

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op: SelectComparisonOp::Eq,
            value,
        }) => {
            filters.insert(field_name.clone(), value.clone());
            true
        },

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) | SelectCondition::Not(_) => false,

    }

}

pub fn count_condition_predicates(condition: &SelectCondition) -> usize {
    match condition {
        SelectCondition::And(children) | SelectCondition::Or(children) => {
            children.iter().map(count_condition_predicates).sum()
        },
        SelectCondition::Not(child) => count_condition_predicates(child),
        SelectCondition::Predicate(_) => 1,
    }
}

pub fn choose_index_lookup<'a>(
    table: &'a DatabaseTable,
    filters: &HashMap<String, Vec<u8>>,
) -> Option<(&'a DatabaseIndex, Vec<Vec<u8>>)> {

    let mut selected: Option<(&DatabaseIndex, Vec<Vec<u8>>, usize)> = None;

    for index in derived_indexes_for_table(table) {

        let field_names = if index.field_names.is_empty() && !index.field_name.is_empty() {
            vec![index.field_name.clone()]
        } else {
            index.field_names.clone()
        };

        if field_names.is_empty() {
            continue;
        }

        let mut lookup_key = Vec::with_capacity(field_names.len());
        let mut all_present = true;
        for field_name in &field_names {
            match filters.get(field_name) {
                Some(value) => lookup_key.push(value.clone()),
                None => {
                    all_present = false;
                    break;
                }
            }
        }

        if !all_present {
            continue;
        }

        let score = field_names.len();
        let should_replace = selected
            .as_ref()
            .map(|(_, _, best_score)| score > *best_score)
            .unwrap_or(true);
        
        if should_replace {
            selected = Some((index, lookup_key, score));
        }

    }

    selected.map(|(index, lookup_key, _)| (index, lookup_key))

}


#[cfg(test)]
#[path = "access_test.rs"]
mod tests;
