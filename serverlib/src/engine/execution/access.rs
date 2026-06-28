use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use crate::engine::database::transaction::TransactionLog;
use crate::engine::database::runtime_index::derived_indexes_for_table;
use crate::engine::database::schema_migration::{convert_value_to_field_type, TypeConversionPolicy};
use crate::{
    TransactionPayloadContext,
    decode_row_payload, ConcurrentWalManager, DatabaseIndex, DatabaseTable, RuntimeIndexStore,
    SelectComparisonOp, SelectCondition, SelectPredicate, TableSchema, TransactionKind,
};

use super::MaterializedRelationRow;

static LIVE_ROW_COUNT_CACHE: OnceLock<Mutex<HashMap<(usize, String), (u64, usize)>>> =
    OnceLock::new();

#[derive(Debug, Default)]
struct EqualityTableCacheEntry {
    latest_tx_id: u64,
    rows_by_id: HashMap<u64, HashMap<String, Vec<u8>>>,
    row_ids_by_field_value: HashMap<String, HashMap<Vec<u8>, Vec<u64>>>,
}

static EQUALITY_TABLE_CACHE: OnceLock<Mutex<HashMap<(usize, String), EqualityTableCacheEntry>>> =
    OnceLock::new();

fn build_postings_for_field(
    rows_by_id: &HashMap<u64, HashMap<String, Vec<u8>>>,
    field_name: &str,
) -> HashMap<Vec<u8>, Vec<u64>> {
    let mut row_ids_by_value = HashMap::<Vec<u8>, Vec<u64>>::new();

    for (row_id, row_map) in rows_by_id {
        if let Some(value) = row_map.get(field_name).cloned() {
            row_ids_by_value.entry(value).or_default().push(*row_id);
        }
    }

    row_ids_by_value
}

fn rows_for_field_value(
    entry: &EqualityTableCacheEntry,
    field_name: &str,
    lookup_value: &[u8],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {
    entry
        .row_ids_by_field_value
        .get(field_name)
        .and_then(|row_ids_by_value| row_ids_by_value.get(lookup_value).cloned())
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row_id| {
            entry
                .rows_by_id
                .get(&row_id)
                .cloned()
                .map(|row_map| (row_id, row_map))
        })
        .collect()
}

pub fn warm_equality_cache_from_live_rows(
    cache_scope_id: usize,
    table_id: &str,
    latest_tx_id: u64,
    live_rows: &[(u64, HashMap<String, Vec<u8>>) ],
    field_names: &[String],
) {
    if field_names.is_empty() {
        return;
    }

    let mut rows_by_id = HashMap::with_capacity(live_rows.len());
    for (row_id, row_map) in live_rows {
        rows_by_id.insert(*row_id, row_map.clone());
    }

    let mut row_ids_by_field_value = HashMap::<String, HashMap<Vec<u8>, Vec<u64>>>::new();
    for field_name in field_names {
        if field_name.is_empty() {
            continue;
        }
        row_ids_by_field_value.insert(
            field_name.clone(),
            build_postings_for_field(&rows_by_id, field_name),
        );
    }

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut cache_guard) = cache.lock() {
        cache_guard.insert(
            (cache_scope_id, table_id.to_string()),
            EqualityTableCacheEntry {
                latest_tx_id,
                rows_by_id,
                row_ids_by_field_value,
            },
        );
    }
}

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

pub fn collect_indexable_equality_filters_for_schema(
    schema: &TableSchema,
    condition: &SelectCondition,
    filters: &mut HashMap<String, Vec<u8>>,
) -> bool {

    match condition {
        SelectCondition::And(children) => children
            .iter()
            .all(|child| collect_indexable_equality_filters_for_schema(schema, child, filters)),

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op: SelectComparisonOp::Eq,
            value,
        }) => {
            let resolved_field_name = if schema.field(field_name).is_some() {
                field_name.clone()
            } else {
                field_name
                    .rsplit('.')
                    .next()
                    .filter(|candidate| schema.field(candidate).is_some())
                    .map(str::to_string)
                    .unwrap_or_else(|| field_name.clone())
            };

            let normalized_value = schema
                .field(&resolved_field_name)
                .and_then(|field| {
                    convert_value_to_field_type(
                        value,
                        &field.field_type,
                        TypeConversionPolicy::Safe,
                    )
                    .ok()
                })
                .unwrap_or_else(|| value.clone());

            filters.insert(resolved_field_name, normalized_value);
            true
        }

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) | SelectCondition::Not(_) => false,

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
    let context = TransactionPayloadContext::default();
    load_live_rows_with_context(wal, table_id, schema, &context).unwrap_or_default()
}

pub fn load_live_rows_with_context(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
    context: &TransactionPayloadContext,
) -> Result<Vec<(u64, HashMap<String, Vec<u8>>)>, String> {

    let wal_records = wal
        .since_with_context(table_id, None, context)
        .map_err(str::to_string)?;
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
                let Some(payload) = record.payload_logical() else {
                    continue;
                };

                match decode_row_payload(schema, payload) {
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

    let rows = row_order
        .into_iter()
        .filter_map(|id| live_rows.remove(&id).map(|row_map| (id, row_map)))
        .collect::<Vec<_>>();

    Ok(rows)

}

pub fn load_live_rows_by_equality(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    lookup_value: &[u8],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let cache_scope_id = wal.cache_scope_id();
    let latest_tx_id = wal
        .latest_transaction_id(table_id)
        .map(|tx| tx.0)
        .unwrap_or(0);
    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = cache_guard.get_mut(&(cache_scope_id, table_id.to_string()))
        && entry.latest_tx_id == latest_tx_id {
            if !entry.row_ids_by_field_value.contains_key(field_name) {
                entry.row_ids_by_field_value.insert(
                    field_name.to_string(),
                    build_postings_for_field(&entry.rows_by_id, field_name),
                );
            }

            return rows_for_field_value(entry, field_name, lookup_value);
        }

    let live_rows = load_live_rows(wal, table_id, schema);
    let mut rows_by_id = HashMap::with_capacity(live_rows.len());
    for (row_id, row_map) in live_rows {
        rows_by_id.insert(row_id, row_map);
    }

    let mut row_ids_by_field_value = HashMap::<String, HashMap<Vec<u8>, Vec<u64>>>::new();
    row_ids_by_field_value.insert(
        field_name.to_string(),
        build_postings_for_field(&rows_by_id, field_name),
    );

    let entry = EqualityTableCacheEntry {
        latest_tx_id,
        rows_by_id,
        row_ids_by_field_value,
    };

    let result = rows_for_field_value(&entry, field_name, lookup_value);

    if let Ok(mut cache_guard) = cache.lock() {
        cache_guard.insert((cache_scope_id, table_id.to_string()), entry);
    }

    result

}

pub fn load_live_row_count(
    wal: &ConcurrentWalManager,
    table_id: &str,
) -> usize {

    let cache_scope_id = wal.cache_scope_id();
    let latest_tx_id = wal
        .latest_transaction_id(table_id)
        .map(|tx| tx.0)
        .unwrap_or(0);

    let cache = LIVE_ROW_COUNT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(cache_guard) = cache.lock()
        && let Some((cached_latest_tx_id, cached_count)) = cache_guard.get(&(cache_scope_id, table_id.to_string()))
        && *cached_latest_tx_id == latest_tx_id {
            return *cached_count;
        }

    let wal_records = wal.since(table_id, None);
    let mut live_row_ids = HashSet::new();
    let mut committed_groups = HashSet::new();
    let mut aborted_groups = HashSet::new();

    for record in &wal_records {

        match record.kind {

            TransactionKind::WriteCommit => {
                if let Some(group_id) = record.groupid {
                    committed_groups.insert(group_id.0);
                }
            },

            TransactionKind::WriteAbort => {
                if let Some(group_id) = record.groupid {
                    aborted_groups.insert(group_id.0);
                }
            },

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

            TransactionKind::Insert | TransactionKind::Update => {
                live_row_ids.insert(record.id.0);
            },

            TransactionKind::Delete => {
                if let Some(refid) = record.refid {
                    live_row_ids.remove(&refid.0);
                }
            },
            
            _ => {}

        }

    }

    let count = live_row_ids.len();

    if let Ok(mut cache_guard) = cache.lock() {
        cache_guard.insert((cache_scope_id, table_id.to_string()), (latest_tx_id, count));
    }

    count

}

pub fn plan_relation_access(
    table: &DatabaseTable,
    allow_index_short_circuit: bool,
    index_filter_map: HashMap<String, Vec<u8>>,
) -> RelationAccessPlan {

    if allow_index_short_circuit
        && let Some((index, lookup_key)) = choose_index_lookup(table, &index_filter_map) {
            return RelationAccessPlan {
                strategy: RelationAccessStrategy::RuntimeIndexLookup {
                    index_id: index.index_id.0.clone(),
                    lookup_key,
                },
            };
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
                if state.cardinality() == 0 {
                    return load_live_rows(wal, &table.table_id, schema);
                }

                if !state.contains(lookup_key) {
                    return load_live_rows(wal, &table.table_id, schema);
                }
                }

            if lookup_key.len() == 1
                && let Some(index) = table.indexes.values().find(|index| index.index_id.0 == *index_id) {
                    
                    let field_names = if index.field_names.is_empty() && !index.field_name.is_empty() {
                        vec![index.field_name.clone()]
                    } else {
                        index.field_names.clone()
                    };

                    if field_names.len() == 1 {
                        return load_live_rows_by_equality(
                            wal,
                            &table.table_id,
                            schema,
                            &field_names[0],
                            &lookup_key[0],
                        );
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

            load_live_rows_by_equality(
                wal,
                &table.table_id,
                schema,
                field_name,
                lookup_value,
            )
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
