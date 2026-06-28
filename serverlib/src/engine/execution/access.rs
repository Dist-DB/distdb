use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use ahash::{AHashMap, AHashSet};

use crate::engine::database::transaction::TransactionLog;
use crate::engine::database::runtime_index::derived_indexes_for_table;
use crate::engine::database::schema_migration::{convert_value_to_field_type, TypeConversionPolicy};
use crate::{
    TransactionPayloadContext,
    decode_row_payload, ConcurrentWalManager, DatabaseIndex, DatabaseTable, RuntimeIndexStore,
    SelectComparisonOp, SelectCondition, SelectPredicate, TableSchema, TransactionKind,
    TransactionRecord,
};

use super::MaterializedRelationRow;

static LIVE_ROW_COUNT_CACHE: OnceLock<Mutex<HashMap<(usize, String), (u64, usize)>>> =
    OnceLock::new();

#[derive(Debug, Default)]
struct EqualityTableCacheEntry {
    latest_tx_id: u64,
    rows_by_id: AHashMap<u64, HashMap<String, Vec<u8>>>,
    row_ids_by_field_value: AHashMap<String, AHashMap<Vec<u8>, Vec<u64>>>,
}

static EQUALITY_TABLE_CACHE: OnceLock<Mutex<AHashMap<(usize, String), EqualityTableCacheEntry>>> =
    OnceLock::new();

const LIVE_ROW_APPLY_PARALLEL_MIN_RECORDS: usize = 500_000;
const LIVE_ROW_APPLY_PARALLEL_CHUNK_SIZE: usize = 200_000;
const LIVE_ROW_APPLY_PARALLEL_MAX_WORKERS: usize = 32;
const EQUALITY_WARM_PARALLEL_MIN_ROWS: usize = 250_000;
const EQUALITY_WARM_PARALLEL_MAX_WORKERS: usize = 32;

fn live_row_apply_max_workers() -> usize {
    std::env::var("DISTDB_LIVE_ROW_APPLY_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(LIVE_ROW_APPLY_PARALLEL_MAX_WORKERS)
}

fn equality_warm_max_workers() -> usize {
    std::env::var("DISTDB_RUNTIME_INDEX_WARM_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(EQUALITY_WARM_PARALLEL_MAX_WORKERS)
}

#[inline]
fn record_visible_for_live_row_apply(
    record: &TransactionRecord,
    committed_groups: &AHashSet<u64>,
    aborted_groups: &AHashSet<u64>,
) -> bool {
    if let Some(group_id) = record.groupid {
        let group_id = group_id.0;

        if aborted_groups.contains(&group_id) {
            return false;
        }

        if !committed_groups.contains(&group_id)
            && !matches!(record.kind, TransactionKind::WriteCommit | TransactionKind::WriteAbort)
        {
            return false;
        }
    }

    true
}

fn decode_live_row_chunk(
    chunk: &[TransactionRecord],
    schema: &TableSchema,
    committed_groups: &AHashSet<u64>,
    aborted_groups: &AHashSet<u64>,
    workers: usize,
) -> Vec<Option<HashMap<String, Vec<u8>>>> {
    if workers <= 1 || chunk.len() < 2 {
        let mut decoded = vec![None; chunk.len()];
        for (idx, record) in chunk.iter().enumerate() {
            if !record_visible_for_live_row_apply(record, committed_groups, aborted_groups) {
                continue;
            }

            if matches!(record.kind, TransactionKind::Insert | TransactionKind::Update)
                && let Some(payload) = record.payload_logical()
                && let Ok(row_map) = decode_row_payload(schema, payload)
            {
                decoded[idx] = Some(row_map);
            }
        }

        return decoded;
    }

    let chunk_size = chunk.len().div_ceil(workers);

    let partials = std::thread::scope(|scope| {
        let mut handles = Vec::new();

        for worker_idx in 0..workers {
            let start = worker_idx * chunk_size;
            if start >= chunk.len() {
                break;
            }

            let end = std::cmp::min(start + chunk_size, chunk.len());
            let sub_chunk = &chunk[start..end];

            handles.push(scope.spawn(move || {
                let mut local = Vec::new();

                for (offset, record) in sub_chunk.iter().enumerate() {
                    if !record_visible_for_live_row_apply(record, committed_groups, aborted_groups) {
                        continue;
                    }

                    if matches!(record.kind, TransactionKind::Insert | TransactionKind::Update)
                        && let Some(payload) = record.payload_logical()
                        && let Ok(row_map) = decode_row_payload(schema, payload)
                    {
                        local.push((start + offset, row_map));
                    }
                }

                local
            }));
        }

        let mut all = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(local) = handle.join() {
                all.push(local);
            }
        }
        all
    });

    let mut decoded = vec![None; chunk.len()];
    for local in partials {
        for (idx, row_map) in local {
            decoded[idx] = Some(row_map);
        }
    }

    decoded
}

fn build_postings_for_field(
    rows_by_id: &AHashMap<u64, HashMap<String, Vec<u8>>>,
    field_name: &str,
) -> AHashMap<Vec<u8>, Vec<u64>> {
    let mut row_ids_by_value = AHashMap::<Vec<u8>, Vec<u64>>::new();

    for (row_id, row_map) in rows_by_id {
        if let Some(value) = row_map.get(field_name).cloned() {
            row_ids_by_value.entry(value).or_default().push(*row_id);
        }
    }

    row_ids_by_value
}

fn normalize_distinct_field_names(field_names: &[String]) -> Vec<String> {
    let mut fields = Vec::new();
    let mut seen = AHashSet::with_capacity(field_names.len());

    for field_name in field_names {
        if field_name.is_empty() {
            continue;
        }

        if seen.insert(field_name.clone()) {
            fields.push(field_name.clone());
        }
    }

    fields
}

fn build_warm_equality_cache_serial(
    fields: &[String],
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
) -> EqualityTableCacheEntry {
    let mut rows_by_id = AHashMap::with_capacity(live_rows.len());
    let mut postings_by_field = (0..fields.len())
        .map(|_| AHashMap::<Vec<u8>, Vec<u64>>::new())
        .collect::<Vec<_>>();

    for (row_id, row_map) in live_rows {
        for (field_idx, field_name) in fields.iter().enumerate() {
            if let Some(value) = row_map.get(field_name) {
                postings_by_field[field_idx]
                    .entry(value.clone())
                    .or_default()
                    .push(row_id);
            }
        }

        rows_by_id.insert(row_id, row_map);
    }

    let row_ids_by_field_value = fields
        .iter()
        .cloned()
        .zip(postings_by_field)
        .collect::<AHashMap<_, _>>();

    EqualityTableCacheEntry {
        latest_tx_id: 0,
        rows_by_id,
        row_ids_by_field_value,
    }
}

fn build_warm_equality_cache_parallel(
    fields: &[String],
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
    workers: usize,
) -> EqualityTableCacheEntry {
    let live_row_count = live_rows.len();
    let chunk_size = live_row_count.div_ceil(workers);
    let mut chunks = Vec::with_capacity(workers);
    let mut iter = live_rows.into_iter();

    loop {
        let mut chunk = Vec::with_capacity(chunk_size);
        for _ in 0..chunk_size {
            let Some(row) = iter.next() else {
                break;
            };
            chunk.push(row);
        }

        if chunk.is_empty() {
            break;
        }

        chunks.push(chunk);
    }

    let partials = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            handles.push(scope.spawn(move || {
                let mut local_rows_by_id = AHashMap::with_capacity(chunk.len());
                let mut local_postings_by_field = (0..fields.len())
                    .map(|_| AHashMap::<Vec<u8>, Vec<u64>>::new())
                    .collect::<Vec<_>>();

                for (row_id, row_map) in chunk {
                    for (field_idx, field_name) in fields.iter().enumerate() {
                        if let Some(value) = row_map.get(field_name) {
                            local_postings_by_field[field_idx]
                                .entry(value.clone())
                                .or_default()
                                .push(row_id);
                        }
                    }

                    local_rows_by_id.insert(row_id, row_map);
                }

                (local_rows_by_id, local_postings_by_field)
            }));
        }

        let mut out = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(partial) = handle.join() {
                out.push(partial);
            }
        }
        out
    });

    let mut rows_by_id = AHashMap::with_capacity(live_row_count);
    let mut postings_by_field = (0..fields.len())
        .map(|_| AHashMap::<Vec<u8>, Vec<u64>>::new())
        .collect::<Vec<_>>();

    for (local_rows_by_id, local_postings_by_field) in partials {
        rows_by_id.extend(local_rows_by_id);

        for (field_idx, mut local_postings) in local_postings_by_field.into_iter().enumerate() {
            let global_postings = &mut postings_by_field[field_idx];
            for (value, mut row_ids) in local_postings.drain() {
                global_postings.entry(value).or_default().append(&mut row_ids);
            }
        }
    }

    let row_ids_by_field_value = fields
        .iter()
        .cloned()
        .zip(postings_by_field)
        .collect::<AHashMap<_, _>>();

    EqualityTableCacheEntry {
        latest_tx_id: 0,
        rows_by_id,
        row_ids_by_field_value,
    }
}

fn rows_for_field_value(
    entry: &EqualityTableCacheEntry,
    field_name: &str,
    lookup_value: &[u8],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {
    let Some(row_ids_by_value) = entry.row_ids_by_field_value.get(field_name) else {
        return Vec::new();
    };

    let Some(row_ids) = row_ids_by_value.get(lookup_value) else {
        return Vec::new();
    };

    row_ids
        .iter()
        .filter_map(|row_id| {
            entry
                .rows_by_id
                .get(row_id)
                .cloned()
                .map(|row_map| (*row_id, row_map))
        })
        .collect()
}

pub fn warm_equality_cache_from_live_rows(
    cache_scope_id: usize,
    table_id: &str,
    latest_tx_id: u64,
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
    field_names: &[String],
) {
    
    if field_names.is_empty() {
        return;
    }

    let fields = normalize_distinct_field_names(field_names);
    if fields.is_empty() {
        return;
    }

    let available_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let warm_workers = std::cmp::min(available_workers, equality_warm_max_workers());

    let mut entry = if warm_workers > 1 && live_rows.len() >= EQUALITY_WARM_PARALLEL_MIN_ROWS {
        build_warm_equality_cache_parallel(&fields, live_rows, warm_workers)
    } else {
        build_warm_equality_cache_serial(&fields, live_rows)
    };

    entry.latest_tx_id = latest_tx_id;

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));
    if let Ok(mut cache_guard) = cache.lock() {
        cache_guard.insert(
            (cache_scope_id, table_id.to_string()),
            entry,
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
    ) -> Option<(&'a DatabaseIndex, &'a [Vec<u8>])> {

        let RelationAccessStrategy::RuntimeIndexLookup {
            index_id,
            lookup_key,
        } = &self.strategy else {
            return None;
        };

        table.indexes
            .values()
            .find(|index| index.index_id.0 == *index_id)
            .map(|index| (index, lookup_key.as_slice()))
    
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
        if !index.field_names.is_empty() {
            index.field_names.len() == 1 && index.field_names[0] == field_name
        } else {
            !index.field_name.is_empty() && index.field_name == field_name
        }
    })
}

pub fn build_relation_probe_index(
    rows: &[MaterializedRelationRow],
    field_name: &str,
) -> HashMap<Vec<u8>, Vec<usize>> {

    let mut probe_index = HashMap::new();

    for (index, row) in rows.iter().enumerate() {
        if let Some(value) = row.row_map.get(field_name) {
            probe_index
                .entry(value.clone())
                .or_insert_with(Vec::new)
                .push(index);
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

pub fn load_live_rows_in_place(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let started_at = Instant::now();
    let wal_fetch_started_at = Instant::now();

    wal.with_records(table_id, |records| {
        let wal_fetch_elapsed_ms = wal_fetch_started_at.elapsed().as_millis();
        collect_live_rows_from_records(
            table_id,
            schema,
            records,
            wal_fetch_elapsed_ms,
            started_at,
        )
    })
    .unwrap_or_default()

}

fn collect_live_rows_from_records(
    table_id: &str,
    schema: &TableSchema,
    wal_records: &[TransactionRecord],
    wal_fetch_elapsed_ms: u128,
    started_at: Instant,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let mut live_rows = AHashMap::with_capacity(wal_records.len());
    let mut row_order = Vec::with_capacity(wal_records.len());
    let mut committed_groups = AHashSet::with_capacity(wal_records.len() / 8 + 1);
    let mut aborted_groups = AHashSet::with_capacity(wal_records.len() / 8 + 1);

    let group_scan_started_at = Instant::now();

    for record in wal_records {
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

    let group_scan_elapsed_ms = group_scan_started_at.elapsed().as_millis();

    let apply_started_at = Instant::now();

    let available_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let apply_workers = std::cmp::min(available_workers, live_row_apply_max_workers());
    let should_parallel_apply =
        apply_workers > 1 && wal_records.len() >= LIVE_ROW_APPLY_PARALLEL_MIN_RECORDS;

    if should_parallel_apply {
        for chunk in wal_records.chunks(LIVE_ROW_APPLY_PARALLEL_CHUNK_SIZE) {
            let mut decoded_chunk = decode_live_row_chunk(
                chunk,
                schema,
                &committed_groups,
                &aborted_groups,
                apply_workers,
            );

            for (offset, record) in chunk.iter().enumerate() {
                if !record_visible_for_live_row_apply(record, &committed_groups, &aborted_groups) {
                    continue;
                }

                match record.kind {
                    TransactionKind::Ignore => {}

                    TransactionKind::Insert | TransactionKind::Update => {
                        if let Some(row_map) = decoded_chunk[offset].take() {
                            row_order.push(record.id.0);
                            live_rows.insert(record.id.0, row_map);
                        }
                    }

                    TransactionKind::Delete => {
                        if let Some(refid) = record.refid {
                            live_rows.remove(&refid.0);
                        }
                    }

                    _ => {}
                }
            }
        }
    } else {
        for record in wal_records {
            if !record_visible_for_live_row_apply(record, &committed_groups, &aborted_groups) {
                continue;
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
                }

                TransactionKind::Delete => {
                    if let Some(refid) = record.refid {
                        live_rows.remove(&refid.0);
                    }
                }

                _ => {}
            }
        }
    }

    let apply_elapsed_ms = apply_started_at.elapsed().as_millis();

    let finalize_started_at = Instant::now();
    let rows = row_order
        .into_iter()
        .filter_map(|id| live_rows.remove(&id).map(|row_map| (id, row_map)))
        .collect::<Vec<_>>();

    let finalize_elapsed_ms = finalize_started_at.elapsed().as_millis();

    let total_elapsed_ms = started_at.elapsed().as_millis();

    if total_elapsed_ms >= 1_000 {
        log::info!(
            "live row load timing table={} wal_records={} live_rows={} wal_fetch_ms={} group_scan_ms={} apply_ms={} finalize_ms={} total_ms={}",
            table_id,
            wal_records.len(),
            rows.len(),
            wal_fetch_elapsed_ms,
            group_scan_elapsed_ms,
            apply_elapsed_ms,
            finalize_elapsed_ms,
            total_elapsed_ms,
        );
    }

    rows
}

pub fn load_live_rows_with_context(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
    context: &TransactionPayloadContext,
) -> Result<Vec<(u64, HashMap<String, Vec<u8>>)>, String> {

    let started_at = Instant::now();

    let wal_fetch_started_at = Instant::now();
    let wal_records = wal
        .since_with_context(table_id, None, context)
        .map_err(str::to_string)?;
    let wal_fetch_elapsed_ms = wal_fetch_started_at.elapsed().as_millis();

    Ok(collect_live_rows_from_records(
        table_id,
        schema,
        &wal_records,
        wal_fetch_elapsed_ms,
        started_at,
    ))

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

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = cache_guard.get_mut(&(cache_scope_id, table_id.to_string()))
        && entry.latest_tx_id == latest_tx_id
    {
        if !entry.row_ids_by_field_value.contains_key(field_name) {
            entry.row_ids_by_field_value.insert(
                field_name.to_string(),
                build_postings_for_field(&entry.rows_by_id, field_name),
            );
        }

        return rows_for_field_value(entry, field_name, lookup_value);
    }

    let live_rows = load_live_rows(wal, table_id, schema);
    let mut rows_by_id = AHashMap::with_capacity(live_rows.len());

    for (row_id, row_map) in live_rows {
        rows_by_id.insert(row_id, row_map);
    }

    let mut row_ids_by_field_value = AHashMap::<String, AHashMap<Vec<u8>, Vec<u64>>>::new();
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
        && let Some((cached_latest_tx_id, cached_count)) =
            cache_guard.get(&(cache_scope_id, table_id.to_string()))
        && *cached_latest_tx_id == latest_tx_id
    {
        return *cached_count;
    }

    let wal_records = wal.since(table_id, None);
    let mut live_row_ids = AHashSet::with_capacity(wal_records.len());
    let mut committed_groups = AHashSet::with_capacity(wal_records.len() / 8 + 1);
    let mut aborted_groups = AHashSet::with_capacity(wal_records.len() / 8 + 1);

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
            TransactionKind::Insert | TransactionKind::Update => {
                live_row_ids.insert(record.id.0);
            }

            TransactionKind::Delete => {
                if let Some(refid) = record.refid {
                    live_row_ids.remove(&refid.0);
                }
            }

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
        && let Some((index, lookup_key)) = choose_index_lookup(table, &index_filter_map)
    {
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
                    log::debug!(
                        "relation runtime index lookup table={} index_id={} state_cardinality=0 -> empty result",
                        table.table_id,
                        index_id,
                    );
                    return Vec::new();
                }

                if !state.contains(lookup_key) {
                    log::debug!(
                        "relation runtime index lookup table={} index_id={} key_present=false -> empty result",
                        table.table_id,
                        index_id,
                    );
                    return Vec::new();
                }

                log::debug!(
                    "relation runtime index lookup table={} index_id={} key_present=true",
                    table.table_id,
                    index_id,
                );
            } else {
                log::debug!(
                    "relation runtime index lookup table={} index_id={} state_missing -> fallback_scan",
                    table.table_id,
                    index_id,
                );
                return load_live_rows(wal, &table.table_id, schema);
            }

            if lookup_key.len() == 1
                && let Some(index) = table
                    .indexes
                    .values()
                    .find(|index| index.index_id.0 == *index_id)
            {
                let single_field_name = if index.field_names.len() == 1 {
                    Some(index.field_names[0].as_str())
                } else if index.field_names.is_empty() && !index.field_name.is_empty() {
                    Some(index.field_name.as_str())
                } else {
                    None
                };

                if let Some(single_field_name) = single_field_name {
                    return load_live_rows_by_equality(
                        wal,
                        &table.table_id,
                        schema,
                        single_field_name,
                        &lookup_key[0],
                    );
                }
            }

            load_live_rows(wal, &table.table_id, schema)
        }

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
        }

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

        let mut lookup_key = Vec::new();
        
        let all_present = if !index.field_names.is_empty() {
            
            lookup_key.reserve(index.field_names.len());
            let mut present = true;
            
            for field_name in &index.field_names {
                
                match filters.get(field_name.as_str()) {

                    Some(value) => lookup_key.push(value.clone()),

                    None => {
                        present = false;
                        break;
                    }
                    
                }

            }
            
            present

        } else if !index.field_name.is_empty() {
            match filters.get(index.field_name.as_str()) {
                Some(value) => {
                    lookup_key.push(value.clone());
                    true
                }
                None => false,
            }

        } else {
            
            false

        };

        if !all_present {
            continue;
        }

        let score = lookup_key.len();
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
