use std::borrow::Cow;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use ahash::{AHashMap, AHashSet};
use common::helpers::tphashset::TPHashSet;

use crate::engine::database::transaction::TransactionLog;
use crate::engine::database::runtime_index::{
    derived_indexes_for_table,
    load_live_row_checkpoint_rows,
};
use crate::engine::database::schema::migration::{convert_value_to_field_type, TypeConversionPolicy};
use crate::engine::sql::compare_like_value;
use crate::{
    TransactionPayloadContext,
    decode_row_payload, ConcurrentWalManager, DatabaseIndex, DatabaseTable, RuntimeIndexStore,
    SelectComparisonOp, SelectCondition, SelectPredicate, TableSchema, TransactionKind,
    TransactionRecord,
};

use super::MaterializedRelationRow;

type LiveRowCountTableMap = HashMap<String, (u64, usize)>;
type LiveRowCountScopeMap = HashMap<usize, LiveRowCountTableMap>;

static LIVE_ROW_COUNT_CACHE: OnceLock<Mutex<LiveRowCountScopeMap>> =
    OnceLock::new();

fn cached_live_row_count<'a>(
    cache_guard: &'a LiveRowCountScopeMap,
    cache_scope_id: usize,
    table_id: &str,
) -> Option<&'a (u64, usize)> {
    cache_guard
        .get(&cache_scope_id)
        .and_then(|tables| tables.get(table_id))
}

fn cache_live_row_count(
    cache_guard: &mut LiveRowCountScopeMap,
    cache_scope_id: usize,
    table_id: &str,
    latest_tx_id: u64,
    count: usize,
) {
    cache_guard
        .entry(cache_scope_id)
        .or_default()
        .insert(table_id.to_string(), (latest_tx_id, count));
}

const ACCESSOR_SNAPSHOT_RESTORE_PARALLEL_MIN_ROWS: usize = 250_000;
const ACCESSOR_SNAPSHOT_RESTORE_PARALLEL_MIN_POSTINGS: usize = 50_000;
const ACCESSOR_COLD_DIRECT_SCAN_MIN_ROWS: usize = 250_000;

#[derive(Debug, Default)]
struct EqualityTableCacheEntry {
    latest_tx_id: u64,
    rows_by_id: AHashMap<u64, HashMap<String, Vec<u8>>>,
    row_ids_by_field_value: AHashMap<String, AHashMap<Vec<u8>, Vec<u64>>>,
    string_index_by_field: AHashMap<String, TPHashSet<Vec<u64>>>,
    string_index_ci_by_field: AHashMap<String, TPHashSet<Vec<u64>>>,
}

#[expect(clippy::type_complexity, reason="the types are complex but necessary for the cache structure")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EqualityTableCacheSnapshot {
    pub latest_tx_id: u64,
    pub rows_by_id: Vec<(u64, HashMap<String, Vec<u8>>)>,
    pub row_ids_by_field_value: Vec<(String, Vec<(Vec<u8>, Vec<u64>)>)>,
    pub string_index_by_field: Vec<(String, Vec<(String, Vec<u64>)>)>,
    pub string_index_ci_by_field: Vec<(String, Vec<(String, Vec<u64>)>)>,
}

static EQUALITY_TABLE_CACHE: OnceLock<Mutex<EqualityCacheScopeMap>> =
    OnceLock::new();

type EqualityCacheTableMap = AHashMap<String, EqualityTableCacheEntry>;
type EqualityCacheScopeMap = AHashMap<usize, EqualityCacheTableMap>;

fn equality_cache_table_map_mut(
    cache_guard: &mut EqualityCacheScopeMap,
    cache_scope_id: usize,
) -> Option<&mut EqualityCacheTableMap> {
    cache_guard.get_mut(&cache_scope_id)
}

fn equality_cache_entry_mut<'a>(
    cache_guard: &'a mut EqualityCacheScopeMap,
    cache_scope_id: usize,
    table_id: &str,
) -> Option<&'a mut EqualityTableCacheEntry> {
    equality_cache_table_map_mut(cache_guard, cache_scope_id)
        .and_then(|tables| tables.get_mut(table_id))
}

fn equality_cache_entry<'a>(
    cache_guard: &'a EqualityCacheScopeMap,
    cache_scope_id: usize,
    table_id: &str,
) -> Option<&'a EqualityTableCacheEntry> {
    cache_guard
        .get(&cache_scope_id)
        .and_then(|tables| tables.get(table_id))
}

fn insert_equality_cache_entry(
    cache_guard: &mut EqualityCacheScopeMap,
    cache_scope_id: usize,
    table_id: &str,
    entry: EqualityTableCacheEntry,
) {
    cache_guard
        .entry(cache_scope_id)
        .or_default()
        .insert(table_id.to_string(), entry);
}

fn accessor_snapshot_restore_string_indexes() -> bool {
    
    std::env::var("DISTDB_ACCESSOR_SNAPSHOT_RESTORE_STRING_INDEXES")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)

}

fn accessor_snapshot_persist_string_indexes() -> bool {
    
    std::env::var("DISTDB_ACCESSOR_SNAPSHOT_PERSIST_STRING_INDEXES")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
            .unwrap_or(true)

}

fn cache_entry_matches_loaded_wal_head(
    wal: &ConcurrentWalManager,
    table_id: &str,
    entry_latest_tx_id: u64,
) -> bool {

    wal
        .latest_transaction_id_if_loaded(table_id)
        .map(|tx| tx.0 == entry_latest_tx_id)
        .unwrap_or(true)

}

fn accessor_cold_direct_scan_min_rows() -> usize {

    std::env::var("DISTDB_ACCESSOR_COLD_DIRECT_SCAN_MIN_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(ACCESSOR_COLD_DIRECT_SCAN_MIN_ROWS)

}

fn cache_snapshot_from_entry(entry: &EqualityTableCacheEntry) -> EqualityTableCacheSnapshot {

    let persist_string_indexes = accessor_snapshot_persist_string_indexes();

    EqualityTableCacheSnapshot {

        latest_tx_id: entry.latest_tx_id,
        rows_by_id: entry
            .rows_by_id
            .iter()
            .map(|(row_id, row)| (*row_id, row.clone()))
            .collect(),
        row_ids_by_field_value: entry
            .row_ids_by_field_value
            .iter()
            .map(|(field, postings)| {
                (
                    field.clone(),
                    postings
                        .iter()
                        .map(|(value, row_ids)| (value.clone(), row_ids.clone()))
                        .collect::<Vec<_>>(),
                )
            })
            .collect(),
        string_index_by_field: if persist_string_indexes {
            entry
                .string_index_by_field
                .iter()
                .map(|(field, index)| {
                    (
                        field.clone(),
                        index
                            .iter()
                            .map(|(key, row_ids)| (key.clone(), row_ids.clone()))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect()
        } else {
            Vec::new()
        },
        string_index_ci_by_field: if persist_string_indexes {
            entry
                .string_index_ci_by_field
                .iter()
                .map(|(field, index)| {
                    (
                        field.clone(),
                        index
                            .iter()
                            .map(|(key, row_ids)| (key.clone(), row_ids.clone()))
                            .collect::<Vec<_>>(),
                    )
                })
                .collect()
        } else {
            Vec::new()
        },
    
    }

}

fn cache_entry_from_snapshot(snapshot: EqualityTableCacheSnapshot) -> EqualityTableCacheEntry {

    let EqualityTableCacheSnapshot {
        latest_tx_id,
        rows_by_id,
        row_ids_by_field_value,
        string_index_by_field: snapshot_string_index_by_field,
        string_index_ci_by_field: snapshot_string_index_ci_by_field,
    } = snapshot;

    let rows_by_id = build_rows_by_id_from_snapshot(rows_by_id);

    let row_ids_by_field_value =
        build_row_ids_by_field_value_from_snapshot(row_ids_by_field_value);

    let restore_string_indexes = accessor_snapshot_restore_string_indexes();

    let mut string_index_by_field = AHashMap::new();
    if restore_string_indexes {
        string_index_by_field = AHashMap::with_capacity(snapshot_string_index_by_field.len());
        for (field_name, entries) in snapshot_string_index_by_field {
            let mut index = TPHashSet::new();
            for (key, row_ids) in entries {
                index.insert(key, row_ids);
            }
            string_index_by_field.insert(field_name, index);
        }
    }

    let mut string_index_ci_by_field = AHashMap::new();
    if restore_string_indexes {
        string_index_ci_by_field = AHashMap::with_capacity(snapshot_string_index_ci_by_field.len());
        for (field_name, entries) in snapshot_string_index_ci_by_field {
            let mut index = TPHashSet::new();
            for (key, row_ids) in entries {
                index.insert(key, row_ids);
            }
            string_index_ci_by_field.insert(field_name, index);
        }
    }

    EqualityTableCacheEntry {
        latest_tx_id,
        rows_by_id,
        row_ids_by_field_value,
        string_index_by_field,
        string_index_ci_by_field,
    }

}

fn build_rows_by_id_from_snapshot(
    rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
) -> AHashMap<u64, HashMap<String, Vec<u8>>> {

    if rows.len() < ACCESSOR_SNAPSHOT_RESTORE_PARALLEL_MIN_ROWS {
        let mut rows_by_id = AHashMap::with_capacity(rows.len());
        for (row_id, row_map) in rows {
            rows_by_id.insert(row_id, row_map);
        }
        return rows_by_id;
    }

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    
    let workers = std::cmp::min(available, equality_warm_max_workers());

    if workers <= 1 {
        let mut rows_by_id = AHashMap::with_capacity(rows.len());
        for (row_id, row_map) in rows {
            rows_by_id.insert(row_id, row_map);
        }
        return rows_by_id;
    }

    let chunk_size = rows.len().div_ceil(workers);
    let chunks = split_vec_into_chunks(rows, chunk_size);
    let total_len = chunks.iter().map(|chunk| chunk.len()).sum();

    let mut partials = std::thread::scope(|scope| {

        let mut handles = Vec::with_capacity(chunks.len());

        for chunk in chunks {
            handles.push(scope.spawn(move || {
                let mut partial = AHashMap::with_capacity(chunk.len());
                for (row_id, row_map) in chunk {
                    partial.insert(row_id, row_map);
                }
                partial
            }));
        }

        let mut partials = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(partial) = handle.join() {
                partials.push(partial);
            }
        }
        
        partials

    });

    let mut rows_by_id = AHashMap::with_capacity(total_len);
    
    for partial in partials.drain(..) {
        rows_by_id.extend(partial);
    }

    rows_by_id

}

#[expect(clippy::type_complexity, reason="the types are complex but necessary for the cache structure")]
fn build_row_ids_by_field_value_from_snapshot(
    postings_by_field: Vec<(String, Vec<(Vec<u8>, Vec<u64>)>)>,
) -> AHashMap<String, AHashMap<Vec<u8>, Vec<u64>>> {

    if postings_by_field.is_empty() {
        return AHashMap::new();
    }

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let workers = std::cmp::min(available, equality_warm_max_workers());

    if workers <= 1
        || postings_by_field.len() == 1
            && postings_by_field[0].1.len() < ACCESSOR_SNAPSHOT_RESTORE_PARALLEL_MIN_POSTINGS
    {
        
        let mut row_ids_by_field_value = AHashMap::with_capacity(postings_by_field.len());
        
        for (field_name, postings) in postings_by_field {
            let mut posting_map = AHashMap::with_capacity(postings.len());
            for (value, row_ids) in postings {
                posting_map.insert(value, row_ids);
            }
            row_ids_by_field_value.insert(field_name, posting_map);
        }
        
        return row_ids_by_field_value;

    }

    let mut partials = std::thread::scope(|scope| {

        let mut handles = Vec::with_capacity(postings_by_field.len());

        for (field_name, postings) in postings_by_field {
            handles.push(scope.spawn(move || {
                let mut posting_map = AHashMap::with_capacity(postings.len());
                for (value, row_ids) in postings {
                    posting_map.insert(value, row_ids);
                }
                (field_name, posting_map)
            }));
        }

        let mut partials = Vec::with_capacity(handles.len());
        for handle in handles {
            if let Ok(partial) = handle.join() {
                partials.push(partial);
            }
        }
        
        partials

    });

    let mut row_ids_by_field_value = AHashMap::with_capacity(partials.len());
    for (field_name, posting_map) in partials.drain(..) {
        row_ids_by_field_value.insert(field_name, posting_map);
    }

    row_ids_by_field_value

}

fn split_vec_into_chunks<T>(mut values: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {

    if values.is_empty() || chunk_size == 0 {
        return vec![values];
    }

    let mut chunks = Vec::with_capacity(values.len().div_ceil(chunk_size));
    while !values.is_empty() {
        let split_at = values.len().saturating_sub(chunk_size);
        chunks.push(values.split_off(split_at));
    }

    chunks

}

pub fn snapshot_equality_cache(
    cache_scope_id: usize,
    table_id: &str,
) -> Option<EqualityTableCacheSnapshot> {

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));
    let cache_guard = cache.lock().ok()?;
    let entry = equality_cache_entry(&cache_guard, cache_scope_id, table_id)?;
    
    Some(cache_snapshot_from_entry(entry))

}

pub fn restore_equality_cache_from_snapshot(
    cache_scope_id: usize,
    table_id: &str,
    snapshot: EqualityTableCacheSnapshot,
) {
    
    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));
    
    if let Ok(mut cache_guard) = cache.lock() {
        insert_equality_cache_entry(
            &mut cache_guard,
            cache_scope_id,
            table_id,
            cache_entry_from_snapshot(snapshot),
        );
    }

}

pub fn warm_string_like_cache_for_fields(
    cache_scope_id: usize,
    table_id: &str,
    schema: &TableSchema,
    field_names: &[String],
) {

    let fields = normalize_distinct_field_names(field_names);
    if fields.is_empty() {
        return;
    }

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));
    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_id)
    {
        warm_string_like_accessors(entry, &fields, schema);
    }

}

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

        let field_name = field_name.as_str();

        if field_name.is_empty() {
            continue;
        }

        if seen.insert(field_name) {
            fields.push(field_name.to_string());
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
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field: AHashMap::new(),
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
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field: AHashMap::new(),
    }

}

fn string_key_from_value(value: &[u8], case_insensitive: bool) -> String {
    let mut key = String::from_utf8_lossy(value).into_owned();
    if case_insensitive {
        key.make_ascii_lowercase();
    }
    key
}

fn build_string_index_for_field(
    rows_by_id: &AHashMap<u64, HashMap<String, Vec<u8>>>,
    field_name: &str,
    case_insensitive: bool,
) -> TPHashSet<Vec<u64>> {

    let mut grouped = AHashMap::<String, Vec<u64>>::new();

    for (row_id, row_map) in rows_by_id {
        if let Some(value) = row_map.get(field_name) {
            let key = string_key_from_value(value, case_insensitive);
            grouped.entry(key).or_default().push(*row_id);
        }
    }

    let mut index = TPHashSet::new();
    for (key, row_ids) in grouped {
        index.insert(key, row_ids);
    }

    index

}

fn build_string_index_from_postings(
    postings: &AHashMap<Vec<u8>, Vec<u64>>,
    case_insensitive: bool,
) -> TPHashSet<Vec<u64>> {

    let mut index = TPHashSet::new();

    for (value, row_ids) in postings {
        index.insert(
            string_key_from_value(value, case_insensitive),
            row_ids.clone(),
        );
    }

    index

}

fn field_supports_text_like(schema: &TableSchema, field_name: &str) -> bool {

    let Some(field) = schema.field(field_name) else {
        return false;
    };

    matches!(
        field.field_type,
        common::schema::FieldKind::StringFixed(_) |
        common::schema::FieldKind::Text |
        common::schema::FieldKind::Enum(_)
    )

}

fn warm_string_like_accessors(
    entry: &mut EqualityTableCacheEntry,
    fields: &[String],
    schema: &TableSchema,
) {

    for field_name in fields {

        if !field_supports_text_like(schema, field_name) {
            continue;
        }

        if let Some(postings) = entry.row_ids_by_field_value.get(field_name) {
            entry
                .string_index_by_field
                .entry(field_name.clone())
                .or_insert_with(|| build_string_index_from_postings(postings, false));

            entry
                .string_index_ci_by_field
                .entry(field_name.clone())
                .or_insert_with(|| build_string_index_from_postings(postings, true));
        }

    }

}

fn ensure_string_like_index(
    entry: &mut EqualityTableCacheEntry,
    field_name: &str,
    case_insensitive: bool,
) {

    if case_insensitive {

        if !entry.string_index_ci_by_field.contains_key(field_name) {
            
            let index = entry
                .row_ids_by_field_value
                .get(field_name)
                .map(|postings| build_string_index_from_postings(postings, true))
                .unwrap_or_else(|| build_string_index_for_field(&entry.rows_by_id, field_name, true));
            
            entry
                .string_index_ci_by_field
                .insert(field_name.to_string(), index);
        }

    } else if !entry.string_index_by_field.contains_key(field_name) {
        
        let index = entry
            .row_ids_by_field_value
            .get(field_name)
            .map(|postings| build_string_index_from_postings(postings, false))
            .unwrap_or_else(|| build_string_index_for_field(&entry.rows_by_id, field_name, false));
        
        entry
            .string_index_by_field
            .insert(field_name.to_string(), index);

    }

}

fn rows_for_field_string_like(
    entry: &mut EqualityTableCacheEntry,
    field_name: &str,
    pattern: &str,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    ensure_string_like_index(entry, field_name, false);

    let Some(index) = entry.string_index_by_field.get(field_name) else {
        return Vec::new();
    };

    index
        .search_like(pattern)
        .into_iter()
        .flat_map(|(_, row_ids)| row_ids.iter().copied())
        .filter_map(|row_id| {
            entry
                .rows_by_id
                .get(&row_id)
                .cloned()
                .map(|row_map| (row_id, row_map))
        })
        .collect()

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
    schema: &TableSchema,
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

    warm_string_like_accessors(&mut entry, &fields, schema);

    entry.latest_tx_id = latest_tx_id;

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock() {
        insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
    }

}

fn remove_row_id_from_postings(postings: &mut AHashMap<Vec<u8>, Vec<u64>>, value: &[u8], row_id: u64) {

    let mut should_remove_key = false;

    if let Some(row_ids) = postings.get_mut(value) {
        row_ids.retain(|existing| *existing != row_id);
        should_remove_key = row_ids.is_empty();
    }

    if should_remove_key {
        postings.remove(value);
    }

}

fn remove_row_id_from_string_index(index: &mut TPHashSet<Vec<u64>>, key: &str, row_id: u64) {

    let Some(existing_row_ids) = index.get_mut(key) else {
        return;
    };

    existing_row_ids.retain(|existing| *existing != row_id);
    let should_remove = existing_row_ids.is_empty();

    if should_remove {
        index.remove(key);
    }

}

fn apply_cached_row_insert(
    entry: &mut EqualityTableCacheEntry,
    row_id: u64,
    row_map: &HashMap<String, Vec<u8>>,
) {

    entry.rows_by_id.insert(row_id, row_map.clone());

    for (field_name, value) in row_map {

        if let Some(postings) = entry.row_ids_by_field_value.get_mut(field_name) {
            postings.entry(value.clone()).or_default().push(row_id);
        }

        if let Some(index) = entry.string_index_by_field.get_mut(field_name) {
            let key = string_key_from_value(value, false);
            if let Some(updated) = index.get_mut(&key) {
                updated.push(row_id);
            } else {
                index.insert(key, vec![row_id]);
            }
        }

        if let Some(index) = entry.string_index_ci_by_field.get_mut(field_name) {
            let key = string_key_from_value(value, true);
            if let Some(updated) = index.get_mut(&key) {
                updated.push(row_id);
            } else {
                index.insert(key, vec![row_id]);
            }
        }

    }

}

fn apply_cached_row_delete(
    entry: &mut EqualityTableCacheEntry,
    row_id: u64,
    row_map: &HashMap<String, Vec<u8>>,
) {

    entry.rows_by_id.remove(&row_id);

    for (field_name, value) in row_map {

        if let Some(postings) = entry.row_ids_by_field_value.get_mut(field_name) {
            remove_row_id_from_postings(postings, value, row_id);
        }

        if let Some(index) = entry.string_index_by_field.get_mut(field_name) {
            let key = string_key_from_value(value, false);
            remove_row_id_from_string_index(index, &key, row_id);
        }

        if let Some(index) = entry.string_index_ci_by_field.get_mut(field_name) {
            let key = string_key_from_value(value, true);
            remove_row_id_from_string_index(index, &key, row_id);
        }
    
    }

}

pub fn apply_equality_cache_row_mutation(
    cache_scope_id: usize,
    table_id: &str,
    latest_tx_id: u64,
    kind: TransactionKind,
    row_id: u64,
    row_map: &HashMap<String, Vec<u8>>,
) {

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_id)
    {

        entry.latest_tx_id = latest_tx_id;

        match kind {

            TransactionKind::Insert | 
            TransactionKind::Update => {
                apply_cached_row_insert(entry, row_id, row_map);
            },

            TransactionKind::Delete => {
                apply_cached_row_delete(entry, row_id, row_map);
            },

            _ => {}
        }

    }
    
}

pub fn apply_equality_cache_row_mutation_batch<R>(
    cache_scope_id: usize,
    table_id: &str,
    latest_tx_id: u64,
    kind: TransactionKind,
    first_row_id: u64,
    row_maps: &[R],
)
where
    R: Borrow<HashMap<String, Vec<u8>>>,
{

    if row_maps.is_empty() {
        return;
    }

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_id)
    {
        entry.latest_tx_id = latest_tx_id;

        match kind {

            TransactionKind::Insert | 
            TransactionKind::Update => {
                for (offset, row_map) in row_maps.iter().enumerate() {
                    let row_id = first_row_id.saturating_add(offset as u64);
                    apply_cached_row_insert(entry, row_id, row_map.borrow());
                }
            },

            TransactionKind::Delete => {
                for (offset, row_map) in row_maps.iter().enumerate() {
                    let row_id = first_row_id.saturating_add(offset as u64);
                    apply_cached_row_delete(entry, row_id, row_map.borrow());
                }
            },

            _ => {}

        }

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
    
    PrefixLikeProbe {
        field_name: String,
        prefix: Vec<u8>,
        case_insensitive: bool,
        source: EqualityProbeSource,
    },
    
    StringLikeProbe {
        field_name: String,
        pattern: Vec<u8>,
        case_insensitive: bool,
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

    pub fn string_like_probe_source(&self) -> Option<EqualityProbeSource> {
        let RelationAccessStrategy::StringLikeProbe { source, .. } = self.strategy else {
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
                Cow::Borrowed(field_name.as_str())
            } else {
                field_name
                    .rsplit('.')
                    .next()
                    .filter(|candidate| schema.field(candidate).is_some())
                    .map(Cow::Borrowed)
                    .unwrap_or_else(|| Cow::Borrowed(field_name.as_str()))
            };

            let normalized_value = schema
                .field(resolved_field_name.as_ref())
                .and_then(|field| {
                    convert_value_to_field_type(
                        value,
                        &field.field_type,
                        TypeConversionPolicy::Safe,
                    )
                    .ok()
                })
                .unwrap_or_else(|| value.clone());

            filters.insert(resolved_field_name.into_owned(), normalized_value);
            true
        },

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) | 
        SelectCondition::Not(_) => false,

    }

}

pub fn collect_indexable_prefix_like_filter_for_schema(
    schema: &TableSchema,
    condition: &SelectCondition,
) -> Option<(String, Vec<u8>, bool)> {

    let mut prefix_filter: Option<(String, Vec<u8>, bool)> = None;

    if collect_indexable_prefix_like_filter_into(schema, condition, &mut prefix_filter) {
        prefix_filter
    } else {
        None
    }

}

pub fn collect_indexable_like_filter_for_schema(
    schema: &TableSchema,
    condition: &SelectCondition,
) -> Option<(String, Vec<u8>, bool)> {

    let mut like_filter: Option<(String, Vec<u8>, bool)> = None;

    if collect_indexable_like_filter_into(schema, condition, &mut like_filter) {
        like_filter
    } else {
        None
    }

}

fn collect_indexable_like_filter_into(
    schema: &TableSchema,
    condition: &SelectCondition,
    like_filter: &mut Option<(String, Vec<u8>, bool)>,
) -> bool {

    match condition {

        SelectCondition::And(children) => children
            .iter()
            .all(|child| collect_indexable_like_filter_into(schema, child, like_filter)),

        SelectCondition::Predicate(SelectPredicate::Like {
            field_name,
            pattern,
            negated,
            case_insensitive,
            escape_char,
        }) => {
            if *negated || escape_char.is_some() {
                return true;
            }

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

            if !pattern.is_empty() {
                let normalized_pattern = schema
                    .field(&resolved_field_name)
                    .and_then(|field| {
                        convert_value_to_field_type(
                            pattern,
                            &field.field_type,
                            TypeConversionPolicy::Safe,
                        )
                        .ok()
                    })
                    .unwrap_or_else(|| pattern.clone());

                merge_like_probe(
                    like_filter,
                    resolved_field_name,
                    normalized_pattern,
                    *case_insensitive,
                )
            } else {
                true
            }
        },

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) | 
        SelectCondition::Not(_) => false,

    }

}

fn merge_like_probe(
    slot: &mut Option<(String, Vec<u8>, bool)>,
    field_name: String,
    pattern: Vec<u8>,
    case_insensitive: bool,
) -> bool {

    let Some((existing_field, existing_pattern, existing_case_insensitive)) = slot.as_mut() else {
        *slot = Some((field_name, pattern, case_insensitive));
        return true;
    };

    if *existing_case_insensitive != case_insensitive || *existing_field != field_name {
        return false;
    }

    if pattern.starts_with(existing_pattern) || existing_pattern.starts_with(&pattern) {
        if pattern.len() > existing_pattern.len() {
            *existing_pattern = pattern;
        }
        return true;
    }

    false

}

fn collect_indexable_prefix_like_filter_into(
    schema: &TableSchema,
    condition: &SelectCondition,
    prefix_filter: &mut Option<(String, Vec<u8>, bool)>,
) -> bool {

    match condition {

        SelectCondition::And(children) => children
            .iter()
            .all(|child| collect_indexable_prefix_like_filter_into(schema, child, prefix_filter)),

        SelectCondition::Predicate(SelectPredicate::Like {
            field_name,
            pattern,
            negated,
            case_insensitive,
            escape_char,
        }) => {
            if *negated || escape_char.is_some() {
                return true;
            }

            let Some(raw_prefix) = simple_like_prefix(pattern) else {
                return true;
            };

            if raw_prefix.is_empty() {
                return true;
            }

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

            let normalized_prefix = schema
                .field(&resolved_field_name)
                .and_then(|field| {
                    convert_value_to_field_type(
                        &raw_prefix,
                        &field.field_type,
                        TypeConversionPolicy::Safe,
                    )
                    .ok()
                })
                .unwrap_or(raw_prefix);

            merge_prefix_probe(
                prefix_filter,
                resolved_field_name,
                normalized_prefix,
                *case_insensitive,
            )
        },

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) | 
        SelectCondition::Not(_) => false,

    }

}

fn merge_prefix_probe(
    slot: &mut Option<(String, Vec<u8>, bool)>,
    field_name: String,
    prefix: Vec<u8>,
    case_insensitive: bool,
) -> bool {

    let Some((existing_field, existing_prefix, existing_case_insensitive)) = slot.as_mut() else {
        *slot = Some((field_name, prefix, case_insensitive));
        return true;
    };

    if *existing_case_insensitive != case_insensitive || *existing_field != field_name {
        return false;
    }

    if prefix.starts_with(existing_prefix) {
        *existing_prefix = prefix;
        return true;
    }

    existing_prefix.starts_with(&prefix)

}

fn simple_like_prefix(pattern: &[u8]) -> Option<Vec<u8>> {
    if pattern.is_empty() {
        return None;
    }

    if !pattern.ends_with(b"%") {
        return None;
    }

    let prefix = &pattern[..pattern.len() - 1];

    if prefix.iter().any(|ch| *ch == b'%' || *ch == b'_') {
        return None;
    }

    Some(prefix.to_vec())
}
pub fn field_has_single_column_index<T>(table: T, field_name: &str) -> bool
where
    T: Borrow<DatabaseTable>,
{

    let table = table.borrow();

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

    load_live_rows_in_place(wal, table_id, schema)

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
            },

            TransactionKind::WriteAbort => {
                if let Some(group_id) = record.groupid {
                    aborted_groups.insert(group_id.0);
                }
            },

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

                    TransactionKind::Insert | 
                    TransactionKind::Update => {

                        if let Some(row_map) = decoded_chunk[offset].take() {
                            row_order.push(record.id.0);
                            live_rows.insert(record.id.0, row_map);
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

        }

    } else {

        for record in wal_records {

            if !record_visible_for_live_row_apply(record, &committed_groups, &aborted_groups) {
                continue;
            }

            match record.kind {

                TransactionKind::Ignore => {},

                TransactionKind::Insert | 
                TransactionKind::Update => {

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

#[expect(clippy::type_complexity, reason="returning a vector of tuples with row ID and row map")]
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

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_id)
        && cache_entry_matches_loaded_wal_head(wal, table_id, entry.latest_tx_id)
    {
        if !entry.row_ids_by_field_value.contains_key(field_name) {
            entry.row_ids_by_field_value.insert(
                field_name.to_string(),
                build_postings_for_field(&entry.rows_by_id, field_name),
            );
        }

        return rows_for_field_value(entry, field_name, lookup_value);
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(wal, table_id, schema);

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);

        let result = rows_for_field_value(&entry, field_name, lookup_value);

        if let Ok(mut cache_guard) = cache.lock() {
            insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
        }

        return result;
    }

    let entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    let result = rows_for_field_value(&entry, field_name, lookup_value);

    if let Ok(mut cache_guard) = cache.lock() {
        insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
    }

    result

}

pub fn load_live_rows_by_prefix(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    prefix: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let cache_scope_id = wal.cache_scope_id();

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_id)
        && cache_entry_matches_loaded_wal_head(wal, table_id, entry.latest_tx_id)
    {
        if !entry.row_ids_by_field_value.contains_key(field_name) {
            entry.row_ids_by_field_value.insert(
                field_name.to_string(),
                build_postings_for_field(&entry.rows_by_id, field_name),
            );
        }

        ensure_string_like_index(entry, field_name, case_insensitive);

        return rows_for_field_prefix(entry, field_name, prefix, case_insensitive);
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(wal, table_id, schema);

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);
        ensure_string_like_index(&mut entry, field_name, case_insensitive);
        
        let result = rows_for_field_prefix(&entry, field_name, prefix, case_insensitive);

        if let Ok(mut cache_guard) = cache.lock() {
            insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
        }

        return result;
    }

    let mut entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    ensure_string_like_index(&mut entry, field_name, case_insensitive);

    let result = rows_for_field_prefix(&entry, field_name, prefix, case_insensitive);

    if let Ok(mut cache_guard) = cache.lock() {
        insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
    }

    result

}

pub fn load_live_rows_by_string_like(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    pattern: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if case_insensitive {
        return load_live_rows(wal, table_id, schema)
            .into_iter()
            .filter(|(_, row_map)| {
                row_map
                    .get(field_name)
                    .map(|value| compare_like_value(value, pattern, true, None))
                    .unwrap_or(false)
            })
            .collect();
    }

    let cache_scope_id = wal.cache_scope_id();

    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock()
        && let Some(entry) = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_id)
        && cache_entry_matches_loaded_wal_head(wal, table_id, entry.latest_tx_id)
    {
        ensure_string_like_index(entry, field_name, false);

        if let Some(index) = entry.string_index_by_field.get(field_name) {
            return index
                .search_like(&String::from_utf8_lossy(pattern))
                .into_iter()
                .flat_map(|(_, row_ids)| row_ids.iter().copied())
                .filter_map(|row_id| {
                    entry
                        .rows_by_id
                        .get(&row_id)
                        .cloned()
                        .map(|row_map| (row_id, row_map))
                })
                .collect();
        }
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(wal, table_id, schema);

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);
        ensure_string_like_index(&mut entry, field_name, false);

        let pattern_text = String::from_utf8_lossy(pattern).to_string();
        
        let result = entry
            .string_index_by_field
            .get(field_name)
            .map(|index| {
                index
                    .search_like(&pattern_text)
                    .into_iter()
                    .flat_map(|(_, row_ids)| row_ids.iter().copied())
                    .filter_map(|row_id| {
                        entry
                            .rows_by_id
                            .get(&row_id)
                            .cloned()
                            .map(|row_map| (row_id, row_map))
                    })
                    .collect()
            })
            .unwrap_or_default();

        if let Ok(mut cache_guard) = cache.lock() {
            insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
        }

        return result;

    }

    let mut entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    ensure_string_like_index(&mut entry, field_name, false);

    let pattern_text = String::from_utf8_lossy(pattern).to_string();
    let result = entry
        .string_index_by_field
        .get(field_name)
        .map(|index| {
            index
                .search_like(&pattern_text)
                .into_iter()
                .flat_map(|(_, row_ids)| row_ids.iter().copied())
                .filter_map(|row_id| {
                    entry
                        .rows_by_id
                        .get(&row_id)
                        .cloned()
                        .map(|row_map| (row_id, row_map))
                })
                .collect()
        })
        .unwrap_or_default();

    if let Ok(mut cache_guard) = cache.lock() {
        insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_id, entry);
    }

    result

}

fn rows_for_field_prefix(
    entry: &EqualityTableCacheEntry,
    field_name: &str,
    prefix: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if case_insensitive {

        if let Some(index) = entry.string_index_ci_by_field.get(field_name) {

            let mut prefix_key = String::from_utf8_lossy(prefix).into_owned();
            prefix_key.make_ascii_lowercase();

            return index
                .search_prefix(&prefix_key, false)
                .into_iter()
                .flat_map(|(_, row_ids)| row_ids.iter().copied())
                .filter_map(|row_id| {
                    entry
                        .rows_by_id
                        .get(&row_id)
                        .cloned()
                        .map(|row_map| (row_id, row_map))
                })
                .collect();
        }

    } else if let Some(index) = entry.string_index_by_field.get(field_name) {

        let prefix_text = String::from_utf8_lossy(prefix);

        return index
            .search_prefix(prefix_text.as_ref(), false)
            .into_iter()
            .flat_map(|(_, row_ids)| row_ids.iter().copied())
            .filter_map(|row_id| {
                entry
                    .rows_by_id
                    .get(&row_id)
                    .cloned()
                    .map(|row_map| (row_id, row_map))
            })
            .collect();
    }

    let Some(postings) = entry.row_ids_by_field_value.get(field_name) else {
        return Vec::new();
    };

    postings
        .iter()
        .filter(|(value, _)| {
            if case_insensitive {
                value
                    .get(..prefix.len())
                    .map(|head| head.eq_ignore_ascii_case(prefix))
                    .unwrap_or(false)
            } else {
                value.starts_with(prefix)
            }
        })
        .flat_map(|(_, row_ids)| row_ids.iter().copied())
        .filter_map(|row_id| {
            entry
                .rows_by_id
                .get(&row_id)
                .cloned()
                .map(|row_map| (row_id, row_map))
        })
        .collect()

}

#[expect(clippy::type_complexity, reason="the types are complex but necessary for the cache structure")]
fn load_live_rows_for_accessor_miss(
    wal: &ConcurrentWalManager,
    table_id: &str,
    schema: &TableSchema,
) -> (u64, Vec<(u64, HashMap<String, Vec<u8>>)>) {

    if let Some(data_dir) = wal.data_dir_path()
        && let Some((latest_tx_id, live_rows)) =
            load_live_row_checkpoint_rows(&data_dir, table_id, table_id, schema)
    {
        return (latest_tx_id, live_rows);
    }

    let latest_tx_id = wal
        .latest_transaction_id(table_id)
        .map(|tx| tx.0)
        .unwrap_or(0);

    (latest_tx_id, load_live_rows(wal, table_id, schema))

}

fn build_cold_accessor_cache_entry(
    latest_tx_id: u64,
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
    field_names: &[String],
) -> EqualityTableCacheEntry {

    let fields = normalize_distinct_field_names(field_names);
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
    entry

}

fn build_rows_only_cache_entry(
    latest_tx_id: u64,
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
) -> EqualityTableCacheEntry {

    let mut rows_by_id = AHashMap::with_capacity(live_rows.len());

    for (row_id, row_map) in live_rows {
        rows_by_id.insert(row_id, row_map);
    }

    EqualityTableCacheEntry {
        latest_tx_id,
        rows_by_id,
        row_ids_by_field_value: AHashMap::new(),
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field: AHashMap::new(),
    }

}

fn ensure_field_postings(entry: &mut EqualityTableCacheEntry, field_name: &str) {

    if !entry.row_ids_by_field_value.contains_key(field_name) {
        entry.row_ids_by_field_value.insert(
            field_name.to_string(),
            build_postings_for_field(&entry.rows_by_id, field_name),
        );
    }

}

fn filter_live_rows_by_equality(
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
    field_name: &str,
    lookup_value: &[u8],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    live_rows
        .into_iter()
        .filter(|(_, row_map)| {
            row_map
                .get(field_name)
                .map(|value| value.as_slice() == lookup_value)
                .unwrap_or(false)
        })
        .collect()

}

fn filter_live_rows_by_prefix(
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
    field_name: &str,
    prefix: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    live_rows
        .into_iter()
        .filter(|(_, row_map)| {
            row_map
                .get(field_name)
                .map(|value| {
                    if case_insensitive {
                        value
                            .get(..prefix.len())
                            .map(|head| head.eq_ignore_ascii_case(prefix))
                            .unwrap_or(false)
                    } else {
                        value.starts_with(prefix)
                    }
                })
                .unwrap_or(false)
        })
        .collect()
}

fn filter_live_rows_by_like(
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
    field_name: &str,
    pattern: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    live_rows
        .into_iter()
        .filter(|(_, row_map)| {
            row_map
                .get(field_name)
                .map(|value| compare_like_value(value, pattern, case_insensitive, None))
                .unwrap_or(false)
        })
        .collect()
}


pub fn load_live_row_count(
    wal: &ConcurrentWalManager,
    table_id: &str,
) -> usize {

    let cache_scope_id = wal.cache_scope_id();

    let cache = LIVE_ROW_COUNT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(cache_guard) = cache.lock()
        && let Some((cached_latest_tx_id, cached_count)) =
            cached_live_row_count(&cache_guard, cache_scope_id, table_id)
        && wal
            .latest_transaction_id_if_loaded(table_id)
            .map(|tx| tx.0 == *cached_latest_tx_id)
            .unwrap_or(true)
    {
        return *cached_count;
    }

    let latest_tx_id = wal
        .latest_transaction_id(table_id)
        .map(|tx| tx.0)
        .unwrap_or(0);

    let count = wal
        .with_records(table_id, |wal_records| {
            let mut live_row_ids = AHashSet::with_capacity(wal_records.len());
            let mut committed_groups = AHashSet::with_capacity(wal_records.len() / 8 + 1);
            let mut aborted_groups = AHashSet::with_capacity(wal_records.len() / 8 + 1);

            for record in wal_records {

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

            for record in wal_records {

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

                    TransactionKind::Insert |
                    TransactionKind::Update => {
                        live_row_ids.insert(record.id.0);
                    },

                    TransactionKind::Delete => {
                        if let Some(refid) = record.refid {
                            live_row_ids.remove(&refid.0);
                        }
                    },

                    _ => {},

                }

            }

            live_row_ids.len()
        })
        .unwrap_or(0);

    if let Ok(mut cache_guard) = cache.lock() {
        cache_live_row_count(&mut cache_guard, cache_scope_id, table_id, latest_tx_id, count);
    }

    count

}

pub fn plan_relation_access<T>(
    table: T,
    allow_index_short_circuit: bool,
    index_filter_map: HashMap<String, Vec<u8>>,
    like_filter: Option<(String, Vec<u8>, bool)>,
) -> RelationAccessPlan
where
    T: Borrow<DatabaseTable>,
{

    let table = table.borrow();

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

    if let Some((field_name, pattern, case_insensitive)) = like_filter {

        let source = if field_has_single_column_index(table, &field_name) {
            EqualityProbeSource::ExistingIndex
        } else {
            EqualityProbeSource::TemporaryIndex
        };

        if let Some(prefix) = simple_like_prefix(&pattern)
            .or_else(|| {
                if pattern.iter().all(|ch| *ch != b'%' && *ch != b'_') {
                    Some(pattern.clone())
                } else {
                    None
                }
            })
        {
            return RelationAccessPlan {
                strategy: RelationAccessStrategy::PrefixLikeProbe {
                    field_name,
                    prefix,
                    case_insensitive,
                    source,
                },
            };
        }

        return RelationAccessPlan {
            strategy: RelationAccessStrategy::StringLikeProbe {
                field_name,
                pattern,
                case_insensitive,
                source,
            },
        };

    }

    RelationAccessPlan {
        strategy: RelationAccessStrategy::FullScan,
    }

}

pub fn materialize_relation_rows<T, S>(
    wal: &ConcurrentWalManager,
    table: T,
    schema: S,
    runtime_indexes: &RuntimeIndexStore,
    access_plan: &RelationAccessPlan,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> 
where
    T: Borrow<DatabaseTable>,
    S: Borrow<TableSchema>,
{

    let table = table.borrow();
    let schema = schema.borrow();

    let table_stream_id = resolve_materialization_stream_id(wal, table);

    match &access_plan.strategy {

        RelationAccessStrategy::RuntimeIndexLookup {
            index_id,
            lookup_key,
        } => {

            if let Some(state) = runtime_indexes.index_for_table(table_stream_id, index_id) {

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

                return load_live_rows(wal, table_stream_id, schema);

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
                        table_stream_id,
                        schema,
                        single_field_name,
                        &lookup_key[0],
                    );
                }

            }

            load_live_rows(wal, table_stream_id, schema)

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
                table_stream_id,
                schema,
                field_name,
                lookup_value,
            )

        },

        RelationAccessStrategy::PrefixLikeProbe {
            field_name,
            prefix,
            case_insensitive,
            source,
        } => {

            log::debug!(
                "relation access table={} field={} prefix={} strategy={} case_insensitive={}",
                table.table_id,
                field_name,
                String::from_utf8_lossy(prefix),
                match source {
                    EqualityProbeSource::ExistingIndex => "existing_index",
                    EqualityProbeSource::TemporaryIndex => "temporary_index",
                },
                case_insensitive,
            );

            load_live_rows_by_prefix(
                wal,
                table_stream_id,
                schema,
                field_name,
                prefix,
                *case_insensitive,
            )

        },

        RelationAccessStrategy::StringLikeProbe {
            field_name,
            pattern,
            case_insensitive,
            source,
        } => {

            log::debug!(
                "relation access table={} field={} like_pattern={} strategy={} case_insensitive={}",
                table.table_id,
                field_name,
                String::from_utf8_lossy(pattern),
                match source {
                    EqualityProbeSource::ExistingIndex => "existing_index",
                    EqualityProbeSource::TemporaryIndex => "temporary_index",
                },
                case_insensitive,
            );

            load_live_rows_by_string_like(
                wal,
                table_stream_id,
                schema,
                field_name,
                pattern,
                *case_insensitive,
            )

        },

        RelationAccessStrategy::FullScan => load_live_rows(wal, table_stream_id, schema),

    }

}

fn resolve_materialization_stream_id<'a>(
    wal: &ConcurrentWalManager,
    table: &'a DatabaseTable,
) -> &'a str {

    let scoped_stream_id = if table.entity_id.is_empty() {
        table.table_id.as_str()
    } else {
        table.entity_id.as_str()
    };

    if scoped_stream_id != table.table_id
        && wal.data_dir_path().is_none()
        && wal.latest_transaction_id_if_loaded(scoped_stream_id).is_none()
        && wal.latest_transaction_id_if_loaded(&table.table_id).is_some()
    {
        return table.table_id.as_str();
    }

    scoped_stream_id

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

        SelectCondition::Or(_) | 
        SelectCondition::Not(_) => false,

    }

}

pub fn count_condition_predicates(condition: &SelectCondition) -> usize {

    match condition {

        SelectCondition::And(children) | 
        SelectCondition::Or(children) => {
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
