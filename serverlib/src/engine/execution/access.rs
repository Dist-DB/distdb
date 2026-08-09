use std::borrow::Cow;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use ahash::{AHashMap, AHashSet};
use common::helpers::tphashset::TPHashSet;

use crate::engine::database::transaction::TransactionLog;
use crate::engine::database::runtime_index::{
    derived_indexes_for_table,
    load_live_row_count_checkpoint,
    load_live_row_checkpoint_rows,
};
use crate::engine::database::runtime_index_snapshot::RuntimeIndexSnapshotService;
use crate::engine::database::row_payload::{
    RowPayloadSchemaCache,
    decode_row_payload_if_field_equals_with_schema_cache,
    row_payload_schema_cache,
};
use crate::engine::database::schema::migration::{convert_value_to_field_type, TypeConversionPolicy};
use crate::engine::sql::{compare_like_value, compare_row_value};
use crate::{
    TransactionPayloadContext,
    decode_row_payload, ConcurrentWalManager, DatabaseIndex, DatabaseTable, RuntimeIndexStore,
    SelectComparisonOp, SelectCondition, SelectPredicate, TableSchema, TransactionKind,
    TransactionRecord,
    WalStreamMode,
};

use super::MaterializedRelationRow;

type LiveRowCountTableMap = HashMap<String, (u64, usize)>;
type LiveRowCountScopeMap = HashMap<usize, LiveRowCountTableMap>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeBound {
    pub value: Vec<u8>,
    pub inclusive: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeFilterBounds {
    pub field_name: String,
    pub lower_bound: Option<RangeBound>,
    pub upper_bound: Option<RangeBound>,
}

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
const ACCESSOR_SNAPSHOT_MAX_LIVE_ROWS: usize = 150_000;
const ACCESSOR_POSTINGS_PARALLEL_MIN_ROWS: usize = 200_000;
const ACCESSOR_CACHE_MAX_ROWS_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Default)]
struct EqualityTableCacheEntry {
    latest_tx_id: u64,
    rows_by_id: AHashMap<u64, HashMap<String, Vec<u8>>>,
    approx_rows_bytes: usize,
    row_ids_by_field_value: AHashMap<String, AHashMap<Vec<u8>, Vec<u64>>>,
    string_index_by_field: AHashMap<String, TPHashSet<Vec<u64>>>,
    string_index_ci_by_field: AHashMap<String, TPHashSet<Vec<u64>>>,
    range_row_ids_cache: AHashMap<String, Vec<u64>>,
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
type LiveRow = (u64, HashMap<String, Vec<u8>>);

const ACCESSOR_SOURCE_LOG_INTERVAL_MS: u64 = 30_000;

#[derive(Debug, Default, Clone, Copy)]
struct AccessorLoadSourceStats {
    snapshot_loads: u64,
    checkpoint_loads: u64,
    wal_scan_loads: u64,
    total_live_rows: u64,
    max_live_rows: usize,
    total_elapsed_ms: u128,
    max_elapsed_ms: u128,
    last_log_epoch_ms: u64,
}

static ACCESSOR_LOAD_SOURCE_STATS: OnceLock<Mutex<AHashMap<String, AccessorLoadSourceStats>>> =
    OnceLock::new();
static EQUALITY_PROBE_RESULT_CACHE: OnceLock<Mutex<EqualityProbeResultCacheScopeMap>> =
    OnceLock::new();

type EqualityProbeResultCacheTableMap = AHashMap<EqualityProbeCacheKey, EqualityProbeCacheEntry>;
type EqualityProbeResultCacheScopeMap = AHashMap<(usize, String), EqualityProbeResultCacheTableMap>;

const EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRIES_PER_TABLE: usize = 16;
const EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRY_ROWS: usize = 10_000;
const EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EqualityProbeCacheKey {
    filters: Vec<(String, Vec<u8>)>,
}

#[derive(Debug, Clone)]
struct EqualityProbeCacheEntry {
    latest_tx_id: u64,
    rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
}

fn equality_probe_result_cache_max_entries_per_table() -> usize {

    std::env::var("DISTDB_EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRIES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRIES_PER_TABLE)

}

fn equality_probe_result_cache_max_entry_rows() -> usize {

    std::env::var("DISTDB_EQUALITY_PROBE_RESULT_CACHE_MAX_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRY_ROWS)

}

fn equality_probe_result_cache_max_entry_bytes() -> usize {

    std::env::var("DISTDB_EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRY_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(EQUALITY_PROBE_RESULT_CACHE_MAX_ENTRY_BYTES)

}

fn equality_probe_cache_key(equality_filters: &HashMap<String, Vec<u8>>) -> EqualityProbeCacheKey {

    let mut filters = equality_filters
        .iter()
        .map(|(field_name, value)| (field_name.clone(), value.clone()))
        .collect::<Vec<_>>();

    filters.sort_by(|(left_field, left_value), (right_field, right_value)| {
        left_field
            .cmp(right_field)
            .then_with(|| left_value.cmp(right_value))
    });

    EqualityProbeCacheKey { filters }

}

fn estimate_live_rows_bytes(rows: &[(u64, HashMap<String, Vec<u8>>)]) -> usize {

    let mut bytes = rows
        .len()
        .saturating_mul(std::mem::size_of::<(u64, HashMap<String, Vec<u8>>)>());

    for (_, row_map) in rows {
        bytes = bytes.saturating_add(estimate_row_map_bytes(row_map));
    }

    bytes

}

fn cached_equality_probe_rows(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    equality_filters: &HashMap<String, Vec<u8>>,
) -> Option<Vec<(u64, HashMap<String, Vec<u8>>)>> {

    let latest_tx_id = wal.latest_transaction_id_if_loaded(table_stream_id)?.0;
    let cache_scope_id = wal.cache_scope_id();
    let table_key = (cache_scope_id, table_stream_id.to_string());
    let filter_key = equality_probe_cache_key(equality_filters);

    let cache = EQUALITY_PROBE_RESULT_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));
    let mut guard = cache.lock().ok()?;
    let table_cache = guard.get_mut(&table_key)?;
    let entry = table_cache.get(&filter_key)?;

    if entry.latest_tx_id != latest_tx_id {
        table_cache.remove(&filter_key);
        return None;
    }

    Some(entry.rows.clone())

}

fn maybe_cache_equality_probe_rows(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    equality_filters: &HashMap<String, Vec<u8>>,
    rows: &[(u64, HashMap<String, Vec<u8>>)],
) {

    let max_entries = equality_probe_result_cache_max_entries_per_table();
    let max_rows = equality_probe_result_cache_max_entry_rows();
    let max_bytes = equality_probe_result_cache_max_entry_bytes();

    if max_entries == 0 || rows.len() > max_rows {
        return;
    }

    let approx_bytes = estimate_live_rows_bytes(rows);
    if max_bytes > 0 && approx_bytes > max_bytes {
        return;
    }

    let Some(latest_tx_id) = wal.latest_transaction_id_if_loaded(table_stream_id).map(|tx| tx.0) else {
        return;
    };

    let cache_scope_id = wal.cache_scope_id();
    let table_key = (cache_scope_id, table_stream_id.to_string());
    let filter_key = equality_probe_cache_key(equality_filters);

    let cache = EQUALITY_PROBE_RESULT_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));
    let Ok(mut guard) = cache.lock() else {
        return;
    };

    let table_cache = guard.entry(table_key).or_default();
    table_cache.retain(|_, entry| entry.latest_tx_id == latest_tx_id);

    if table_cache.len() >= max_entries
        && let Some(evict_key) = table_cache.keys().next().cloned()
    {
        table_cache.remove(&evict_key);
    }

    table_cache.insert(
        filter_key,
        EqualityProbeCacheEntry {
            latest_tx_id,
            rows: rows.to_vec(),
        },
    );

}

fn record_accessor_load_source(table_stream_id: &str, source: &str, live_rows: usize, elapsed_ms: u128) {

    let stats_map = ACCESSOR_LOAD_SOURCE_STATS.get_or_init(|| Mutex::new(AHashMap::new()));

    let Ok(mut guard) = stats_map.lock() else {
        return;
    };

    let now_ms = common::epoch_ms!();
    let stats = guard.entry(table_stream_id.to_string()).or_default();

    match source {
        "accessor_snapshot" => stats.snapshot_loads = stats.snapshot_loads.saturating_add(1),
        "live_row_checkpoint" => stats.checkpoint_loads = stats.checkpoint_loads.saturating_add(1),
        "wal_scan" | "wal_scan_filtered" => {
            stats.wal_scan_loads = stats.wal_scan_loads.saturating_add(1)
        },
        _ => {}
    }

    stats.total_live_rows = stats.total_live_rows.saturating_add(live_rows as u64);
    stats.max_live_rows = std::cmp::max(stats.max_live_rows, live_rows);
    stats.total_elapsed_ms = stats.total_elapsed_ms.saturating_add(elapsed_ms);
    stats.max_elapsed_ms = std::cmp::max(stats.max_elapsed_ms, elapsed_ms);

    let total_loads = stats
        .snapshot_loads
        .saturating_add(stats.checkpoint_loads)
        .saturating_add(stats.wal_scan_loads);

    if total_loads == 0 {
        return;
    }

    let should_log = stats.last_log_epoch_ms == 0
        || now_ms.saturating_sub(stats.last_log_epoch_ms) >= ACCESSOR_SOURCE_LOG_INTERVAL_MS;

    if should_log {
        let avg_live_rows = stats.total_live_rows / total_loads;
        let avg_elapsed_ms = stats.total_elapsed_ms / (total_loads as u128);

        log::info!(
            "accessor load source stats stream={} total_loads={} snapshot_loads={} checkpoint_loads={} wal_scan_loads={} avg_live_rows={} max_live_rows={} avg_elapsed_ms={} max_elapsed_ms={}",
            table_stream_id,
            total_loads,
            stats.snapshot_loads,
            stats.checkpoint_loads,
            stats.wal_scan_loads,
            avg_live_rows,
            stats.max_live_rows,
            avg_elapsed_ms,
            stats.max_elapsed_ms,
        );

        stats.last_log_epoch_ms = now_ms;
    }

}

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

fn with_matching_equality_cache_entry<R>(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    f: impl FnOnce(&mut EqualityTableCacheEntry) -> R,
) -> Option<R> {

    let cache_scope_id = wal.cache_scope_id();
    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    let Ok(mut cache_guard) = cache.lock() else {
        return None;
    };

    let entry = equality_cache_entry_mut(&mut cache_guard, cache_scope_id, table_stream_id)?;

    if !cache_entry_matches_loaded_wal_head(wal, table_stream_id, entry.latest_tx_id) {
        return None;
    }

    Some(f(entry))

}

fn insert_scoped_equality_cache_entry(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    mut entry: EqualityTableCacheEntry,
) {

    if !enforce_entry_row_budget(&mut entry, table_stream_id, "insert") {
        return;
    }

    let cache_scope_id = wal.cache_scope_id();
    let cache = EQUALITY_TABLE_CACHE.get_or_init(|| Mutex::new(AHashMap::new()));

    if let Ok(mut cache_guard) = cache.lock() {
        insert_equality_cache_entry(&mut cache_guard, cache_scope_id, table_stream_id, entry);
    }

}

fn accessor_cold_direct_scan_min_rows() -> usize {

    std::env::var("DISTDB_ACCESSOR_COLD_DIRECT_SCAN_MIN_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(ACCESSOR_COLD_DIRECT_SCAN_MIN_ROWS)

}

fn accessor_snapshot_max_live_rows() -> usize {

    std::env::var("DISTDB_ACCESSOR_SNAPSHOT_MAX_LIVE_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(ACCESSOR_SNAPSHOT_MAX_LIVE_ROWS)

}

fn range_intersection_diagnostics_enabled() -> bool {

    std::env::var("DISTDB_RANGE_INTERSECTION_DIAGNOSTICS")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)

}

fn accessor_cache_rows_max_bytes() -> usize {

    std::env::var("DISTDB_ACCESSOR_CACHE_MAX_ROWS_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(ACCESSOR_CACHE_MAX_ROWS_BYTES)

}

fn estimate_row_map_bytes(row_map: &HashMap<String, Vec<u8>>) -> usize {

    let mut bytes = 64usize;

    for (field_name, value) in row_map {
        bytes = bytes
            .saturating_add(48)
            .saturating_add(field_name.len())
            .saturating_add(value.len());
    }

    bytes

}

fn estimate_rows_by_id_bytes(rows_by_id: &AHashMap<u64, HashMap<String, Vec<u8>>>) -> usize {

    let mut bytes = rows_by_id
        .capacity()
        .saturating_mul(std::mem::size_of::<(u64, HashMap<String, Vec<u8>>)>())
        .saturating_add(64);

    for row_map in rows_by_id.values() {
        bytes = bytes.saturating_add(estimate_row_map_bytes(row_map));
    }

    bytes

}

fn clear_cache_entry_payload(entry: &mut EqualityTableCacheEntry) {

    entry.rows_by_id.clear();
    entry.approx_rows_bytes = 0;
    entry.row_ids_by_field_value.clear();
    entry.string_index_by_field.clear();
    entry.string_index_ci_by_field.clear();
    entry.range_row_ids_cache.clear();

}

fn enforce_entry_row_budget(
    entry: &mut EqualityTableCacheEntry,
    table_id: &str,
    reason: &str,
) -> bool {

    let max_bytes = accessor_cache_rows_max_bytes();

    if max_bytes == 0 || entry.approx_rows_bytes <= max_bytes {
        return true;
    }

    log::warn!(
        "accessor cache entry evicted table={} reason={} row_bytes={} max_row_bytes={} live_rows={}",
        table_id,
        reason,
        entry.approx_rows_bytes,
        max_bytes,
        entry.rows_by_id.len(),
    );

    clear_cache_entry_payload(entry);
    false

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

    let approx_rows_bytes = estimate_rows_by_id_bytes(&rows_by_id);

    EqualityTableCacheEntry {
        latest_tx_id,
        rows_by_id,
        approx_rows_bytes,
        row_ids_by_field_value,
        string_index_by_field,
        string_index_ci_by_field,
        range_row_ids_cache: AHashMap::new(),
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
) -> Vec<(usize, HashMap<String, Vec<u8>>)> {

    if workers <= 1 || chunk.len() < 2 {

        let mut decoded = Vec::new();
        
        for (idx, record) in chunk.iter().enumerate() {
            if !record_visible_for_live_row_apply(record, committed_groups, aborted_groups) {
                continue;
            }

            if matches!(record.kind, TransactionKind::Insert | TransactionKind::Update)
                && let Some(payload) = record.payload_logical()
                && let Ok(row_map) = decode_row_payload(schema, payload)
            {
                decoded.push((idx, row_map));
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

    let total_decoded = partials.iter().map(|local| local.len()).sum();
    let mut decoded = Vec::with_capacity(total_decoded);

    for mut local in partials {
        decoded.append(&mut local);
    }

    decoded

}

fn build_postings_for_field(
    rows_by_id: &AHashMap<u64, HashMap<String, Vec<u8>>>,
    field_name: &str,
) -> AHashMap<Vec<u8>, Vec<u64>> {

    if rows_by_id.len() < ACCESSOR_POSTINGS_PARALLEL_MIN_ROWS {

        let mut row_ids_by_value = AHashMap::<Vec<u8>, Vec<u64>>::new();

        for (row_id, row_map) in rows_by_id {
            if let Some(value) = row_map.get(field_name).cloned() {
                row_ids_by_value.entry(value).or_default().push(*row_id);
            }
        }

        return row_ids_by_value;
    }

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let workers = std::cmp::min(available, equality_warm_max_workers());

    if workers <= 1 {
        let mut row_ids_by_value = AHashMap::<Vec<u8>, Vec<u64>>::new();
        for (row_id, row_map) in rows_by_id {
            if let Some(value) = row_map.get(field_name).cloned() {
                row_ids_by_value.entry(value).or_default().push(*row_id);
            }
        }
        return row_ids_by_value;
    }

    let rows = rows_by_id
        .iter()
        .map(|(row_id, row_map)| (*row_id, row_map))
        .collect::<Vec<_>>();

    let chunk_size = rows.len().div_ceil(workers);

    let mut partials = std::thread::scope(|scope| {
        let mut handles = Vec::new();

        for chunk in rows.chunks(chunk_size) {
            handles.push(scope.spawn(move || {
                let mut local = AHashMap::<Vec<u8>, Vec<u64>>::new();
                for (row_id, row_map) in chunk {
                    if let Some(value) = row_map.get(field_name) {
                        local.entry(value.clone()).or_default().push(*row_id);
                    }
                }
                local
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

    let mut merged = AHashMap::<Vec<u8>, Vec<u64>>::new();

    for partial in partials.drain(..) {
        for (value, mut row_ids) in partial {
            merged.entry(value).or_default().append(&mut row_ids);
        }
    }

    merged
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
        approx_rows_bytes: estimate_rows_by_id_bytes(&rows_by_id),
        rows_by_id,
        row_ids_by_field_value,
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field: AHashMap::new(),
        range_row_ids_cache: AHashMap::new(),
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
        approx_rows_bytes: estimate_rows_by_id_bytes(&rows_by_id),
        rows_by_id,
        row_ids_by_field_value,
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field: AHashMap::new(),
        range_row_ids_cache: AHashMap::new(),
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

fn rows_for_field_string_like_case_insensitive(
    entry: &EqualityTableCacheEntry,
    field_name: &str,
    pattern: &[u8],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    entry
        .rows_by_id
        .iter()
        .filter_map(|(row_id, row_map)| {
            row_map
                .get(field_name)
                .filter(|value| compare_like_value(value, pattern, true, None))
                .map(|_| (*row_id, row_map.clone()))
        })
        .collect()

}

fn rows_for_field_string_like_indexed(
    entry: &EqualityTableCacheEntry,
    field_name: &str,
    pattern: &[u8],
) -> Option<Vec<LiveRow>> {

    let pattern_text = String::from_utf8_lossy(pattern).to_string();

    entry.string_index_by_field.get(field_name).map(|index| {
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

fn rows_for_field_values(
    entry: &EqualityTableCacheEntry,
    equality_filters: &HashMap<String, Vec<u8>>,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if equality_filters.len() == 1
        && let Some((field_name, lookup_value)) = equality_filters.iter().next()
    {
        return rows_for_field_value(entry, field_name, lookup_value);
    }

    let mut seed_row_ids = None::<Vec<u64>>;

    for (field_name, lookup_value) in equality_filters {
        let Some(row_ids_by_value) = entry.row_ids_by_field_value.get(field_name.as_str()) else {
            return Vec::new();
        };

        let Some(row_ids) = row_ids_by_value.get(lookup_value.as_slice()) else {
            return Vec::new();
        };

        let should_replace = seed_row_ids
            .as_ref()
            .map(|existing| row_ids.len() < existing.len())
            .unwrap_or(true);

        if should_replace {
            seed_row_ids = Some(row_ids.clone());
        }
    }

    let Some(seed_row_ids) = seed_row_ids else {
        return Vec::new();
    };

    seed_row_ids
        .into_iter()
        .filter_map(|row_id| {
            let row_map = entry.rows_by_id.get(&row_id)?;

            let matches_all_filters = equality_filters.iter().all(|(field_name, lookup_value)| {
                row_map
                    .get(field_name.as_str())
                    .map(|value| value.as_slice() == lookup_value.as_slice())
                    .unwrap_or(false)
            });

            if !matches_all_filters {
                return None;
            }

            Some((row_id, row_map.clone()))
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
    entry.approx_rows_bytes = estimate_rows_by_id_bytes(&entry.rows_by_id);

    if !enforce_entry_row_budget(&mut entry, table_id, "warm") {
        return;
    }

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

    entry.range_row_ids_cache.clear();

    if let Some(previous) = entry.rows_by_id.get(&row_id) {
        entry.approx_rows_bytes = entry
            .approx_rows_bytes
            .saturating_sub(estimate_row_map_bytes(previous));
    }

    entry.rows_by_id.insert(row_id, row_map.clone());
    entry.approx_rows_bytes = entry
        .approx_rows_bytes
        .saturating_add(estimate_row_map_bytes(row_map));

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

    entry.range_row_ids_cache.clear();

    if let Some(previous) = entry.rows_by_id.get(&row_id) {
        entry.approx_rows_bytes = entry
            .approx_rows_bytes
            .saturating_sub(estimate_row_map_bytes(previous));
    }

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

        let _ = enforce_entry_row_budget(entry, table_id, "mutation");

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

        let _ = enforce_entry_row_budget(entry, table_id, "mutation_batch");

    }

}

#[derive(Debug, Clone)]
pub enum RelationAccessStrategy {

    FullScan,
    
    RuntimeIndexLookup {
        index_id: String,
        lookup_key: Vec<Vec<u8>>,
    },

    InListProbe {
        field_name: String,
        lookup_values: Vec<Vec<u8>>,
        source: EqualityProbeSource,
    },
    
    EqualityProbe {
        field_name: String,
        lookup_value: Vec<u8>,
        source: EqualityProbeSource,
        equality_filters: HashMap<String, Vec<u8>>,
    },

    RangeProbe {
        field_name: String,
        lower_bound: Option<RangeBound>,
        upper_bound: Option<RangeBound>,
        source: EqualityProbeSource,
    },

    RangeIntersectionProbe {
        filters: Vec<RangeFilterBounds>,
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

pub fn collect_indexable_in_list_filter_for_schema(
    schema: &TableSchema,
    condition: &SelectCondition,
) -> Option<(String, Vec<Vec<u8>>)> {

    let mut in_list_filter: Option<(String, Vec<Vec<u8>>)> = None;

    if collect_indexable_in_list_filter_into(schema, condition, &mut in_list_filter) {
        in_list_filter
    } else {
        None
    }

}

fn collect_indexable_in_list_filter_into(
    schema: &TableSchema,
    condition: &SelectCondition,
    in_list_filter: &mut Option<(String, Vec<Vec<u8>>)>,
) -> bool {

    match condition {

        SelectCondition::And(children) => children
            .iter()
            .all(|child| collect_indexable_in_list_filter_into(schema, child, in_list_filter)),

        SelectCondition::Predicate(SelectPredicate::InList {
            field_name,
            values,
            negated,
        }) => {

            if *negated || values.is_empty() {
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

            let normalized_values = schema
                .field(&resolved_field_name)
                .map(|field| {
                    values
                        .iter()
                        .map(|value| {
                            convert_value_to_field_type(
                                value,
                                &field.field_type,
                                TypeConversionPolicy::Safe,
                            )
                            .unwrap_or_else(|_| value.clone())
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| values.clone());

            if normalized_values.is_empty() {
                return true;
            }

            if let Some((existing_field, existing_values)) = in_list_filter {
                if *existing_field != resolved_field_name {
                    return false;
                }

                for value in normalized_values {
                    if !existing_values.iter().any(|candidate| candidate == &value) {
                        existing_values.push(value);
                    }
                }

                true
            } else {
                *in_list_filter = Some((resolved_field_name, normalized_values));
                true
            }
        },

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) |
        SelectCondition::Not(_) => false,

    }

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

#[derive(Debug, Clone)]
pub struct RelationAccessCandidateDiagnostic {
    pub access_path: String,
    pub score: u32,
    pub index_hint: String,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct RelationAccessPlanDiagnostics {
    pub chosen_access_path: String,
    pub chosen_score: u32,
    pub candidates: Vec<RelationAccessCandidateDiagnostic>,
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

fn rows_for_field_in_list(
    entry: &EqualityTableCacheEntry,
    field_name: &str,
    lookup_values: &[Vec<u8>],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let Some(row_ids_by_value) = entry.row_ids_by_field_value.get(field_name) else {
        return Vec::new();
    };

    let mut seen = AHashSet::new();
    let mut out = Vec::new();

    for lookup_value in lookup_values {
        let Some(row_ids) = row_ids_by_value.get(lookup_value.as_slice()) else {
            continue;
        };

        for row_id in row_ids {
            if seen.insert(*row_id)
                && let Some(row_map) = entry.rows_by_id.get(row_id).cloned()
            {
                out.push((*row_id, row_map));
            }
        }
    }

    out

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

pub fn load_live_rows_by_in_list(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    lookup_values: &[Vec<u8>],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if lookup_values.is_empty() {
        return Vec::new();
    }

    if let Some(result) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {
        ensure_field_postings(entry, field_name);
        rows_for_field_in_list(entry, field_name, lookup_values)
    }) {
        return result;
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        &[field_name.to_string()],
    );

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);
        let result = rows_for_field_in_list(&entry, field_name, lookup_values);
        let live_row_count = entry.rows_by_id.len();

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        maybe_persist_accessor_snapshot_from_accessor_miss(
            wal,
            table_stream_id,
            table_id,
            schema,
            latest_tx_id,
            &[field_name.to_string()],
            live_row_count,
        );

        return result;
    }

    let entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    let result = rows_for_field_in_list(&entry, field_name, lookup_values);
    let live_row_count = entry.rows_by_id.len();

    insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

    maybe_persist_accessor_snapshot_from_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        latest_tx_id,
        &[field_name.to_string()],
        live_row_count,
    );

    result

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

pub fn collect_indexable_range_filters_for_schema(
    schema: &TableSchema,
    condition: &SelectCondition,

) -> Vec<RangeFilterBounds> {

    let mut range_filters: HashMap<String, (Option<RangeBound>, Option<RangeBound>)> =
        HashMap::new();

    if !collect_indexable_range_filter_into(schema, condition, &mut range_filters) {
        return Vec::new();
    }

    let mut result = range_filters
        .into_iter()
        .filter_map(|(field_name, (lower_bound, upper_bound))| {
            if lower_bound.is_none() && upper_bound.is_none() {
                None
            } else {
                Some(RangeFilterBounds {
                    field_name,
                    lower_bound,
                    upper_bound,
                })
            }
        })
        .collect::<Vec<_>>();

    result.sort_by(|a, b| a.field_name.cmp(&b.field_name));
    result

}

pub fn collect_indexable_range_filter_for_schema(
    schema: &TableSchema,
    condition: &SelectCondition,
) -> Option<RangeFilterBounds> {

    collect_indexable_range_filters_for_schema(schema, condition)
        .into_iter()
        .next()

}

fn collect_indexable_range_filter_into(
    schema: &TableSchema,
    condition: &SelectCondition,
    range_filters: &mut HashMap<String, (Option<RangeBound>, Option<RangeBound>)>,
) -> bool {

    match condition {

        SelectCondition::And(children) => children
            .iter()
            .all(|child| collect_indexable_range_filter_into(schema, child, range_filters)),

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name,
            op,
            value,
        }) => {

            let (is_lower, inclusive) = match op {
                SelectComparisonOp::Gt => (true, false),
                SelectComparisonOp::GtEq => (true, true),
                SelectComparisonOp::Lt => (false, false),
                SelectComparisonOp::LtEq => (false, true),
                _ => return true,
            };

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

            merge_range_probe(
                range_filters,
                resolved_field_name,
                normalized_value,
                is_lower,
                inclusive,
            )
        },

        SelectCondition::Predicate(_) => true,

        SelectCondition::Or(_) |
        SelectCondition::Not(_) => false,

    }

}

fn merge_range_probe(
    slot: &mut HashMap<String, (Option<RangeBound>, Option<RangeBound>)>,
    field_name: String,
    value: Vec<u8>,
    is_lower: bool,
    inclusive: bool,
) -> bool {

    let (lower, upper) = slot.entry(field_name).or_insert((None, None));

    if is_lower {
        match lower {
            Some(existing) => {
                if compare_row_value(&value, &existing.value, &SelectComparisonOp::Gt) {
                    existing.value = value;
                    existing.inclusive = inclusive;
                } else if compare_row_value(&value, &existing.value, &SelectComparisonOp::Eq) {
                    existing.inclusive = existing.inclusive && inclusive;
                }
            }
            None => {
                *lower = Some(RangeBound { value, inclusive });
            }
        }
    } else {
        match upper {
            Some(existing) => {
                if compare_row_value(&value, &existing.value, &SelectComparisonOp::Lt) {
                    existing.value = value;
                    existing.inclusive = inclusive;
                } else if compare_row_value(&value, &existing.value, &SelectComparisonOp::Eq) {
                    existing.inclusive = existing.inclusive && inclusive;
                }
            }
            None => {
                *upper = Some(RangeBound { value, inclusive });
            }
        }
    }

    true

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
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    load_live_rows_for_accessor_miss(wal, table_stream_id, table_id, schema, &[]).1

}

pub fn load_live_rows_in_place(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    schema: &TableSchema,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let started_at = Instant::now();
    let wal_fetch_started_at = Instant::now();

    wal.with_records(table_stream_id, |records| {
        let wal_fetch_elapsed_ms = wal_fetch_started_at.elapsed().as_millis();
        collect_live_rows_from_records(
            table_stream_id,
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

            let decoded_chunk = decode_live_row_chunk(
                chunk,
                schema,
                &committed_groups,
                &aborted_groups,
                apply_workers,
            );

            let mut decoded_iter = decoded_chunk.into_iter().peekable();

            for (offset, record) in chunk.iter().enumerate() {
                match record.kind {

                    TransactionKind::Ignore => {}

                    TransactionKind::Insert | 
                    TransactionKind::Update => {

                        if let Some((decoded_offset, _)) = decoded_iter.peek()
                            && *decoded_offset == offset
                            && let Some((_, row_map)) = decoded_iter.next()
                        {
                            row_order.push(record.id.0);
                            live_rows.insert(record.id.0, row_map);
                        }

                    },

                    TransactionKind::Delete => {

                        if !record_visible_for_live_row_apply(record, &committed_groups, &aborted_groups) {
                            continue;
                        }

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

fn equality_probe_direct_scan_enabled() -> bool {

    std::env::var("DISTDB_EQUALITY_PROBE_DIRECT_SCAN")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)

}

fn should_use_direct_scan_for_equality_probe(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
) -> bool {

    if !equality_probe_direct_scan_enabled() {
        return false;
    }

    // If WAL is already loaded for this stream, direct scan stays fast and avoids
    // extra checkpoint/snapshot restore overhead.
    if wal.latest_transaction_id_if_loaded(table_stream_id).is_some() {
        return true;
    }

    // For cold durable streams with a saved live-row checkpoint, prefer the
    // accessor-miss restore path to avoid first-query full WAL replay.
    if wal.stream_mode(table_stream_id) == WalStreamMode::Durable
        && let Some(data_dir) = wal.data_dir_path()
        && load_live_row_count_checkpoint(&data_dir, table_stream_id, table_id, schema).is_some()
    {
        return false;
    }

    true

}

fn row_matches_equality_filters(
    row_map: &HashMap<String, Vec<u8>>,
    equality_filters: &HashMap<String, Vec<u8>>,
) -> bool {

    equality_filters.iter().all(|(field_name, lookup_value)| {
        row_map
            .get(field_name)
            .map(|row_value| row_value.as_slice() == lookup_value.as_slice())
            .unwrap_or(false)
    })

}

fn load_live_rows_by_equality_filters_direct_wal_scan(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    schema: &TableSchema,
    equality_filters: &HashMap<String, Vec<u8>>,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if equality_filters.is_empty() {
        return Vec::new();
    }

    let started_at = Instant::now();
    let schema_cache = row_payload_schema_cache(schema);

    let rows = wal
        .with_records(table_stream_id, |records| {
            let mut live_rows = AHashMap::with_capacity(equality_filters.len().saturating_mul(32));
            let mut row_order = Vec::with_capacity(equality_filters.len().saturating_mul(32));
            let mut committed_groups = AHashSet::with_capacity(records.len() / 8 + 1);
            let mut aborted_groups = AHashSet::with_capacity(records.len() / 8 + 1);
            let single_filter = if equality_filters.len() == 1 {
                equality_filters
                    .iter()
                    .next()
                    .map(|(field_name, value)| (field_name.as_str(), value.as_slice()))
            } else {
                None
            };

            let available_workers = std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            let apply_workers = std::cmp::min(available_workers, live_row_apply_max_workers());
            let should_parallel_apply =
                apply_workers > 1 && records.len() >= LIVE_ROW_APPLY_PARALLEL_MIN_RECORDS;

            for record in records {
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

            if should_parallel_apply {

                for chunk in records.chunks(LIVE_ROW_APPLY_PARALLEL_CHUNK_SIZE) {

                    let decoded_chunk = if let Some((field_name, lookup_value)) = single_filter {
                        decode_matching_live_row_chunk(
                            chunk,
                            &schema_cache,
                            &committed_groups,
                            &aborted_groups,
                            apply_workers,
                            field_name,
                            lookup_value,
                        )
                    } else {
                        decode_live_row_chunk(
                            chunk,
                            schema,
                            &committed_groups,
                            &aborted_groups,
                            apply_workers,
                        )
                    };

                    let mut decoded_iter = decoded_chunk.into_iter().peekable();

                    for (offset, record) in chunk.iter().enumerate() {
                        match record.kind {

                            TransactionKind::Insert | TransactionKind::Update => {
                                if let Some((decoded_offset, _)) = decoded_iter.peek()
                                    && *decoded_offset == offset
                                    && let Some((_, row_map)) = decoded_iter.next()
                                    && (single_filter.is_some() || row_matches_equality_filters(&row_map, equality_filters))
                                {
                                    row_order.push(record.id.0);
                                    live_rows.insert(record.id.0, row_map);
                                }
                            },

                            TransactionKind::Delete => {
                                if !record_visible_for_live_row_apply(record, &committed_groups, &aborted_groups) {
                                    continue;
                                }

                                if let Some(refid) = record.refid {
                                    live_rows.remove(&refid.0);
                                }
                            },

                            _ => {}
                        }
                    }

                }

            } else {

                for record in records {
                    if !record_visible_for_live_row_apply(record, &committed_groups, &aborted_groups) {
                        continue;
                    }

                    match record.kind {
                        TransactionKind::Insert | TransactionKind::Update => {
                            let Some(payload) = record.payload_logical() else {
                                continue;
                            };

                            if let Some((field_name, lookup_value)) = single_filter {
                                let Ok(maybe_row_map) = decode_row_payload_if_field_equals_with_schema_cache(
                                    &schema_cache,
                                    payload,
                                    field_name,
                                    lookup_value,
                                ) else {
                                    continue;
                                };

                                let Some(row_map) = maybe_row_map else {
                                    continue;
                                };

                                if row_matches_equality_filters(&row_map, equality_filters) {
                                    row_order.push(record.id.0);
                                    live_rows.insert(record.id.0, row_map);
                                }

                                continue;
                            }

                            let Ok(row_map) = decode_row_payload(schema, payload) else {
                                continue;
                            };

                            if row_matches_equality_filters(&row_map, equality_filters) {
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

            row_order
                .into_iter()
                .filter_map(|id| live_rows.remove(&id).map(|row_map| (id, row_map)))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let elapsed_ms = started_at.elapsed().as_millis();

    if elapsed_ms >= 100 {
        log::info!(
            "equality probe direct scan stream={} filters={} live_rows={} elapsed_ms={}",
            table_stream_id,
            equality_filters.len(),
            rows.len(),
            elapsed_ms,
        );
    }

    record_accessor_load_source(table_stream_id, "wal_scan_filtered", rows.len(), elapsed_ms);

    maybe_cache_equality_probe_rows(
        wal,
        table_stream_id,
        equality_filters,
        &rows,
    );

    rows

}

fn decode_matching_live_row_chunk(
    chunk: &[TransactionRecord],
    schema_cache: &RowPayloadSchemaCache,
    committed_groups: &AHashSet<u64>,
    aborted_groups: &AHashSet<u64>,
    workers: usize,
    field_name: &str,
    lookup_value: &[u8],
) -> Vec<(usize, HashMap<String, Vec<u8>>)> {

    if workers <= 1 || chunk.len() < 2 {

        let mut decoded = Vec::new();

        for (idx, record) in chunk.iter().enumerate() {
            if !record_visible_for_live_row_apply(record, committed_groups, aborted_groups) {
                continue;
            }

            if !matches!(record.kind, TransactionKind::Insert | TransactionKind::Update) {
                continue;
            }

            let Some(payload) = record.payload_logical() else {
                continue;
            };

            let Ok(maybe_row_map) = decode_row_payload_if_field_equals_with_schema_cache(
                schema_cache,
                payload,
                field_name,
                lookup_value,
            ) else {
                continue;
            };

            let Some(row_map) = maybe_row_map else {
                continue;
            };

            decoded.push((idx, row_map));
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
            let schema_cache = schema_cache.clone();

            handles.push(scope.spawn(move || {

                let mut local = Vec::new();

                for (offset, record) in sub_chunk.iter().enumerate() {

                    if !record_visible_for_live_row_apply(record, committed_groups, aborted_groups) {
                        continue;
                    }

                    if !matches!(record.kind, TransactionKind::Insert | TransactionKind::Update) {
                        continue;
                    }

                    let Some(payload) = record.payload_logical() else {
                        continue;
                    };

                    let Ok(maybe_row_map) = decode_row_payload_if_field_equals_with_schema_cache(
                        &schema_cache,
                        payload,
                        field_name,
                        lookup_value,
                    ) else {
                        continue;
                    };

                    let Some(row_map) = maybe_row_map else {
                        continue;
                    };

                    local.push((start + offset, row_map));
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

    let total_decoded = partials.iter().map(|local| local.len()).sum();
    let mut decoded = Vec::with_capacity(total_decoded);

    for mut local in partials {
        decoded.append(&mut local);
    }

    decoded

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
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    lookup_value: &[u8],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let mut equality_filters = HashMap::with_capacity(1);
    equality_filters.insert(field_name.to_string(), lookup_value.to_vec());

    load_live_rows_by_equality_filters(
        wal,
        table_stream_id,
        table_id,
        schema,
        &equality_filters,
    )

}

pub fn load_live_rows_by_equality_filters(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    equality_filters: &HashMap<String, Vec<u8>>,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if equality_filters.is_empty() {
        return Vec::new();
    }

    if let Some(result) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {
        for field_name in equality_filters.keys() {
            ensure_field_postings(entry, field_name);
        }

        rows_for_field_values(entry, equality_filters)
    }) {
        return result;
    }

    if let Some(rows) = cached_equality_probe_rows(wal, table_stream_id, equality_filters) {
        return rows;
    }

    if should_use_direct_scan_for_equality_probe(wal, table_stream_id, table_id, schema) {
        return load_live_rows_by_equality_filters_direct_wal_scan(
            wal,
            table_stream_id,
            schema,
            equality_filters,
        );
    }

    let warm_fields = equality_filters
        .keys()
        .cloned()
        .collect::<Vec<_>>();

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        &warm_fields,
    );

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        for field_name in equality_filters.keys() {
            ensure_field_postings(&mut entry, field_name);
        }

        let result = rows_for_field_values(&entry, equality_filters);
        let live_row_count = entry.rows_by_id.len();

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        maybe_persist_accessor_snapshot_from_accessor_miss(
            wal,
            table_stream_id,
            table_id,
            schema,
            latest_tx_id,
            &warm_fields,
            live_row_count,
        );

        return result;
    }

    let entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &warm_fields,
    );

    let result = rows_for_field_values(&entry, equality_filters);
    let live_row_count = entry.rows_by_id.len();

    insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

    maybe_persist_accessor_snapshot_from_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        latest_tx_id,
        &warm_fields,
        live_row_count,
    );

    result

}

pub fn load_live_rows_by_prefix(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    prefix: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if let Some(result) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {
        ensure_field_postings(entry, field_name);

        ensure_string_like_index(entry, field_name, case_insensitive);

        rows_for_field_prefix(entry, field_name, prefix, case_insensitive)
    }) {
        return result;
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        &[field_name.to_string()],
    );

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);
        ensure_string_like_index(&mut entry, field_name, case_insensitive);
        
        let result = rows_for_field_prefix(&entry, field_name, prefix, case_insensitive);

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        return result;
    }

    let mut entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    ensure_string_like_index(&mut entry, field_name, case_insensitive);

    let result = rows_for_field_prefix(&entry, field_name, prefix, case_insensitive);

    insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

    result

}

pub fn load_live_rows_by_string_like(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    pattern: &[u8],
    case_insensitive: bool,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if case_insensitive {

        if let Some(result) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {
            rows_for_field_string_like_case_insensitive(entry, field_name, pattern)
        }) {
            return result;
        }

        let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
            wal,
            table_stream_id,
            table_id,
            schema,
            &[],
        );

        let entry = build_rows_only_cache_entry(latest_tx_id, live_rows);

        let result = rows_for_field_string_like_case_insensitive(&entry, field_name, pattern);

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        return result;

    }

    // we are case sensitive, so we can use the string index for faster lookups

    if let Some(Some(rows)) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {

        ensure_string_like_index(entry, field_name, false);

        rows_for_field_string_like_indexed(entry, field_name, pattern)
        
    }) {
        return rows;
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        &[field_name.to_string()],
    );

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);
        ensure_string_like_index(&mut entry, field_name, false);

        let result = rows_for_field_string_like_indexed(&entry, field_name, pattern)
            .unwrap_or_default();

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        return result;

    }

    let mut entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    ensure_string_like_index(&mut entry, field_name, false);

    let result = rows_for_field_string_like_indexed(&entry, field_name, pattern)
        .unwrap_or_default();

    insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

    result

}

pub fn load_live_rows_by_range(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    field_name: &str,
    lower_bound: Option<&RangeBound>,
    upper_bound: Option<&RangeBound>,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if lower_bound.is_none() && upper_bound.is_none() {
        return load_live_rows(wal, table_stream_id, table_id, schema);
    }

    if let Some(result) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {
        ensure_field_postings(entry, field_name);
        rows_for_field_range(entry, field_name, lower_bound, upper_bound)
    }) {
        return result;
    }

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        &[field_name.to_string()],
    );

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {

        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        ensure_field_postings(&mut entry, field_name);

        let result = rows_for_field_range(&mut entry, field_name, lower_bound, upper_bound);

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        return result;

    }

    let mut entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &[field_name.to_string()],
    );

    let result = rows_for_field_range(&mut entry, field_name, lower_bound, upper_bound);

    insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

    result

}

pub fn load_live_rows_by_range_intersection(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    filters: &[RangeFilterBounds],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    if filters.is_empty() {
        return load_live_rows(wal, table_stream_id, table_id, schema);
    }

    let anchor_field = filters.first().map(|filter| filter.field_name.as_str());

    if let Some(result) = with_matching_equality_cache_entry(wal, table_stream_id, |entry| {
        if let Some(anchor_field) = anchor_field {
            ensure_field_postings(entry, anchor_field);
        }
        rows_for_range_filters(entry, filters)
    }) {
        return result;
    }

    let warm_fields = anchor_field
        .map(|field_name| vec![field_name.to_string()])
        .unwrap_or_default();

    let (latest_tx_id, live_rows) = load_live_rows_for_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        &warm_fields,
    );

    if live_rows.len() >= accessor_cold_direct_scan_min_rows() {
        let mut entry = build_rows_only_cache_entry(latest_tx_id, live_rows);
        if let Some(anchor_field) = anchor_field {
            ensure_field_postings(&mut entry, anchor_field);
        }

        let result = rows_for_range_filters(&mut entry, filters);

        insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

        return result;
    }

    let mut entry = build_cold_accessor_cache_entry(
        latest_tx_id,
        live_rows,
        &warm_fields,
    );

    let result = rows_for_range_filters(&mut entry, filters);

    insert_scoped_equality_cache_entry(wal, table_stream_id, entry);

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

fn rows_for_field_range(
    entry: &mut EqualityTableCacheEntry,
    field_name: &str,
    lower_bound: Option<&RangeBound>,
    upper_bound: Option<&RangeBound>,
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    row_ids_for_field_range(entry, field_name, lower_bound, upper_bound)
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

fn row_ids_for_field_range(
    entry: &mut EqualityTableCacheEntry,
    field_name: &str,
    lower_bound: Option<&RangeBound>,
    upper_bound: Option<&RangeBound>,
) -> Vec<u64> {

    let cache_key = range_bounds_cache_key(field_name, lower_bound, upper_bound);
    if let Some(cached_row_ids) = entry.range_row_ids_cache.get(&cache_key) {
        return cached_row_ids.clone();
    }

    let Some(postings) = entry.row_ids_by_field_value.get(field_name) else {
        return Vec::new();
    };

    let row_ids = postings
        .iter()
        .filter(|(value, _)| value_within_range(value, lower_bound, upper_bound))
        .flat_map(|(_, row_ids)| row_ids.iter().copied())
        .collect::<Vec<_>>();

    entry
        .range_row_ids_cache
        .insert(cache_key, row_ids.clone());

    row_ids

}

fn range_bounds_cache_key(
    field_name: &str,
    lower_bound: Option<&RangeBound>,
    upper_bound: Option<&RangeBound>,
) -> String {

    fn append_bound_key(
        out: &mut String,
        label: &str,
        bound: Option<&RangeBound>,
    ) {
        out.push_str(label);
        out.push('=');

        if let Some(bound) = bound {
            out.push(if bound.inclusive { '1' } else { '0' });
            out.push(':');
            for byte in &bound.value {
                out.push_str(format!("{byte:02x}").as_str());
            }
        } else {
            out.push('_');
        }
    }

    let mut key = String::with_capacity(field_name.len() + 96);
    key.push_str(field_name);
    key.push('|');
    append_bound_key(&mut key, "l", lower_bound);
    key.push('|');
    append_bound_key(&mut key, "u", upper_bound);
    key

}

fn rows_for_range_filters(
    entry: &mut EqualityTableCacheEntry,
    filters: &[RangeFilterBounds],
) -> Vec<(u64, HashMap<String, Vec<u8>>)> {

    let Some(anchor_filter) = filters.first() else {
        return entry
            .rows_by_id
            .iter()
            .map(|(row_id, row_map)| (*row_id, row_map.clone()))
            .collect();
    };

    let anchor_row_ids = row_ids_for_field_range(
        entry,
        &anchor_filter.field_name,
        anchor_filter.lower_bound.as_ref(),
        anchor_filter.upper_bound.as_ref(),
    );

    let diagnostics_enabled = range_intersection_diagnostics_enabled();
    let anchor_row_count = anchor_row_ids.len();

    if filters.len() == 1 {
        let rows = anchor_row_ids
            .into_iter()
            .filter_map(|row_id| {
                entry
                    .rows_by_id
                    .get(&row_id)
                    .cloned()
                    .map(|row_map| (row_id, row_map))
            })
            .collect::<Vec<_>>();

        if diagnostics_enabled {
            log::info!(
                "range intersection diagnostics: fields={} anchor_field={} anchor_rows={} output_rows={}",
                anchor_filter.field_name,
                anchor_filter.field_name,
                anchor_row_count,
                anchor_row_count,
            );
        }
        return rows;
    }

    let remaining_filters = &filters[1..];

    let filtered_rows = anchor_row_ids
        .into_iter()
        .filter_map(|row_id| {
            let row_map = entry.rows_by_id.get(&row_id)?;

            let matched = remaining_filters.iter().all(|filter| {
                row_map
                    .get(&filter.field_name)
                    .map(|value| {
                        value_within_range(
                            value,
                            filter.lower_bound.as_ref(),
                            filter.upper_bound.as_ref(),
                        )
                    })
                    .unwrap_or(false)
            });

            if matched {
                Some((row_id, row_map.clone()))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if diagnostics_enabled {
        let field_list = filters
            .iter()
            .map(|filter| filter.field_name.as_str())
            .collect::<Vec<_>>()
            .join(",");

        log::info!(
            "range intersection diagnostics: fields={} anchor_field={} anchor_rows={} output_rows={}",
            field_list,
            anchor_filter.field_name,
            anchor_row_count,
            filtered_rows.len(),
        );
    }

    filtered_rows

}

fn value_within_range(
    value: &[u8],
    lower_bound: Option<&RangeBound>,
    upper_bound: Option<&RangeBound>,
) -> bool {

    if let Some(lower_bound) = lower_bound {
        let lower_op = if lower_bound.inclusive {
            SelectComparisonOp::GtEq
        } else {
            SelectComparisonOp::Gt
        };

        if !compare_row_value(value, &lower_bound.value, &lower_op) {
            return false;
        }
    }

    if let Some(upper_bound) = upper_bound {
        let upper_op = if upper_bound.inclusive {
            SelectComparisonOp::LtEq
        } else {
            SelectComparisonOp::Lt
        };

        if !compare_row_value(value, &upper_bound.value, &upper_op) {
            return false;
        }
    }

    true

}

#[expect(clippy::type_complexity, reason="the types are complex but necessary for the cache structure")]
fn load_live_rows_for_accessor_miss(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    warm_fields: &[String],
) -> (u64, Vec<(u64, HashMap<String, Vec<u8>>)>) {

    let started_at = Instant::now();
    let accessor_snapshot_limit = accessor_snapshot_max_live_rows();

    if !warm_fields.is_empty()
        && let Some(data_dir) = wal.data_dir_path()
    {
        let snapshot_row_count = load_live_row_count_checkpoint(
            &data_dir,
            table_stream_id,
            table_id,
            schema,
        )
        .map(|(_, count)| count);

        let should_try_accessor_snapshot = snapshot_row_count
            .map(|count| count <= accessor_snapshot_limit)
            .unwrap_or(true);

        if should_try_accessor_snapshot
            && let Some((latest_tx_id, live_rows)) = load_live_rows_from_accessor_snapshot(
                &data_dir,
                table_stream_id,
                table_id,
                schema,
                warm_fields,
            )
        {
            let elapsed_ms = started_at.elapsed().as_millis();
            if elapsed_ms >= 100 {
                log::info!(
                    "accessor miss load source=accessor_snapshot table={} stream={} warm_fields={} live_rows={} elapsed_ms={}",
                    table_id,
                    table_stream_id,
                    warm_fields.len(),
                    live_rows.len(),
                    elapsed_ms,
                );
            }
            record_accessor_load_source(
                table_stream_id,
                "accessor_snapshot",
                live_rows.len(),
                elapsed_ms,
            );
            return (latest_tx_id, live_rows);
        }

        if let Some(row_count) = snapshot_row_count
            && row_count > accessor_snapshot_limit
        {
            log::debug!(
                "accessor miss load skipped source=accessor_snapshot table={} stream={} warm_fields={} live_rows={} max_live_rows={}",
                table_id,
                table_stream_id,
                warm_fields.len(),
                row_count,
                accessor_snapshot_limit,
            );
        }
    }

    if let Some(data_dir) = wal.data_dir_path()
        && let Some((latest_tx_id, live_rows)) =
            load_live_row_checkpoint_rows(&data_dir, table_stream_id, table_id, schema)
    {
        let elapsed_ms = started_at.elapsed().as_millis();
        if elapsed_ms >= 100 {
            log::info!(
                "accessor miss load source=live_row_checkpoint table={} stream={} warm_fields={} live_rows={} elapsed_ms={}",
                table_id,
                table_stream_id,
                warm_fields.len(),
                live_rows.len(),
                elapsed_ms,
            );
        }
        record_accessor_load_source(
            table_stream_id,
            "live_row_checkpoint",
            live_rows.len(),
            elapsed_ms,
        );
        return (latest_tx_id, live_rows);
    }

    if warm_fields.is_empty()
        && let Some(data_dir) = wal.data_dir_path()
        && let Some((latest_tx_id, live_rows)) = load_live_rows_from_accessor_snapshot(
            &data_dir,
            table_stream_id,
            table_id,
            schema,
            warm_fields,
        )
    {
        let elapsed_ms = started_at.elapsed().as_millis();
        if elapsed_ms >= 100 {
            log::info!(
                "accessor miss load source=accessor_snapshot table={} stream={} warm_fields={} live_rows={} elapsed_ms={}",
                table_id,
                table_stream_id,
                warm_fields.len(),
                live_rows.len(),
                elapsed_ms,
            );
        }
        record_accessor_load_source(
            table_stream_id,
            "accessor_snapshot",
            live_rows.len(),
            elapsed_ms,
        );
        return (latest_tx_id, live_rows);
    }

    let latest_tx_id = wal
        .latest_transaction_id(table_stream_id)
        .map(|tx| tx.0)
        .unwrap_or(0);

    let live_rows = load_live_rows_in_place(wal, table_stream_id, schema);

    maybe_persist_live_row_checkpoint_from_accessor_miss(
        wal,
        table_stream_id,
        table_id,
        schema,
        latest_tx_id,
        &live_rows,
    );

    let elapsed_ms = started_at.elapsed().as_millis();
    if elapsed_ms >= 100 {
        log::info!(
            "accessor miss load source=wal_scan table={} stream={} warm_fields={} live_rows={} elapsed_ms={}",
            table_id,
            table_stream_id,
            warm_fields.len(),
            live_rows.len(),
            elapsed_ms,
        );
    }

    record_accessor_load_source(table_stream_id, "wal_scan", live_rows.len(), elapsed_ms);

    (latest_tx_id, live_rows)

}

fn maybe_persist_live_row_checkpoint_from_accessor_miss(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    latest_tx_id: u64,
    live_rows: &[(u64, HashMap<String, Vec<u8>>) ],
) {

    if wal.stream_mode(table_stream_id) != WalStreamMode::Durable {
        return;
    }

    let Some(data_dir) = wal.data_dir_path() else {
        return;
    };

    let wal_fingerprint =
        RuntimeIndexSnapshotService::wal_stream_fingerprint(&data_dir, table_stream_id);

    let table = DatabaseTable::new(table_id.to_string(), schema.clone(), HashMap::new());

    if let Err(err) = RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        table_stream_id,
        latest_tx_id,
        wal_fingerprint,
        live_rows,
    ) {
        log::debug!(
            "accessor miss live-row checkpoint save skipped table={} reason={}",
            table_id,
            err,
        );
    }

}

fn maybe_persist_accessor_snapshot_from_accessor_miss(
    wal: &ConcurrentWalManager,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    latest_tx_id: u64,
    warm_fields: &[String],
    live_row_count: usize,
) {

    if warm_fields.is_empty() {
        return;
    }

    if wal.stream_mode(table_stream_id) != WalStreamMode::Durable {
        return;
    }

    if live_row_count > accessor_snapshot_max_live_rows() {
        return;
    }

    let Some(data_dir) = wal.data_dir_path() else {
        return;
    };

    let wal_fingerprint =
        RuntimeIndexSnapshotService::wal_stream_fingerprint(&data_dir, table_stream_id);

    let table = DatabaseTable::new(table_id.to_string(), schema.clone(), HashMap::new());
    let table_stream_id = table_stream_id.to_string();
    let warm_fields = warm_fields.to_vec();
    let cache_scope_id = wal.cache_scope_id();

    std::thread::spawn(move || {
        if let Err(err) = RuntimeIndexSnapshotService::save_accessor_cache_snapshot(
            &data_dir,
            &table,
            &table_stream_id,
            latest_tx_id,
            wal_fingerprint,
            &warm_fields,
            cache_scope_id,
        ) {
            log::debug!(
                "accessor miss accessor snapshot save skipped table={} reason={}",
                table.table_id,
                err,
            );
        }
    });

}

#[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows)")]
fn load_live_rows_from_accessor_snapshot(
    data_dir: &Path,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
    warm_fields: &[String],
) -> Option<(u64, Vec<(u64, HashMap<String, Vec<u8>>)>)> {

    let wal_fingerprint =
        RuntimeIndexSnapshotService::wal_stream_fingerprint(data_dir, table_stream_id);

    let table = DatabaseTable::new(table_id.to_string(), schema.clone(), HashMap::new());

    let snapshot = RuntimeIndexSnapshotService::load_accessor_cache_snapshot(
        data_dir,
        &table,
        table_stream_id,
        wal_fingerprint,
        warm_fields,
    )?;

    Some((snapshot.latest_tx_id, snapshot.cache.rows_by_id))

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
    entry.approx_rows_bytes = estimate_rows_by_id_bytes(&entry.rows_by_id);
    entry

}

fn build_rows_only_cache_entry(
    latest_tx_id: u64,
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
) -> EqualityTableCacheEntry {

    let rows_by_id = build_rows_by_id_from_snapshot(live_rows);

    EqualityTableCacheEntry {
        latest_tx_id,
        approx_rows_bytes: estimate_rows_by_id_bytes(&rows_by_id),
        rows_by_id,
        row_ids_by_field_value: AHashMap::new(),
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field: AHashMap::new(),
        range_row_ids_cache: AHashMap::new(),
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
    in_list_filter: Option<(String, Vec<Vec<u8>>)>,
    range_filters: Vec<RangeFilterBounds>,
    like_filter: Option<(String, Vec<u8>, bool)>,
) -> RelationAccessPlan
where
    T: Borrow<DatabaseTable>,
{

    let table = table.borrow();
    let candidates = collect_relation_access_candidates(
        table,
        allow_index_short_circuit,
        &index_filter_map,
        in_list_filter.as_ref(),
        &range_filters,
        like_filter.as_ref(),
    );

    candidates
        .first()
        .map(|candidate| candidate.plan.clone())
        .unwrap_or(RelationAccessPlan {
            strategy: RelationAccessStrategy::FullScan,
        })

}

pub fn relation_access_plan_diagnostics<T>(
    table: T,
    allow_index_short_circuit: bool,
    index_filter_map: HashMap<String, Vec<u8>>,
    in_list_filter: Option<(String, Vec<Vec<u8>>)>,
    range_filters: Vec<RangeFilterBounds>,
    like_filter: Option<(String, Vec<u8>, bool)>,
) -> RelationAccessPlanDiagnostics
where
    T: Borrow<DatabaseTable>,
{

    let table = table.borrow();

    let candidates = collect_relation_access_candidates(
        table,
        allow_index_short_circuit,
        &index_filter_map,
        in_list_filter.as_ref(),
        &range_filters,
        like_filter.as_ref(),
    );

    if candidates.is_empty() {
        return RelationAccessPlanDiagnostics {
            chosen_access_path: "full_scan".to_string(),
            chosen_score: 0,
            candidates: Vec::new(),
        };
    }

    let chosen = &candidates[0];
    let serialized_candidates = candidates
        .iter()
        .map(|candidate| RelationAccessCandidateDiagnostic {
            access_path: access_path_name(&candidate.plan.strategy).to_string(),
            score: candidate.score,
            index_hint: candidate.index_hint.clone(),
            reason: candidate.reason.clone(),
        })
        .collect::<Vec<_>>();

    RelationAccessPlanDiagnostics {
        chosen_access_path: access_path_name(&chosen.plan.strategy).to_string(),
        chosen_score: chosen.score,
        candidates: serialized_candidates,
    }

}

#[derive(Debug, Clone)]
struct ScoredRelationAccessCandidate {
    score: u32,
    rank: u8,
    plan: RelationAccessPlan,
    reason: String,
    index_hint: String,
}

fn collect_relation_access_candidates(
    table: &DatabaseTable,
    allow_index_short_circuit: bool,
    index_filter_map: &HashMap<String, Vec<u8>>,
    in_list_filter: Option<&(String, Vec<Vec<u8>>)>,
    range_filters: &[RangeFilterBounds],
    like_filter: Option<&(String, Vec<u8>, bool)>,
) -> Vec<ScoredRelationAccessCandidate> {

    let mut candidates = Vec::new();

    if allow_index_short_circuit
        && let Some((index, lookup_key)) = choose_index_lookup(table, index_filter_map)
        && runtime_index_lookup_allowed(index)
    {
        consider_relation_access_candidate(
            &mut candidates,
            RelationAccessPlan {
                strategy: RelationAccessStrategy::RuntimeIndexLookup {
                    index_id: index.index_id.0.clone(),
                    lookup_key,
                },
            },
            score_runtime_index_lookup(index),
            format!("runtime lookup for index {}", index.index_id.0),
            index.index_id.0.clone(),
        );
    }

    if let Some((field_name, lookup_value, source)) =
        choose_equality_probe_filter(table, index_filter_map)
    {
        let index_hint = single_field_index_id(table, &field_name).unwrap_or_default();

        consider_relation_access_candidate(
            &mut candidates,
            RelationAccessPlan {
                strategy: RelationAccessStrategy::EqualityProbe {
                    field_name,
                    lookup_value,
                    source,
                    equality_filters: index_filter_map.clone(),
                },
            },
            score_equality_probe(source, index_filter_map.len()),
            match source {
                EqualityProbeSource::ExistingIndex => "equality filter matched indexed field".to_string(),
                EqualityProbeSource::TemporaryIndex => "equality filter requires temporary postings".to_string(),
            },
            index_hint,
        );
    }

    if let Some((field_name, lookup_values)) = in_list_filter
        && !lookup_values.is_empty()
    {

        let source = if field_has_single_column_index(table, field_name) {
            EqualityProbeSource::ExistingIndex
        } else {
            EqualityProbeSource::TemporaryIndex
        };

        let index_hint = single_field_index_id(table, field_name).unwrap_or_default();

        consider_relation_access_candidate(
            &mut candidates,
            RelationAccessPlan {
                strategy: RelationAccessStrategy::InListProbe {
                    field_name: field_name.clone(),
                    lookup_values: lookup_values.clone(),
                    source,
                },
            },
            score_in_list_probe(source, lookup_values.len()),
            format!("IN-list on field '{}'", field_name),
            index_hint,
        );
    }

    if !range_filters.is_empty() {

        if range_filters.len() > 1 {
            let range_indexes = range_filters
                .iter()
                .filter_map(|filter| single_field_index_id(table, &filter.field_name))
                .collect::<Vec<_>>()
                .join(",");

            consider_relation_access_candidate(
                &mut candidates,
                RelationAccessPlan {
                    strategy: RelationAccessStrategy::RangeIntersectionProbe {
                        filters: range_filters.to_vec(),
                    },
                },
                score_range_intersection_probe(table, range_filters),
                "multiple range filters can be intersected".to_string(),
                range_indexes,
            );
        }

        for range_filter in range_filters {
            let source = if field_has_single_column_index(table, &range_filter.field_name) {
                EqualityProbeSource::ExistingIndex
            } else {
                EqualityProbeSource::TemporaryIndex
            };

            let index_hint = single_field_index_id(table, &range_filter.field_name).unwrap_or_default();

            consider_relation_access_candidate(
                &mut candidates,
                RelationAccessPlan {
                    strategy: RelationAccessStrategy::RangeProbe {
                        field_name: range_filter.field_name.clone(),
                        lower_bound: range_filter.lower_bound.clone(),
                        upper_bound: range_filter.upper_bound.clone(),
                        source,
                    },
                },
                score_range_probe(source),
                format!("range filter on field '{}'", range_filter.field_name),
                index_hint,
            );
        }
    }

    if let Some((field_name, pattern, case_insensitive)) = like_filter {

        let source = if field_has_single_column_index(table, field_name) {
            EqualityProbeSource::ExistingIndex
        } else {
            EqualityProbeSource::TemporaryIndex
        };

        let index_hint = single_field_index_id(table, field_name).unwrap_or_default();

        if let Some(prefix) = simple_like_prefix(pattern)
            .or_else(|| {
                if pattern.iter().all(|ch| *ch != b'%' && *ch != b'_') {
                    Some(pattern.clone())
                } else {
                    None
                }
            })
        {
            consider_relation_access_candidate(
                &mut candidates,
                RelationAccessPlan {
                    strategy: RelationAccessStrategy::PrefixLikeProbe {
                        field_name: field_name.clone(),
                        prefix,
                        case_insensitive: *case_insensitive,
                        source,
                    },
                },
                score_prefix_like_probe(source),
                format!("LIKE predicate has indexable prefix on '{}'", field_name),
                index_hint.clone(),
            );
        }

        consider_relation_access_candidate(
            &mut candidates,
            RelationAccessPlan {
                strategy: RelationAccessStrategy::StringLikeProbe {
                    field_name: field_name.clone(),
                    pattern: pattern.clone(),
                    case_insensitive: *case_insensitive,
                    source,
                },
            },
            score_string_like_probe(source),
            format!("LIKE predicate on field '{}'", field_name),
            index_hint,
        );
    }

    consider_relation_access_candidate(
        &mut candidates,
        RelationAccessPlan {
            strategy: RelationAccessStrategy::FullScan,
        },
        0,
        "fallback when no candidate can beat scan".to_string(),
        String::new(),
    );

    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.rank.cmp(&right.rank))
            .then_with(|| access_path_name(&left.plan.strategy).cmp(access_path_name(&right.plan.strategy)))
            .then_with(|| left.index_hint.cmp(&right.index_hint))
    });

    candidates

}

fn consider_relation_access_candidate(
    candidates: &mut Vec<ScoredRelationAccessCandidate>,
    plan: RelationAccessPlan,
    score: u32,
    reason: String,
    index_hint: String,
) {

    let rank = relation_access_strategy_rank(&plan.strategy);

    candidates.push(ScoredRelationAccessCandidate {
        score,
        rank,
        plan,
        reason,
        index_hint,
    });

}

fn access_path_name(strategy: &RelationAccessStrategy) -> &'static str {

    match strategy {
        RelationAccessStrategy::FullScan => "full_scan",
        RelationAccessStrategy::EqualityProbe { .. } => "equality_probe",
        RelationAccessStrategy::InListProbe { .. } => "in_list_probe",
        RelationAccessStrategy::PrefixLikeProbe { .. } => "prefix_like_probe",
        RelationAccessStrategy::StringLikeProbe { .. } => "string_like_probe",
        RelationAccessStrategy::RangeProbe { .. } => "range_probe",
        RelationAccessStrategy::RangeIntersectionProbe { .. } => "range_intersection_probe",
        RelationAccessStrategy::RuntimeIndexLookup { .. } => "index_lookup_then_scan",
    }

}

fn single_field_index_id(table: &DatabaseTable, field_name: &str) -> Option<String> {

    table
        .indexes
        .values()
        .filter(|index| {
            (!index.field_names.is_empty()
                && index.field_names.len() == 1
                && index.field_names[0] == field_name)
                || (index.field_names.is_empty() && index.field_name == field_name)
        })
        .map(|index| index.index_id.0.clone())
        .min()

}

fn relation_access_strategy_rank(strategy: &RelationAccessStrategy) -> u8 {

    match strategy {
        RelationAccessStrategy::RuntimeIndexLookup { .. } => 0,
        RelationAccessStrategy::EqualityProbe { .. } => 1,
        RelationAccessStrategy::InListProbe { .. } => 2,
        RelationAccessStrategy::RangeIntersectionProbe { .. } => 3,
        RelationAccessStrategy::RangeProbe { .. } => 4,
        RelationAccessStrategy::PrefixLikeProbe { .. } => 5,
        RelationAccessStrategy::StringLikeProbe { .. } => 6,
        RelationAccessStrategy::FullScan => 7,
    }

}

fn score_runtime_index_lookup(index: &DatabaseIndex) -> u32 {

    let base = if index.is_primary_key() {
        1000
    } else if index.is_unique_key() {
        950
    } else if index.is_relationship_driven() {
        900
    } else {
        850
    };

    let width = if !index.field_names.is_empty() {
        index.field_names.len() as u32
    } else if !index.field_name.is_empty() {
        1
    } else {
        0
    };

    base + width.saturating_mul(10)

}

fn score_equality_probe(source: EqualityProbeSource, filter_count: usize) -> u32 {

    let base = match source {
        EqualityProbeSource::ExistingIndex => 760,
        EqualityProbeSource::TemporaryIndex => 620,
    };

    base + (filter_count.min(8) as u32).saturating_mul(20)

}

fn score_in_list_probe(source: EqualityProbeSource, value_count: usize) -> u32 {

    let base = match source {
        EqualityProbeSource::ExistingIndex => 710,
        EqualityProbeSource::TemporaryIndex => 570,
    };

    let value_bonus = 120u32.saturating_sub(value_count.min(120) as u32);
    base + value_bonus

}

fn score_range_probe(source: EqualityProbeSource) -> u32 {

    match source {
        EqualityProbeSource::ExistingIndex => 640,
        EqualityProbeSource::TemporaryIndex => 510,
    }

}

fn score_range_intersection_probe(
    table: &DatabaseTable,
    range_filters: &[RangeFilterBounds],
) -> u32 {

    let indexed_count = range_filters
        .iter()
        .filter(|filter| field_has_single_column_index(table, &filter.field_name))
        .count() as u32;

    660 + indexed_count.saturating_mul(35) + (range_filters.len() as u32).saturating_mul(10)

}

fn score_prefix_like_probe(source: EqualityProbeSource) -> u32 {

    match source {
        EqualityProbeSource::ExistingIndex => 560,
        EqualityProbeSource::TemporaryIndex => 430,
    }

}

fn score_string_like_probe(source: EqualityProbeSource) -> u32 {

    match source {
        EqualityProbeSource::ExistingIndex => 500,
        EqualityProbeSource::TemporaryIndex => 370,
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

                return load_live_rows(wal, table_stream_id, &table.table_id, schema);

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
                        &table.table_id,
                        schema,
                        single_field_name,
                        &lookup_key[0],
                    );
                }

            }

            load_live_rows(wal, table_stream_id, &table.table_id, schema)

        },

        RelationAccessStrategy::EqualityProbe {
            field_name,
            lookup_value,
            source,
            equality_filters,
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

            if equality_filters.len() > 1 {
                load_live_rows_by_equality_filters(
                    wal,
                    table_stream_id,
                    &table.table_id,
                    schema,
                    equality_filters,
                )
            } else {
                load_live_rows_by_equality(
                    wal,
                    table_stream_id,
                    &table.table_id,
                    schema,
                    field_name,
                    lookup_value,
                )
            }

        },

        RelationAccessStrategy::InListProbe {
            field_name,
            lookup_values,
            source,
        } => {

            log::debug!(
                "relation access table={} field={} strategy={} values={}",
                table.table_id,
                field_name,
                match source {
                    EqualityProbeSource::ExistingIndex => "existing_index",
                    EqualityProbeSource::TemporaryIndex => "temporary_index",
                },
                lookup_values.len(),
            );

            load_live_rows_by_in_list(
                wal,
                table_stream_id,
                &table.table_id,
                schema,
                field_name,
                lookup_values,
            )

        },

        RelationAccessStrategy::RangeProbe {
            field_name,
            lower_bound,
            upper_bound,
            source,
        } => {

            log::debug!(
                "relation access table={} field={} strategy={} range_lower={} range_upper={}",
                table.table_id,
                field_name,
                match source {
                    EqualityProbeSource::ExistingIndex => "existing_index",
                    EqualityProbeSource::TemporaryIndex => "temporary_index",
                },
                lower_bound
                    .as_ref()
                    .map(|bound| {
                        format!(
                            "{}{}",
                            if bound.inclusive { ">=" } else { ">" },
                            String::from_utf8_lossy(&bound.value),
                        )
                    })
                    .unwrap_or_else(|| "none".to_string()),
                upper_bound
                    .as_ref()
                    .map(|bound| {
                        format!(
                            "{}{}",
                            if bound.inclusive { "<=" } else { "<" },
                            String::from_utf8_lossy(&bound.value),
                        )
                    })
                    .unwrap_or_else(|| "none".to_string()),
            );

            load_live_rows_by_range(
                wal,
                table_stream_id,
                &table.table_id,
                schema,
                field_name,
                lower_bound.as_ref(),
                upper_bound.as_ref(),
            )

        },

        RelationAccessStrategy::RangeIntersectionProbe {
            filters,
        } => {

            log::debug!(
                "relation access table={} strategy=range_intersection filters={}",
                table.table_id,
                filters.len(),
            );

            load_live_rows_by_range_intersection(
                wal,
                table_stream_id,
                &table.table_id,
                schema,
                filters,
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
                &table.table_id,
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
                &table.table_id,
                schema,
                field_name,
                pattern,
                *case_insensitive,
            )

        },

        RelationAccessStrategy::FullScan => {
            load_live_rows(wal, table_stream_id, &table.table_id, schema)
        },

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

    let mut selected: Option<(&DatabaseIndex, u8, usize)> = None;

    for index in derived_indexes_for_table(table) {

        let Some(score) = index_lookup_match_score(index, filters) else {
            continue;
        };

        let priority = index_lookup_priority(index);

        #[expect(clippy::unnecessary_map_or, reason="this is intentional for clarity")]
        let should_replace = selected.as_ref().map_or(true, |(best_index, best_priority, best_score)| {
            priority > *best_priority
                || (priority == *best_priority && score > *best_score)
                || (priority == *best_priority
                    && score == *best_score
                    && index.index_id.0 < best_index.index_id.0)
        });
        
        if should_replace {
            selected = Some((index, priority, score));
        }

    }

    selected.and_then(|(index, _, _)| {
        build_lookup_key(index, filters).map(|lookup_key| (index, lookup_key))
    })

}

fn index_lookup_match_score(
    index: &DatabaseIndex,
    filters: &HashMap<String, Vec<u8>>,
) -> Option<usize> {

    if !index.field_names.is_empty() {
        for field_name in &index.field_names {
            if !filters.contains_key(field_name.as_str()) {
                return None;
            }
        }

        return Some(index.field_names.len());
    }

    if index.field_name.is_empty() {
        return None;
    }

    filters
        .contains_key(index.field_name.as_str())
        .then_some(1)

}

fn build_lookup_key(
    index: &DatabaseIndex,
    filters: &HashMap<String, Vec<u8>>,
) -> Option<Vec<Vec<u8>>> {

    if !index.field_names.is_empty() {
        let mut lookup_key = Vec::with_capacity(index.field_names.len());

        for field_name in &index.field_names {
            lookup_key.push(filters.get(field_name.as_str())?.clone());
        }

        return Some(lookup_key);
    }

    if index.field_name.is_empty() {
        return None;
    }

    filters
        .get(index.field_name.as_str())
        .cloned()
        .map(|value| vec![value])

}

fn index_lookup_priority(index: &DatabaseIndex) -> u8 {

    if index.is_primary_key() {
        return 4;
    }

    if index.is_unique_key() {
        return 3;
    }

    if index.is_relationship_driven() {
        return 2;
    }

    1

}

fn runtime_index_lookup_allowed(index: &DatabaseIndex) -> bool {
    index.is_unique_key() || index.is_relationship_driven()
}

fn choose_equality_probe_filter<T>(
    table: T,
    filters: &HashMap<String, Vec<u8>>,
) -> Option<(String, Vec<u8>, EqualityProbeSource)>
where
    T: Borrow<DatabaseTable>,
{
    let table = table.borrow();

    let mut selected: Option<(String, Vec<u8>, EqualityProbeSource)> = None;

    for (field_name, lookup_value) in filters {
        let source = if field_has_single_column_index(table, field_name) {
            EqualityProbeSource::ExistingIndex
        } else {
            EqualityProbeSource::TemporaryIndex
        };

        let should_replace = selected.as_ref().is_none_or(|(best_field_name, _, best_source)| {
            matches!(source, EqualityProbeSource::ExistingIndex)
                && matches!(best_source, EqualityProbeSource::TemporaryIndex)
                || (matches!(source, EqualityProbeSource::ExistingIndex)
                    == matches!(best_source, EqualityProbeSource::ExistingIndex)
                    && field_name < best_field_name)
        });

        if should_replace {
            selected = Some((field_name.clone(), lookup_value.clone(), source));
        }
    }

    selected
}


#[cfg(test)]
#[path = "access_test.rs"]
mod tests;
