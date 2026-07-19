use ahash::{AHashMap, AHashSet};
use common::epoch_ms;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::fs;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::time::UNIX_EPOCH;

use common::helpers::format::{make_header, verify_header, FileKind, HEADER_SIZE};
use common::helpers::hash::stable_id;
use common::helpers::io::{read_bytes, write_bytes_atomic};
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

use super::table::DatabaseTable;
use crate::engine::execution::access::{
    load_live_rows_in_place,
    warm_string_like_cache_for_fields,
};
use crate::{
    EqualityTableCacheSnapshot,
    restore_equality_cache_from_snapshot,
    snapshot_equality_cache,
    warm_equality_cache_from_live_rows, ConcurrentWalManager, DatabaseCatalog, DatabaseIndex, DatabaseIndexOrigin,
    TableSchema,
    TransactionKind,
};

const RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS: usize = 250_000;
const RUNTIME_INDEX_PARALLEL_BUILD_MAX_WORKERS: usize = 32;
const RUNTIME_INDEX_SNAPSHOT_FILE_STEM_PREFIX: &str = "rtix";
const LIVE_ROW_CHECKPOINT_FILE_STEM_PREFIX: &str = "lrows";
const ACCESSOR_CACHE_SNAPSHOT_FILE_STEM_PREFIX: &str = "acix";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RuntimeIndexTableSnapshot {
    table_id: String,
    latest_tx_id: u64,
    schema_fingerprint: String,
    live_row_count: usize,
    #[serde(default)]
    wal_size_bytes: u64,
    #[serde(default)]
    wal_modified_epoch_ms: u64,
    indexes: Vec<RuntimeIndexSnapshotIndex>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct RuntimeIndexSnapshotIndex {
    index_id: String,
    entries: Vec<Vec<Vec<u8>>>,
}

#[derive(Debug, Clone)]
struct LoadedRuntimeIndexSnapshot {
    snapshot: RuntimeIndexTableSnapshot,
    legacy_plain_encoding: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TableLiveRowCheckpoint {
    table_id: String,
    latest_tx_id: u64,
    schema_fingerprint: String,
    wal_size_bytes: u64,
    wal_modified_epoch_ms: u64,
    live_rows: Vec<(u64, HashMap<String, Vec<u8>>)>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct TableAccessorCacheSnapshot {
    table_id: String,
    latest_tx_id: u64,
    schema_fingerprint: String,
    wal_size_bytes: u64,
    wal_modified_epoch_ms: u64,
    live_row_count: usize,
    warm_fields: Vec<String>,
    cache: EqualityTableCacheSnapshot,
}

fn encode_snapshot_payload<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, String> {

    let raw = bincode::serialize(value)
        .map_err(|_| "snapshot serialization failed".to_string())?;

    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());

    encoder
        .write_all(&raw)
        .map_err(|_| "snapshot compression failed".to_string())?;

    encoder
        .finish()
        .map_err(|_| "snapshot compression finish failed".to_string())

}

fn decode_snapshot_payload<T: serde::de::DeserializeOwned>(payload: &[u8]) -> Option<(T, bool)> {

    if let Ok(decoded) = bincode::deserialize::<T>(payload) {
        return Some((decoded, true));
    }

    let decoder = ZlibDecoder::new(payload);
    let mut reader = BufReader::new(decoder);

    bincode::deserialize_from::<_, T>(&mut reader)
        .ok()
        .map(|decoded| (decoded, false))

}

fn runtime_index_parallel_build_max_workers() -> usize {

    std::env::var("DISTDB_RUNTIME_INDEX_BUILD_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(RUNTIME_INDEX_PARALLEL_BUILD_MAX_WORKERS)

}

fn runtime_index_migrate_legacy_snapshot_on_bootstrap() -> bool {

    std::env::var("DISTDB_RUNTIME_INDEX_MIGRATE_LEGACY_ON_BOOTSTRAP")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)

}

fn runtime_index_incremental_persistence_on_commit() -> bool {

    std::env::var("DISTDB_RUNTIME_INDEX_INCREMENTAL_PERSIST_ON_COMMIT")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)

}

fn runtime_index_incremental_persistence_min_interval_ms() -> u64 {

    std::env::var("DISTDB_RUNTIME_INDEX_INCREMENTAL_PERSIST_MIN_INTERVAL_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(1_000)

}

fn runtime_index_preload_accessors_on_bootstrap() -> bool {
    
    std::env::var("DISTDB_RUNTIME_INDEX_PRELOAD_ACCESSORS_ON_BOOTSTRAP")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)
}

// fn epoch_ms_now() -> u64 {
//     std::time::SystemTime::now()
//         .duration_since(UNIX_EPOCH)
//         .map(|duration| duration.as_millis() as u64)
//         .unwrap_or(0)
// }

/// In-memory state for a single index.
/// Each entry is a composite key tuple in the index's field order.
#[derive(Debug, Clone, Default)]
pub struct RuntimeIndexState {
    pub index: Option<DatabaseIndex>,
    entries: AHashSet<Vec<Vec<u8>>>,
}

impl RuntimeIndexState {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, pk_val: &[Vec<u8>]) -> bool {
        self.entries.contains(pk_val)
    }

    pub fn insert(&mut self, pk_val: Vec<Vec<u8>>) {
        self.entries.insert(pk_val);
    }

    pub fn remove(&mut self, pk_val: &[Vec<u8>]) {
        self.entries.remove(pk_val);
    }

    pub fn cardinality(&self) -> usize {
        self.entries.len()
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn rebuild(&mut self, entries: AHashSet<Vec<Vec<u8>>>) {
        self.entries = entries;
    }

    pub fn reserve_entries(&mut self, additional: usize) {

        if additional == 0 {
            return;
        }

        // Keep a meaningful free-capacity runway so sustained ingest does not
        // repeatedly hit expensive resize/rehash cliffs.
        let len = self.entries.len();
        let required = len.saturating_add(additional);
        let capacity = self.entries.capacity();
        let spare = capacity.saturating_sub(len);

        if capacity < required || spare < additional {
            let geometric_target = required.saturating_mul(2);
            let runway_target = required.saturating_add(8_192);
            let target = geometric_target.max(runway_target);
            self.entries.reserve(target.saturating_sub(len));
        }

    }

}

/// Runtime indexes for all tables across all databases.
#[derive(Debug, Clone)]
pub struct RuntimeIndexStore {
    indexes: AHashMap<String, RuntimeIndexState>,
    materialize_non_primary: bool,
    non_primary_field_allowlist: AHashSet<String>,
    non_primary_index_allowlist: AHashSet<String>,
    incremental_persist_last_saved_ms: AHashMap<String, u64>,
}

fn scoped_index_id(table_scope_id: &str, index_id: &str) -> String {
    format!("{}::{}", table_scope_id, index_id)
}

fn table_scope_id(table: &DatabaseTable) -> &str {

    if table.entity_id.is_empty() {
        table.table_id.as_str()
    } else {
        table.entity_id.as_str()
    }

}

fn resolve_table_stream_id_for_bootstrap(
    catalog: &DatabaseCatalog,
    table_id: &str,
    wal: &ConcurrentWalManager,
) -> String {

    let scoped_stream_id = catalog
        .entity_wal_stream_id(table_id)
        .unwrap_or_else(|| table_id.to_string());

    if scoped_stream_id != table_id
        && wal.data_dir_path().is_none()
        && wal.latest_transaction_id_if_loaded(&scoped_stream_id).is_none()
        && wal.latest_transaction_id_if_loaded(table_id).is_some()
    {
        return table_id.to_string();
    }

    scoped_stream_id

}

impl RuntimeIndexStore {

    pub fn new() -> Self {

        Self {
            indexes: AHashMap::new(),
            materialize_non_primary: runtime_index_materialize_non_primary(),
            non_primary_field_allowlist: runtime_index_non_primary_field_allowlist(),
            non_primary_index_allowlist: runtime_index_non_primary_index_allowlist(),
            incremental_persist_last_saved_ms: AHashMap::new(),
        }

    }

    pub fn should_track_index(&self, index: &DatabaseIndex) -> bool {
        
        if index.is_temporary() {
            return false;
        }

        true
        
    }

    fn should_materialize_index_for_bootstrap(&self, index: &DatabaseIndex) -> bool {

        if index.is_primary_key() {
            return true;
        }

        if self.materialize_non_primary {
            return true;
        }

        if self
            .non_primary_index_allowlist
            .contains(&common::normalize_identifier!(&index.index_id.0))
        {
            return true;
        }

        if index.field_names.is_empty() {
            return !index.field_name.is_empty()
                && self
                    .non_primary_field_allowlist
                    .contains(&common::normalize_identifier!(&index.field_name));
        }

        index
            .field_names
            .iter()
            .any(|field_name| {
                self.non_primary_field_allowlist
                    .contains(&common::normalize_identifier!(field_name))
            })

    }

    pub fn index(&self, index_id: &str) -> Option<&RuntimeIndexState> {
        self.indexes.get(index_id)
    }

    pub fn index_for_table(&self, table_scope_id: &str, index_id: &str) -> Option<&RuntimeIndexState> {
        let scoped = scoped_index_id(table_scope_id, index_id);
        self.indexes.get(&scoped)
    }

    #[expect(clippy::should_implement_trait, reason="Index access by string ID, not by reference")]
    pub fn index_mut(&mut self, index_id: &str) -> &mut RuntimeIndexState {
        
        match self.indexes.entry(index_id.to_string()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RuntimeIndexState::default()),
        }

    }

    pub fn index_mut_for_table(&mut self, table_scope_id: &str, index_id: &str) -> &mut RuntimeIndexState {
        
        let scoped = scoped_index_id(table_scope_id, index_id);

        match self.indexes.entry(scoped) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RuntimeIndexState::default()),
        }

    }

    pub fn remove_index_for_table(&mut self, table_scope_id: &str, index_id: &str) {
        let scoped = scoped_index_id(table_scope_id, index_id);
        self.indexes.remove(&scoped);
    }

    pub fn remove_table_indexes(&mut self, table_scope_id: &str) {
        let prefix = format!("{}::", table_scope_id);
        self.indexes.retain(|index_id, _| !index_id.starts_with(&prefix));
        self.incremental_persist_last_saved_ms.remove(table_scope_id);
    }

    pub fn cardinality(&self, index_id: &str) -> Option<usize> {
        self.index(index_id).map(|state| state.cardinality())
    }

    pub fn cardinality_for_table(&self, table_scope_id: &str, index_id: &str) -> Option<usize> {
        self.index_for_table(table_scope_id, index_id)
            .map(|state| state.cardinality())
    }

    pub fn stats(&self, index_id: &str) -> Option<(usize, usize)> {
        self.index(index_id)
            .map(|state| (state.cardinality(), state.capacity()))
    }

    pub fn stats_for_table(&self, table_scope_id: &str, index_id: &str) -> Option<(usize, usize)> {
        self.index_for_table(table_scope_id, index_id)
            .map(|state| (state.cardinality(), state.capacity()))
    }

    pub fn register_index(&mut self, index: DatabaseIndex) {
        
        if !self.should_track_index(&index) {
            return;
        }

        let index_id = index.index_id.0.clone();
        self.indexes.entry(index_id).or_insert_with(|| RuntimeIndexState {
            index: Some(index),
            entries: AHashSet::new(),
        });

    }

    pub fn register_index_for_table(&mut self, table_scope_id: &str, index: DatabaseIndex) {

        if !self.should_track_index(&index) {
            return;
        }

        let index_id = scoped_index_id(table_scope_id, &index.index_id.0);
        self.indexes.entry(index_id).or_insert_with(|| RuntimeIndexState {
            index: Some(index),
            entries: AHashSet::new(),
        });

    }

    pub fn record_row(&mut self, index: &DatabaseIndex, row_map: &HashMap<String, Vec<u8>>) {
        
        if !self.should_track_index(index) {
            return;
        }

        let key = index_value_tuple(index, row_map);
        self.index_mut(&index.index_id.0).insert(key);

    }

    pub fn record_row_for_table(
        &mut self,
        table_scope_id: &str,
        index: &DatabaseIndex,
        row_map: &HashMap<String, Vec<u8>>,
    ) {

        if !self.should_track_index(index) {
            return;
        }

        let key = index_value_tuple(index, row_map);
        self.index_mut_for_table(table_scope_id, &index.index_id.0)
            .insert(key);

    }

    pub fn record_table_row<'a, I>(&mut self, indexes: I, row_map: &HashMap<String, Vec<u8>>)
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {
            self.record_row(index, row_map);
        }
    }

    pub fn record_table_row_for_table<'a, I>(
        &mut self,
        table_scope_id: &str,
        indexes: I,
        row_map: &HashMap<String, Vec<u8>>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {
            self.record_row_for_table(table_scope_id, index, row_map);
        }
    }

    pub fn remove_table_row<'a, I>(&mut self, indexes: I, row_map: &HashMap<String, Vec<u8>>)
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        
        for index in indexes {
            
            if !self.should_track_index(index) {
                continue;
            }

            let key = index_value_tuple(index, row_map);
            self.index_mut(&index.index_id.0).remove(&key);

        }

    }

    pub fn remove_table_row_for_table<'a, I>(
        &mut self,
        table_scope_id: &str,
        indexes: I,
        row_map: &HashMap<String, Vec<u8>>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            let key = index_value_tuple(index, row_map);
            self.index_mut_for_table(table_scope_id, &index.index_id.0)
                .remove(&key);

        }
    }

    pub fn record_table_rows_batch<R>(
        &mut self,
        table_scope_id: &str,
        indexes: &[&DatabaseIndex],
        row_maps: &[R],
    )
    where
        R: Borrow<HashMap<String, Vec<u8>>>,
    {

        if row_maps.is_empty() {
            return;
        }

        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            let state = self.index_mut_for_table(table_scope_id, &index.index_id.0);

            state.reserve_entries(row_maps.len());

            for row_map in row_maps {
                let key = index_value_tuple(index, row_map.borrow());
                state.insert(key);
            }
        
        }

    }

    pub fn remove_table_rows_batch<R>(
        &mut self,
        table_scope_id: &str,
        indexes: &[&DatabaseIndex],
        row_maps: &[R],
    )
    where
        R: Borrow<HashMap<String, Vec<u8>>>,
    {

        if row_maps.is_empty() {
            return;
        }

        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            let state = self.index_mut_for_table(table_scope_id, &index.index_id.0);

            for row_map in row_maps {
                let key = index_value_tuple(index, row_map.borrow());
                state.remove(&key);
            }

        }

    }

    pub fn reserve_table_indexes<'a, I>(&mut self, indexes: I, additional: usize)
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        
        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            self.index_mut(&index.index_id.0).reserve_entries(additional);
        
        }

    }

    pub fn apply_table_row_mutation<'a, I>(
        &mut self,
        table_scope_id: &str,
        indexes: I,
        kind: TransactionKind,
        row_map: &HashMap<String, Vec<u8>>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {

        match kind {
            
            TransactionKind::Ignore => {},

            TransactionKind::Delete => self.remove_table_row_for_table(table_scope_id, indexes, row_map),

            TransactionKind::Insert 
            | TransactionKind::Update => {
                self.record_table_row_for_table(table_scope_id, indexes, row_map)
            },

            _ => {}

        }

    }

    /// Populate indexes for every table in every catalog by replaying their WALs.
    /// Should be called once during server bootstrap after catalogs are loaded.
    pub fn bootstrap_from_catalogs(
        &mut self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        wal: &ConcurrentWalManager,
    ) {

        let bootstrap_started_at = Instant::now();

        log::info!(
            "runtime index bootstrap mode materialize_non_primary={} preload_accessors_on_bootstrap={} warm_equality_cache_on_bootstrap=true non_primary_field_allowlist={} non_primary_index_allowlist={}",
            self.materialize_non_primary,
            runtime_index_preload_accessors_on_bootstrap(),
            
            if self.non_primary_field_allowlist.is_empty() {
                "<none>".to_string()
            } else {
                self.non_primary_field_allowlist
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            },
            
            if self.non_primary_index_allowlist.is_empty() {
                "<none>".to_string()
            } else {
                self.non_primary_index_allowlist
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(",")
            },

        );

        let mut bootstrapped_tables = 0usize;
        let mut bootstrapped_indexes = 0usize;
        let mut bootstrapped_rows = 0usize;

        let snapshot_data_dir = wal.data_dir_path();

        for (database_id, catalog) in catalogs {
            
            for table_id in catalog.table_ids() {

                let table_started_at = Instant::now();

                let Some(table) = catalog.table(&table_id) else {
                    continue;
                };

                let table_stream_id = resolve_table_stream_id_for_bootstrap(catalog, &table_id, wal);
                if table.indexes.is_empty() {
                    continue;
                }

                let tracked_indexes = table
                    .indexes
                    .values()
                    .filter(|index| {
                        self.should_track_index(index)
                            && self.should_materialize_index_for_bootstrap(index)
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                if tracked_indexes.is_empty() {
                    continue;
                }

                for index in &tracked_indexes {
                    self.register_index_for_table(&table_stream_id, index.clone());
                }

                let wal_fingerprint = snapshot_data_dir
                    .as_ref()
                    .and_then(|data_dir| wal_stream_fingerprint(data_dir, &table_stream_id));

                let mut warm_fields = tracked_indexes
                    .iter()
                    .flat_map(|index| {
                        if index.field_names.len() == 1 {
                            vec![index.field_names[0].clone()]
                        } else if index.field_names.is_empty() && !index.field_name.is_empty() {
                            vec![index.field_name.clone()]
                        } else {
                            Vec::new()
                        }
                    })
                    .filter(|field_name| !field_name.is_empty())
                    .map(|field_name| common::normalize_identifier!(field_name))
                    .collect::<Vec<_>>();

                warm_fields.sort();
                warm_fields.dedup();

                if let Some(snapshot_info) = snapshot_data_dir
                    .as_ref()
                    .and_then(|data_dir| {
                        load_runtime_index_snapshot(
                            data_dir,
                            table,
                            &table_stream_id,
                            &tracked_indexes,
                            wal_fingerprint,
                        )
                    })
                {

                    let snapshot = &snapshot_info.snapshot;
                    bootstrapped_tables += 1;
                    bootstrapped_indexes += tracked_indexes.len();
                    bootstrapped_rows += snapshot.live_row_count;

                    let restored = build_snapshot_index_entries(&tracked_indexes, snapshot);
                    let restored_index_count = restored.len();
                    let restored_entry_count = restored
                        .iter()
                        .map(|(_, _, entries)| entries.len())
                        .sum::<usize>();

                    if restored_index_count != tracked_indexes.len() {
                        log::warn!(
                            "runtime index snapshot restore mismatch database={} table={} expected_indexes={} restored_indexes={}",
                            database_id,
                            table_id,
                            tracked_indexes.len(),
                            restored_index_count,
                        );
                    }

                    for (index_id, index, entries) in restored {
                        let state = self.index_mut_for_table(&table_stream_id, &index_id);
                        state.rebuild(entries);
                        state.index = Some(index);
                    }

                    log::info!(
                        "runtime index snapshot restore database={} table={} restored_indexes={} index_tuples={} live_rows={}",
                        database_id,
                        table_id,
                        restored_index_count,
                        restored_entry_count,
                        snapshot.live_row_count,
                    );

                    if snapshot_info.legacy_plain_encoding
                        && runtime_index_migrate_legacy_snapshot_on_bootstrap()
                        && let Some(data_dir) = snapshot_data_dir.as_ref()
                    {
                        let _ = save_runtime_index_snapshot(
                            data_dir,
                            table,
                            &table_stream_id,
                            snapshot.latest_tx_id,
                            snapshot.live_row_count,
                            wal_fingerprint,
                            &tracked_indexes,
                            self,
                        );
                    } else if snapshot_info.legacy_plain_encoding {
                        log::info!(
                            "runtime index legacy snapshot detected table={} migration_deferred=true env=DISTDB_RUNTIME_INDEX_MIGRATE_LEGACY_ON_BOOTSTRAP",
                            table_id,
                        );
                    }

                    if runtime_index_preload_accessors_on_bootstrap() && !warm_fields.is_empty() {
                        
                        let preload_started_at = Instant::now();

                        if let Some(data_dir) = snapshot_data_dir.as_ref()
                            && let Some(accessor_snapshot) = load_accessor_cache_snapshot(
                                data_dir,
                                table,
                                &table_stream_id,
                                wal_fingerprint,
                                &warm_fields,
                            )
                        {

                            let live_row_count = accessor_snapshot.live_row_count;
                            restore_equality_cache_from_snapshot(
                                wal.cache_scope_id(),
                                &table_stream_id,
                                accessor_snapshot.cache,
                            );

                            warm_string_like_cache_for_fields(
                                wal.cache_scope_id(),
                                &table_stream_id,
                                &table.schema,
                                &warm_fields,
                            );

                            log::info!(
                                "runtime index bootstrap accessor preload database={} table={} source={} live_rows={} load_ms={} elapsed_ms={}",
                                database_id,
                                table_id,
                                "accessor_snapshot",
                                live_row_count,
                                0,
                                preload_started_at.elapsed().as_millis(),
                            );

                            log::info!(
                                "runtime index bootstrap table complete database={} table={} indexes={} live_rows={} mode=snapshot elapsed_ms={}",
                                database_id,
                                table_id,
                                tracked_indexes.len(),
                                snapshot.live_row_count,
                                table_started_at.elapsed().as_millis(),
                            );

                            continue;

                        }

                        let checkpoint_started_at = Instant::now();
                        let checkpoint_rows = snapshot_data_dir
                            .as_ref()
                            .and_then(|data_dir| {
                                load_live_row_checkpoint(
                                    data_dir,
                                    table,
                                    &table_stream_id,
                                    wal_fingerprint,
                                )
                            });

                        let checkpoint_elapsed_ms = checkpoint_started_at.elapsed().as_millis();

                        let (latest_tx_id, live_rows, source, load_elapsed_ms) =
                            if let Some(checkpoint) = checkpoint_rows {
                                (
                                    checkpoint.latest_tx_id,
                                    checkpoint.live_rows,
                                    "checkpoint",
                                    checkpoint_elapsed_ms,
                                )
                            } else {
                                let live_rows_started_at = Instant::now();
                                let live_rows = load_live_rows_in_place(wal, &table_stream_id, &table.schema);
                                let live_rows_elapsed_ms = live_rows_started_at.elapsed().as_millis();

                                (
                                    snapshot.latest_tx_id,
                                    live_rows,
                                    "wal",
                                    live_rows_elapsed_ms,
                                )
                            };

                        if let Some(data_dir) = snapshot_data_dir.as_ref()
                            && source == "wal"
                            && let Err(err) = save_live_row_checkpoint(
                                data_dir,
                                table,
                                &table_stream_id,
                                latest_tx_id,
                                wal_fingerprint,
                                &live_rows,
                            )
                        {
                            log::warn!(
                                "live-row checkpoint save skipped table={} reason={}",
                                table_id,
                                err,
                            );
                        }

                        let live_row_count = live_rows.len();

                        warm_equality_cache_from_live_rows(
                            wal.cache_scope_id(),
                            &table_stream_id,
                            &table.schema,
                            latest_tx_id,
                            live_rows,
                            &warm_fields,
                        );

                        if let Some(data_dir) = snapshot_data_dir.as_ref()
                            && let Err(err) = save_accessor_cache_snapshot(
                                data_dir,
                                table,
                                &table_stream_id,
                                latest_tx_id,
                                wal_fingerprint,
                                &warm_fields,
                                wal.cache_scope_id(),
                            )
                        {
                            log::warn!(
                                "accessor cache snapshot save skipped table={} reason={}",
                                table_id,
                                err,
                            );
                        }

                        log::info!(
                            "runtime index bootstrap accessor preload database={} table={} source={} live_rows={} load_ms={} elapsed_ms={}",
                            database_id,
                            table_id,
                            source,
                            live_row_count,
                            load_elapsed_ms,
                            preload_started_at.elapsed().as_millis(),
                        );

                    }

                    log::info!(
                        "runtime index bootstrap table complete database={} table={} indexes={} live_rows={} mode=snapshot elapsed_ms={}",
                        database_id,
                        table_id,
                        tracked_indexes.len(),
                        snapshot.live_row_count,
                        table_started_at.elapsed().as_millis(),
                    );

                    continue;

                }

                let latest_tx_id = wal
                    .latest_transaction_id(&table_stream_id)
                    .map(|tx| tx.0)
                    .unwrap_or(0);

                let checkpoint_started_at = Instant::now();
                let checkpoint_rows = snapshot_data_dir
                    .as_ref()
                    .and_then(|data_dir| {
                        load_live_row_checkpoint(
                            data_dir,
                            table,
                            &table_stream_id,
                            wal_fingerprint,
                        )
                    });
                let checkpoint_elapsed_ms = checkpoint_started_at.elapsed().as_millis();

                let (latest_tx_id, live_rows, live_rows_elapsed_ms, live_rows_mode) =

                    if let Some(checkpoint) = checkpoint_rows {
                        (
                            checkpoint.latest_tx_id,
                            checkpoint.live_rows,
                            checkpoint_elapsed_ms,
                            "checkpoint",
                        )
                    } else {
                        let live_rows_started_at = Instant::now();
                        let live_rows = load_live_rows_in_place(wal, &table_stream_id, &table.schema);
                        let live_rows_elapsed_ms = live_rows_started_at.elapsed().as_millis();

                        (
                            latest_tx_id,
                            live_rows,
                            live_rows_elapsed_ms,
                            "wal",
                        )
                    };
                    
                let live_row_count = live_rows.len();

                if live_rows_elapsed_ms >= 1_000 {
                    log::info!(
                        "runtime index bootstrap live-row materialization database={} table={} source={} live_rows={} elapsed_ms={}",
                        database_id,
                        table_id,
                        live_rows_mode,
                        live_row_count,
                        live_rows_elapsed_ms,
                    );
                }

                let rebuild_started_at = Instant::now();
                let rebuilt = build_bootstrap_index_entries(&tracked_indexes, &live_rows);
                let rebuild_elapsed_ms = rebuild_started_at.elapsed().as_millis();

                for (index_id, index, entries) in rebuilt {
                    let state = self.index_mut_for_table(&table_stream_id, &index_id);
                    state.rebuild(entries);
                    state.index = Some(index);
                }

                if let Some(data_dir) = snapshot_data_dir.as_ref()
                    && live_rows_mode == "wal"
                    && let Err(err) = save_live_row_checkpoint(
                        data_dir,
                        table,
                        &table_stream_id,
                        latest_tx_id,
                        wal_fingerprint,
                        &live_rows,
                    )
                {
                    log::warn!(
                        "live-row checkpoint save skipped table={} reason={}",
                        table_id,
                        err,
                    );
                }

                let warm_started_at = Instant::now();
                warm_equality_cache_from_live_rows(
                    wal.cache_scope_id(),
                    &table_stream_id,
                    &table.schema,
                    latest_tx_id,
                    live_rows,
                    &warm_fields,
                );
                
                let warm_elapsed_ms = warm_started_at.elapsed().as_millis();

                if let Some(data_dir) = snapshot_data_dir.as_ref()
                    && let Err(err) = save_accessor_cache_snapshot(
                        data_dir,
                        table,
                        &table_stream_id,
                        latest_tx_id,
                        wal_fingerprint,
                        &warm_fields,
                        wal.cache_scope_id(),
                    )
                {
                    log::warn!(
                        "accessor cache snapshot save skipped table={} reason={}",
                        table_id,
                        err,
                    );
                }

                if let Some(data_dir) = snapshot_data_dir.as_ref()
                    && let Err(err) = save_runtime_index_snapshot(
                        data_dir,
                        table,
                        &table_stream_id,
                        latest_tx_id,
                        live_row_count,
                        wal_fingerprint,
                        &tracked_indexes,
                        self,
                    )
                {
                    log::warn!(
                        "runtime index snapshot save skipped table={} reason={}",
                        table_id,
                        err,
                    );
                }

                bootstrapped_tables += 1;
                bootstrapped_indexes += tracked_indexes.len();
                bootstrapped_rows += live_row_count;

                log::debug!(
                    "runtime index bootstrapped database={} table={} indexes={} live_rows={}",
                    database_id,
                    table_id,
                    tracked_indexes.len(),
                    live_row_count,
                );

                let table_elapsed_ms = table_started_at.elapsed().as_millis();
                log::info!(
                    "runtime index bootstrap table complete database={} table={} indexes={} live_rows={} live_row_materialization_ms={} index_rebuild_ms={} equality_warm_ms={} elapsed_ms={}",
                    database_id,
                    table_id,
                    tracked_indexes.len(),
                    live_row_count,
                    live_rows_elapsed_ms,
                    rebuild_elapsed_ms,
                    warm_elapsed_ms,
                    table_elapsed_ms,
                );

                #[expect(clippy::manual_is_multiple_of, reason="Readable logging of progress every 10 tables")]
                if bootstrapped_tables % 10 == 0 {
                    log::info!(
                        "runtime index bootstrap progress tables={} indexes={} live_rows={} elapsed_ms={}",
                        bootstrapped_tables,
                        bootstrapped_indexes,
                        bootstrapped_rows,
                        bootstrap_started_at.elapsed().as_millis(),
                    );
                }
            
            }
        
        }

        log::info!(
            "runtime index bootstrap complete tables={} indexes={} live_rows={} elapsed_ms={}",
            bootstrapped_tables,
            bootstrapped_indexes,
            bootstrapped_rows,
            bootstrap_started_at.elapsed().as_millis(),
        );
    
    }

    pub fn clone_for_tables(
        &self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        table_ids: &HashSet<String>,
    ) -> Self {

        let mut scoped = Self::new();

        for catalog in catalogs.values() {
            
            for table_id in catalog.table_ids() {

                if !table_ids.contains(&table_id) {
                    continue;
                }

                let Some(table) = catalog.table(&table_id) else {
                    continue;
                };

                let table_stream_id = catalog
                    .entity_wal_stream_id(&table_id)
                    .unwrap_or_else(|| table_id.clone());

                for index in table.indexes.values() {
                    if let Some(state) = self.index_for_table(&table_stream_id, &index.index_id.0) {
                        let scoped_id = scoped_index_id(&table_stream_id, &index.index_id.0);
                        scoped.indexes.insert(scoped_id, state.clone());
                    }
                }

            }

        }

        scoped
        
    }

    pub fn persist_table_snapshot_on_commit(
        &mut self,
        table: &DatabaseTable,
        table_stream_id: &str,
        wal: &ConcurrentWalManager,
    ) -> Result<(), String> {

        if !runtime_index_incremental_persistence_on_commit() {
            return Ok(());
        }

        let Some(data_dir) = wal.data_dir_path() else {
            return Ok(());
        };

        let tracked_indexes = table
            .indexes
            .values()
            .filter(|index| {
                self.should_track_index(index)
                    && self.should_materialize_index_for_bootstrap(index)
            })
            .cloned()
            .collect::<Vec<_>>();

        if tracked_indexes.is_empty() {
            return Ok(());
        }

        let min_interval_ms = runtime_index_incremental_persistence_min_interval_ms();
        let now_ms = epoch_ms!();

        if min_interval_ms > 0
            && let Some(last_persist_ms) = self.incremental_persist_last_saved_ms.get(table_stream_id)
            && now_ms.saturating_sub(*last_persist_ms) < min_interval_ms
        {
            return Ok(());
        }

        let wal_fingerprint = wal_stream_fingerprint(&data_dir, table_stream_id);
        let latest_tx_id = wal
            .latest_transaction_id(table_stream_id)
            .map(|tx| tx.0)
            .unwrap_or(0);

        let table_scope_id = table_scope_id(table);
        let live_row_count = primary_key_index(table)
            .and_then(|index| self.cardinality_for_table(table_scope_id, &index.index_id.0))
            .unwrap_or_else(|| {
                tracked_indexes
                    .iter()
                    .filter_map(|index| self.cardinality_for_table(table_scope_id, &index.index_id.0))
                    .max()
                    .unwrap_or(0)
            });

        save_runtime_index_snapshot(
            &data_dir,
            table,
            table_stream_id,
            latest_tx_id,
            live_row_count,
            wal_fingerprint,
            &tracked_indexes,
            self,
        )?;

        self.incremental_persist_last_saved_ms
            .insert(table_stream_id.to_string(), now_ms);

        Ok(())

    }

}

fn runtime_index_snapshot_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
    
    let table_key = stable_id(&[table_stream_id]);
    let stem = format!("{}_{}", RUNTIME_INDEX_SNAPSHOT_FILE_STEM_PREFIX, table_key);
    
    data_dir
        .join("runtime-index")
        .join(FileKind::Entity.file_name(stem))

}

fn accessor_cache_snapshot_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
    
    let table_key = stable_id(&[table_stream_id]);
    let stem = format!("{}_{}", ACCESSOR_CACHE_SNAPSHOT_FILE_STEM_PREFIX, table_key);
    
    data_dir
        .join("accessor-cache")
        .join(FileKind::Entity.file_name(stem))

}

fn live_row_checkpoint_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
    
    let table_key = stable_id(&[table_stream_id]);
    let stem = format!("{}_{}", LIVE_ROW_CHECKPOINT_FILE_STEM_PREFIX, table_key);
    
    data_dir
    .join("live-rows")
    .join(FileKind::Entity.file_name(stem))

}

fn wal_stream_path(data_dir: &Path, table_stream_id: &str) -> PathBuf {
    
    let stream_key = stable_id(&[table_stream_id]);
    
    data_dir.join(FileKind::Data.file_name(stream_key))

}

fn wal_stream_fingerprint(data_dir: &Path, table_stream_id: &str) -> Option<(u64, u64)> {
    
    let path = wal_stream_path(data_dir, table_stream_id);
    let metadata = fs::metadata(path).ok()?;
    
    let modified_epoch_ms = metadata
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis() as u64;

    Some((metadata.len(), modified_epoch_ms))

}

fn table_schema_fingerprint(table: &DatabaseTable) -> Option<String> {
    table_schema_fingerprint_for_parts(&table.table_id, table.schema())
}

fn table_schema_fingerprint_for_parts(
    table_id: &str,
    schema: &TableSchema,
) -> Option<String> {
    
    let encoded = bincode::serialize(schema).ok()?;
    
    let hex = encoded
        .iter()
        .map(|byte| format!("{:02x}", byte))
        .collect::<String>();
    
    Some(stable_id(&[table_id, &hex]))

}

#[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows)")]
pub fn load_live_row_checkpoint_rows(
    data_dir: &Path,
    table_stream_id: &str,
    table_id: &str,
    schema: &TableSchema,
) -> Option<(u64, Vec<(u64, HashMap<String, Vec<u8>>)>)> {

    let checkpoint_path = live_row_checkpoint_path(data_dir, table_stream_id);
    let bytes = read_bytes(&checkpoint_path).ok()?;

    if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
        return None;
    }

    let (checkpoint, _legacy_plain_encoding): (TableLiveRowCheckpoint, bool) =
        decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

    let schema_fingerprint = table_schema_fingerprint_for_parts(table_id, schema)?;

    if checkpoint.table_id != table_id || checkpoint.schema_fingerprint != schema_fingerprint {
        return None;
    }

    let (wal_size_bytes, wal_modified_epoch_ms) = wal_stream_fingerprint(data_dir, table_stream_id)?;
    if checkpoint.wal_size_bytes != wal_size_bytes
        || checkpoint.wal_modified_epoch_ms != wal_modified_epoch_ms
    {
        return None;
    }

    Some((checkpoint.latest_tx_id, checkpoint.live_rows))

}

fn load_runtime_index_snapshot(
    data_dir: &Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    tracked_indexes: &[DatabaseIndex],
    wal_fingerprint: Option<(u64, u64)>,
) -> Option<LoadedRuntimeIndexSnapshot> {

    let snapshot_path = runtime_index_snapshot_path(data_dir, table_stream_id);
    let bytes = read_bytes(&snapshot_path).ok()?;

    if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
        return None;
    }

    let (snapshot, legacy_plain_encoding): (RuntimeIndexTableSnapshot, bool) =
        decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

    let schema_fingerprint = table_schema_fingerprint(table)?;

    if snapshot.table_id != table.table_id
        || snapshot.schema_fingerprint != schema_fingerprint
    {
        return None;
    }

    #[expect(clippy::question_mark, reason="we want to return None if the wal fingerprint is unavailable")]
    let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
        return None;
    };

    if snapshot.wal_size_bytes != wal_size_bytes
        || snapshot.wal_modified_epoch_ms != wal_modified_epoch_ms
    {
        return None;
    }

    let snapshot_index_ids = snapshot
        .indexes
        .iter()
        .map(|index| index.index_id.as_str())
        .collect::<HashSet<_>>();

    if tracked_indexes
        .iter()
        .any(|index| !snapshot_index_ids.contains(index.index_id.0.as_str()))
    {
        return None;
    }

    Some(LoadedRuntimeIndexSnapshot {
        snapshot,
        legacy_plain_encoding,
    })

}

fn load_live_row_checkpoint(
    data_dir: &Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    wal_fingerprint: Option<(u64, u64)>,
) -> Option<TableLiveRowCheckpoint> {

    let checkpoint_path = live_row_checkpoint_path(data_dir, table_stream_id);
    let bytes = read_bytes(&checkpoint_path).ok()?;

    if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
        return None;
    }

    let (checkpoint, _legacy_plain_encoding): (TableLiveRowCheckpoint, bool) =
        decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

    let schema_fingerprint = table_schema_fingerprint(table)?;

    if checkpoint.table_id != table.table_id || checkpoint.schema_fingerprint != schema_fingerprint {
        return None;
    }

    #[expect(clippy::question_mark, reason="we want to return None if the wal fingerprint is unavailable")]
    let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
        return None;
    };

    if checkpoint.wal_size_bytes != wal_size_bytes
        || checkpoint.wal_modified_epoch_ms != wal_modified_epoch_ms
    {
        return None;
    }

    Some(checkpoint)

}

fn load_accessor_cache_snapshot(
    data_dir: &Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    wal_fingerprint: Option<(u64, u64)>,
    warm_fields: &[String],
) -> Option<TableAccessorCacheSnapshot> {

    let snapshot_path = accessor_cache_snapshot_path(data_dir, table_stream_id);
    let bytes = read_bytes(&snapshot_path).ok()?;

    if verify_header(FileKind::Entity, &bytes).is_err() || bytes.len() <= HEADER_SIZE {
        return None;
    }

    let (snapshot, _legacy_plain_encoding): (TableAccessorCacheSnapshot, bool) =
        decode_snapshot_payload(&bytes[HEADER_SIZE..])?;

    let schema_fingerprint = table_schema_fingerprint(table)?;

    if snapshot.table_id != table.table_id || snapshot.schema_fingerprint != schema_fingerprint {
        return None;
    }

    #[expect(clippy::question_mark, reason="we want to return None if the wal fingerprint is unavailable")]
    let Some((wal_size_bytes, wal_modified_epoch_ms)) = wal_fingerprint else {
        return None;
    };

    if snapshot.wal_size_bytes != wal_size_bytes
        || snapshot.wal_modified_epoch_ms != wal_modified_epoch_ms
    {
        return None;
    }

    if !warm_fields
        .iter()
        .all(|field_name| snapshot.warm_fields.iter().any(|saved| saved == field_name))
    {
        return None;
    }

    Some(snapshot)

}

#[expect(clippy::too_many_arguments, reason="this function needs many arguments to save the runtime index snapshot")]
fn save_runtime_index_snapshot(
    data_dir: &Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    latest_tx_id: u64,
    live_row_count: usize,
    wal_fingerprint: Option<(u64, u64)>,
    tracked_indexes: &[DatabaseIndex],
    store: &RuntimeIndexStore,
) -> Result<(), String> {

    let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
        .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

    let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

    if wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
        return Err("wal fingerprint changed before snapshot write".to_string());
    }

    let schema_fingerprint = table_schema_fingerprint(table)
        .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

    let mut indexes = Vec::with_capacity(tracked_indexes.len());

    for index in tracked_indexes {
        let state = store
            .index(&index.index_id.0)
            .ok_or_else(|| format!("missing runtime index state '{}'", index.index_id.0))?;

        indexes.push(RuntimeIndexSnapshotIndex {
            index_id: index.index_id.0.clone(),
            entries: state.entries.iter().cloned().collect(),
        });
    }

    let snapshot = RuntimeIndexTableSnapshot {
        table_id: table.table_id.clone(),
        latest_tx_id,
        schema_fingerprint,
        live_row_count,
        wal_size_bytes,
        wal_modified_epoch_ms,
        indexes,
    };

    let mut content = make_header(FileKind::Entity).to_vec();
    let payload = encode_snapshot_payload(&snapshot)?;
    content.extend_from_slice(&payload);

    let snapshot_path = runtime_index_snapshot_path(data_dir, table_stream_id);
    write_bytes_atomic(&snapshot_path, &content)
        .map_err(|err| format!("snapshot write failed: {err}"))?;

    if wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
        let _ = fs::remove_file(&snapshot_path);
        return Err("wal fingerprint changed after snapshot write".to_string());
    }

    Ok(())

}

fn save_live_row_checkpoint(
    data_dir: &Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    latest_tx_id: u64,
    wal_fingerprint: Option<(u64, u64)>,
    live_rows: &[(u64, HashMap<String, Vec<u8>>)],
) -> Result<(), String> {

    let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
        .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

    let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

    if wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
        return Err("wal fingerprint changed before live-row checkpoint write".to_string());
    }

    let schema_fingerprint = table_schema_fingerprint(table)
        .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

    let checkpoint = TableLiveRowCheckpoint {
        table_id: table.table_id.clone(),
        latest_tx_id,
        schema_fingerprint,
        wal_size_bytes,
        wal_modified_epoch_ms,
        live_rows: live_rows.to_vec(),
    };

    let mut content = make_header(FileKind::Entity).to_vec();
    let payload = encode_snapshot_payload(&checkpoint)?;
    content.extend_from_slice(&payload);

    let checkpoint_path = live_row_checkpoint_path(data_dir, table_stream_id);
    write_bytes_atomic(&checkpoint_path, &content)
        .map_err(|err| format!("live-row checkpoint write failed: {err}"))?;

    if wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
        let _ = fs::remove_file(&checkpoint_path);
        return Err("wal fingerprint changed after live-row checkpoint write".to_string());
    }

    Ok(())

}

fn save_accessor_cache_snapshot(
    data_dir: &Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    latest_tx_id: u64,
    wal_fingerprint: Option<(u64, u64)>,
    warm_fields: &[String],
    cache_scope_id: usize,
) -> Result<(), String> {

    let (wal_size_bytes, wal_modified_epoch_ms) = wal_fingerprint
        .ok_or_else(|| "wal fingerprint unavailable".to_string())?;

    let expected_wal_fingerprint = (wal_size_bytes, wal_modified_epoch_ms);

    if wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
        return Err("wal fingerprint changed before accessor cache snapshot write".to_string());
    }

    let schema_fingerprint = table_schema_fingerprint(table)
        .ok_or_else(|| "schema fingerprint serialization failed".to_string())?;

    let cache = snapshot_equality_cache(cache_scope_id, table_stream_id)
        .ok_or_else(|| "equality cache snapshot missing".to_string())?;

    let snapshot = TableAccessorCacheSnapshot {
        table_id: table.table_id.clone(),
        latest_tx_id,
        schema_fingerprint,
        wal_size_bytes,
        wal_modified_epoch_ms,
        live_row_count: cache.rows_by_id.len(),
        warm_fields: warm_fields.to_vec(),
        cache,
    };

    let mut content = make_header(FileKind::Entity).to_vec();
    let payload = encode_snapshot_payload(&snapshot)?;
    content.extend_from_slice(&payload);

    let snapshot_path = accessor_cache_snapshot_path(data_dir, table_stream_id);
    write_bytes_atomic(&snapshot_path, &content)
        .map_err(|err| format!("accessor cache snapshot write failed: {err}"))?;

    if wal_stream_fingerprint(data_dir, table_stream_id) != Some(expected_wal_fingerprint) {
        let _ = fs::remove_file(&snapshot_path);
        return Err("wal fingerprint changed after accessor cache snapshot write".to_string());
    }

    Ok(())
    
}

#[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows)")]
fn build_bootstrap_index_entries(
    tracked_indexes: &[DatabaseIndex],
    live_rows: &[(u64, HashMap<String, Vec<u8>>)],
) -> Vec<(String, DatabaseIndex, AHashSet<Vec<Vec<u8>>>)> {

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let should_parallel = available > 1
        && tracked_indexes.len() > 1
        && live_rows.len() >= RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS;

    if !should_parallel {

        return tracked_indexes
            .iter()
            .map(|index| {
                let mut entries = AHashSet::with_capacity(live_rows.len());
                for (_, row_map) in live_rows {
                    entries.insert(index_value_tuple(index, row_map));
                }
                (index.index_id.0.clone(), index.clone(), entries)
            })
            .collect();

    }

    let workers = std::cmp::min(
        std::cmp::min(available, runtime_index_parallel_build_max_workers()),
        tracked_indexes.len(),
    );

    let chunk_size = tracked_indexes.len().div_ceil(workers);

    let mut chunks = std::thread::scope(|scope| {

        let mut handles = Vec::new();

        for worker_idx in 0..workers {
            
            let start = worker_idx * chunk_size;
            if start >= tracked_indexes.len() {
                break;
            }

            let end = std::cmp::min(start + chunk_size, tracked_indexes.len());
            let indexes = &tracked_indexes[start..end];

            handles.push(scope.spawn(move || {

                let mut chunk = Vec::with_capacity(indexes.len());
                
                for index in indexes {

                    let mut entries = AHashSet::with_capacity(live_rows.len());
                    for (_, row_map) in live_rows {
                        entries.insert(index_value_tuple(index, row_map));
                    }
                    chunk.push((index.index_id.0.clone(), index.clone(), entries));
                
                }
                
                (start, chunk)

            }));

        }

        let mut out = Vec::with_capacity(handles.len());
        
        for handle in handles {
            if let Ok(chunk) = handle.join() {
                out.push(chunk);
            }
        }
        
        out

    });

    chunks.sort_by_key(|(start, _)| *start);

    let mut rebuilt = Vec::with_capacity(tracked_indexes.len());

    for (_, mut chunk) in chunks {
        rebuilt.append(&mut chunk);
    }

    rebuilt

}

#[expect(clippy::type_complexity, reason="returning a tuple of (index_id, index, entries)")]
fn build_snapshot_index_entries(
    tracked_indexes: &[DatabaseIndex],
    snapshot: &RuntimeIndexTableSnapshot,
) -> Vec<(String, DatabaseIndex, AHashSet<Vec<Vec<u8>>>)> {

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let should_parallel = available > 1
        && tracked_indexes.len() > 1
        && snapshot.live_row_count >= RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS;

    if !should_parallel {
        return tracked_indexes
            .iter()
            .filter_map(|index| {

                snapshot
                    .indexes
                    .iter()
                    .find(|item| item.index_id == index.index_id.0)
                    .map(|item| {
                        (
                            index.index_id.0.clone(),
                            index.clone(),
                            item.entries.iter().cloned().collect::<AHashSet<_>>(),
                        )
                    })

            })
            .collect();
    }

    let workers = std::cmp::min(
        std::cmp::min(available, runtime_index_parallel_build_max_workers()),
        tracked_indexes.len(),
    );

    let chunk_size = tracked_indexes.len().div_ceil(workers);

    let mut chunks = std::thread::scope(|scope| {
        
        let mut handles = Vec::new();

        for worker_idx in 0..workers {

            let start = worker_idx * chunk_size;
            if start >= tracked_indexes.len() {
                break;
            }

            let end = std::cmp::min(start + chunk_size, tracked_indexes.len());
            let indexes = &tracked_indexes[start..end];

            handles.push(scope.spawn(move || {

                let mut chunk = Vec::with_capacity(indexes.len());

                for index in indexes {
                    let Some(item) = snapshot
                        .indexes
                        .iter()
                        .find(|item| item.index_id == index.index_id.0) else {
                        continue;
                    };

                    chunk.push((
                        index.index_id.0.clone(),
                        index.clone(),
                        item.entries.iter().cloned().collect::<AHashSet<_>>(),
                    ));
                }

                (start, chunk)
                
            }));
        }

        let mut out = Vec::with_capacity(handles.len());

        for handle in handles {
            if let Ok(chunk) = handle.join() {
                out.push(chunk);
            }
        }

        out
        
    });

    chunks.sort_by_key(|(start, _)| *start);

    let mut rebuilt = Vec::with_capacity(tracked_indexes.len());

    for (_, mut chunk) in chunks {
        rebuilt.append(&mut chunk);
    }

    rebuilt

}

impl Default for RuntimeIndexStore {
    
    fn default() -> Self {
        Self::new()
    }

}

fn runtime_index_materialize_non_primary() -> bool {

    std::env::var("DISTDB_RUNTIME_INDEX_MATERIALIZE_NON_PRIMARY")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(true)

}

fn runtime_index_non_primary_field_allowlist() -> AHashSet<String> {
    parse_runtime_index_allowlist_env("DISTDB_RUNTIME_INDEX_NON_PRIMARY_FIELDS")
}

fn runtime_index_non_primary_index_allowlist() -> AHashSet<String> {
    parse_runtime_index_allowlist_env("DISTDB_RUNTIME_INDEX_NON_PRIMARY_INDEX_IDS")
}

fn parse_runtime_index_allowlist_entries(value: &str) -> AHashSet<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| common::normalize_identifier!(entry))
        .collect()
}

fn parse_runtime_index_allowlist_env(var_name: &str) -> AHashSet<String> {

    let Some(value) = std::env::var(var_name).ok() else {
        return AHashSet::new();
    };

    parse_runtime_index_allowlist_entries(&value)

}

pub fn index_value_tuple(index: &DatabaseIndex, row_map: &HashMap<String, Vec<u8>>) -> Vec<Vec<u8>> {

    let mut values = Vec::with_capacity(if index.field_names.is_empty() { 1 } else { index.field_names.len() });

    if index.field_names.is_empty() && !index.field_name.is_empty() {
        values.push(row_map.get(&index.field_name).cloned().unwrap_or_default());
        return values;
    }

    for field_name in &index.field_names {
        values.push(row_map.get(field_name).cloned().unwrap_or_default());
    }

    values

}

pub fn primary_key_index(table: &DatabaseTable) -> Option<&DatabaseIndex> {

    table
        .indexes
        .values()
        .find(|index| index.is_primary_key())
        .or_else(|| {
            table
                .indexes
                .values()
                .find(|index| index.index_id.0.to_ascii_lowercase().starts_with("pri:"))
        })
        
}


// pub fn primary_key_index<'a>(table: &'a DatabaseTable) -> Option<&'a DatabaseIndex> {
//     table.indexes.values().find(|index| index.is_primary_key())
// }

pub fn derived_indexes_for_table(table: &DatabaseTable) -> impl Iterator<Item = &DatabaseIndex> + '_ {
    table.indexes.values().filter(|index| !matches!(index.origin, DatabaseIndexOrigin::Temporary))
}

#[cfg(test)]
#[path = "runtime_index_test.rs"]
mod tests;
