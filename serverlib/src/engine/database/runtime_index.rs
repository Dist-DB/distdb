use ahash::{AHashMap, AHashSet};
use common::epoch_ms;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use super::runtime_index_snapshot::{
    RuntimeIndexSnapshotIndex,
    RuntimeIndexSnapshotService,
    RuntimeIndexTableSnapshot,
};
use super::table::DatabaseTable;
use crate::engine::execution::access::{
    load_live_rows_in_place,
    warm_string_like_cache_for_fields,
};
use crate::{
    restore_equality_cache_from_snapshot,
    warm_equality_cache_from_live_rows, ConcurrentWalManager, DatabaseCatalog, DatabaseIndex, DatabaseIndexOrigin,
    TransactionKind,
};

const RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS: usize = 1_000_000;
const RUNTIME_INDEX_PARALLEL_BUILD_MAX_WORKERS: usize = 1;
const RUNTIME_INDEX_BOOTSTRAP_LIVE_ROW_CHECKPOINT_MAX_ROWS_DEFAULT: usize = 0;
const RUNTIME_INDEX_BOOTSTRAP_INDEX_BUILD_CHUNK_ROWS_DEFAULT: usize = 65_536;
static RUNTIME_INDEX_BOOTSTRAP_PROGRESS: OnceLock<Mutex<RuntimeIndexBootstrapProgress>> = OnceLock::new();

#[derive(Debug, Clone, Default)]
pub struct RuntimeIndexBootstrapProgress {
    pub phase: String,
    pub tables_total: usize,
    pub tables_completed: usize,
    pub current_database_id: String,
    pub current_table_id: String,
    pub current_table_started_epoch_ms: u64,
    pub done: bool,
    pub started_epoch_ms: u64,
    pub last_update_epoch_ms: u64,
}

fn runtime_index_bootstrap_progress_store() -> &'static Mutex<RuntimeIndexBootstrapProgress> {
    RUNTIME_INDEX_BOOTSTRAP_PROGRESS
        .get_or_init(|| Mutex::new(RuntimeIndexBootstrapProgress::default()))
}

fn set_runtime_index_bootstrap_progress(
    mut update: impl FnMut(&mut RuntimeIndexBootstrapProgress),
) {
    if let Ok(mut guard) = runtime_index_bootstrap_progress_store().lock() {
        update(&mut guard);
    }
}

fn mark_runtime_index_bootstrap_table_complete() {
    set_runtime_index_bootstrap_progress(|progress| {
        progress.tables_completed = progress.tables_completed.saturating_add(1);
        progress.current_database_id.clear();
        progress.current_table_id.clear();
        progress.current_table_started_epoch_ms = 0;
        progress.last_update_epoch_ms = epoch_ms!();
    });
}

pub fn current_runtime_index_bootstrap_progress() -> RuntimeIndexBootstrapProgress {

    runtime_index_bootstrap_progress_store()
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default()

}

fn runtime_index_parallel_build_max_workers() -> usize {

    std::env::var("DISTDB_RUNTIME_INDEX_BUILD_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(RUNTIME_INDEX_PARALLEL_BUILD_MAX_WORKERS)

}

fn runtime_index_parallel_build_min_rows() -> usize {

    std::env::var("DISTDB_RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS)

}

fn runtime_index_aggressive_reserve_growth() -> bool {

    std::env::var("DISTDB_RUNTIME_INDEX_AGGRESSIVE_RESERVE_GROWTH")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)

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

fn runtime_index_incremental_persistence_large_table_interval_ms(
    live_row_count: usize,
) -> u64 {

    if live_row_count >= 750_000 {
        300_000
    } else if live_row_count >= 250_000 {
        60_000
    } else if live_row_count >= 100_000 {
        15_000
    } else {
        0
    }

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
        .unwrap_or(false)
        
}

fn runtime_index_bootstrap_accessor_preload_max_live_rows() -> usize {

    std::env::var("DISTDB_RUNTIME_INDEX_BOOTSTRAP_ACCESSOR_PRELOAD_MAX_LIVE_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(150_000)

}

fn runtime_index_background_prewarm_skipped_accessors() -> bool {

    std::env::var("DISTDB_RUNTIME_INDEX_BACKGROUND_PREWARM_SKIPPED_ACCESSORS")
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)

}

fn runtime_index_bootstrap_live_row_checkpoint_max_rows() -> usize {

    std::env::var("DISTDB_RUNTIME_INDEX_BOOTSTRAP_LIVE_ROW_CHECKPOINT_MAX_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(RUNTIME_INDEX_BOOTSTRAP_LIVE_ROW_CHECKPOINT_MAX_ROWS_DEFAULT)

}

fn runtime_index_bootstrap_index_build_chunk_rows() -> usize {

    std::env::var("DISTDB_RUNTIME_INDEX_BOOTSTRAP_INDEX_BUILD_CHUNK_ROWS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(RUNTIME_INDEX_BOOTSTRAP_INDEX_BUILD_CHUNK_ROWS_DEFAULT)

}

fn should_preload_accessors_for_bootstrap(live_row_count: usize) -> bool {
    live_row_count <= runtime_index_bootstrap_accessor_preload_max_live_rows()
}

fn spawn_background_accessor_prewarm_from_checkpoint(
    data_dir: std::path::PathBuf,
    cache_scope_id: usize,
    database_id: String,
    table_id: String,
    table_stream_id: String,
    schema: crate::TableSchema,
    warm_fields: Vec<String>,
) {

    if warm_fields.is_empty() {
        return;
    }

    std::thread::spawn(move || {

        let started_at = Instant::now();

        let Some((latest_tx_id, live_rows)) = RuntimeIndexSnapshotService::load_live_row_checkpoint_rows(
            &data_dir,
            &table_stream_id,
            &table_id,
            &schema,
        ) else {
            log::debug!(
                "runtime index background accessor prewarm skipped database={} table={} reason=live_row_checkpoint_unavailable",
                database_id,
                table_id,
            );
            return;
        };

        let load_elapsed_ms = started_at.elapsed().as_millis();
        let live_row_count = live_rows.len();

        warm_equality_cache_from_live_rows(
            cache_scope_id,
            &table_stream_id,
            &schema,
            latest_tx_id,
            live_rows,
            &warm_fields,
        );

        let elapsed_ms = started_at.elapsed().as_millis();

        log::info!(
            "runtime index background accessor prewarm complete database={} table={} source=live_row_checkpoint live_rows={} load_ms={} elapsed_ms={}",
            database_id,
            table_id,
            live_row_count,
            load_elapsed_ms,
            elapsed_ms,
        );

    });

}

/// In-memory state for a single index.
/// Each entry is a composite key tuple in the index's field order.
#[derive(Debug, Clone, Default)]
pub struct RuntimeIndexState {
    pub index: Option<DatabaseIndex>,
    entries: AHashMap<Vec<Vec<u8>>, Option<u64>>,
}

struct RuntimeIndexRebuildItem {
    index_id: String,
    entries: AHashSet<Vec<Vec<u8>>>,
    row_refs: AHashMap<Vec<Vec<u8>>, u64>,
}

impl RuntimeIndexState {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn contains(&self, pk_val: &[Vec<u8>]) -> bool {
        self.entries.contains_key(pk_val)
    }

    pub fn insert(&mut self, pk_val: Vec<Vec<u8>>) {
        self.insert_with_row_ref(pk_val, None);
    }

    pub fn insert_with_row_ref(&mut self, pk_val: Vec<Vec<u8>>, row_ref: Option<u64>) {
        let stored_row_ref = if self
            .index
            .as_ref()
            .map(|index| index.is_unique_key())
            .unwrap_or(true)
        {
            row_ref
        } else {
            None
        };

        self.entries.insert(pk_val, stored_row_ref);
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
        self.entries = entries.into_iter().map(|key| (key, None)).collect();
    }

    pub fn rebuild_with_row_refs(
        &mut self,
        entries: AHashSet<Vec<Vec<u8>>>,
        mut row_refs: AHashMap<Vec<Vec<u8>>, u64>,
    ) {
        row_refs.retain(|key, _| entries.contains(key));
        self.entries = entries
            .into_iter()
            .map(|key| {
                let row_ref = row_refs.get(&key).copied();
                let stored_row_ref = if self
                    .index
                    .as_ref()
                    .is_some_and(|index| index.is_unique_key())
                {
                    row_ref
                } else {
                    None
                };
                (key, stored_row_ref)
            })
            .collect();
    }

    pub fn row_ref(&self, pk_val: &[Vec<u8>]) -> Option<u64> {
        self.entries.get(pk_val).copied().flatten()
    }

    pub fn reserve_entries(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }

        // Keep reserve decisions conservative: reserve when required capacity
        // would be exceeded, and only apply extra runway for large batches.
        // This avoids triggering expensive doublings on small tail batches.
        let len = self.entries.len();
        let required = len.saturating_add(additional);
        let capacity = self.entries.capacity();
        let spare = capacity.saturating_sub(len);

        let high_ingest_batch = additional >= 64;
        let near_capacity = capacity > 0
            && len.saturating_mul(10) >= capacity.saturating_mul(9);

        let is_unique_key_index = self
            .index
            .as_ref()
            .is_some_and(|index| index.is_unique_key());

        // For sustained bulk ingest on large unique-key indexes, grow before
        // hitting the near-full threshold so rehash happens on smaller maps.
        let proactive_growth = runtime_index_aggressive_reserve_growth()
            && is_unique_key_index
            && high_ingest_batch
            && capacity >= 16_384
            && len.saturating_mul(100) >= capacity.saturating_mul(55);

        // Keep large unique-key indexes below dense occupancy during sustained
        // ingest so expensive rehashes happen less often and on smaller states.
        let proactive_target_capacity = if proactive_growth {
            let target_load_percent = if capacity >= 229_376 {
                40usize
            } else if capacity >= 114_688 {
                50usize
            } else {
                60usize
            };

            required
                .saturating_mul(100)
                .checked_div(target_load_percent)
                .unwrap_or(usize::MAX)
        } else {
            required
        };

        let proactive_skip_capacity = if proactive_growth {
            if capacity >= 229_376 {
                capacity.saturating_mul(4)
            } else if capacity >= 57_344 {
                capacity.saturating_mul(3)
            } else {
                capacity.saturating_mul(2)
            }
        } else {
            0
        };

        let should_add_runway = additional >= 1_024
            || (high_ingest_batch && near_capacity)
            || proactive_growth;

        let desired_runway = if should_add_runway {
            if proactive_growth {
                if capacity >= 229_376 {
                    capacity.clamp(131_072, 2_097_152)
                } else if capacity >= 114_688 {
                    capacity.clamp(65_536, 1_048_576)
                } else {
                    capacity.clamp(16_384, 262_144)
                }
            } else if high_ingest_batch && near_capacity {
                // Under sustained large-batch ingest near load threshold, reserve
                // a wider runway to avoid repeated tier-by-tier rehash cliffs.
                capacity.clamp(4_096, 32_768)
            } else {
                additional.saturating_mul(2).clamp(4_096, 32_768)
            }
        } else {
            0
        };

        if capacity < required || (should_add_runway && spare < desired_runway) {
            let target = required
                .saturating_add(desired_runway)
                .max(proactive_target_capacity)
                .max(proactive_skip_capacity);
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
    let mut scoped = String::with_capacity(table_scope_id.len() + 2 + index_id.len());
    scoped.push_str(table_scope_id);
    scoped.push_str("::");
    scoped.push_str(index_id);
    scoped
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

    fn should_track_non_primary_index(&self, index: &DatabaseIndex) -> bool {

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

    pub fn new() -> Self {

        Self {
            indexes: AHashMap::new(),
            materialize_non_primary: runtime_index_materialize_non_primary(),
            non_primary_field_allowlist: runtime_index_non_primary_field_allowlist(),
            non_primary_index_allowlist: runtime_index_non_primary_index_allowlist(),
            incremental_persist_last_saved_ms: AHashMap::new(),
        }

    }

    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    pub fn should_track_index(&self, index: &DatabaseIndex) -> bool {
        
        if index.is_temporary() {
            return false;
        }

        if index.is_unique_key() {
            return true;
        }

        self.should_track_non_primary_index(index)
        
    }

    fn should_materialize_index_for_bootstrap(&self, index: &DatabaseIndex) -> bool {

        if index.is_unique_key() {
            return true;
        }

        self.should_track_non_primary_index(index)

    }

    pub fn index(&self, index_id: &str) -> Option<&RuntimeIndexState> {
        self.indexes.get(index_id)
    }

    pub fn index_for_table(&self, table_scope_id: &str, index_id: &str) -> Option<&RuntimeIndexState> {
        let scoped = scoped_index_id(table_scope_id, index_id);
        self.indexes.get(&scoped)
    }

    pub fn find_scoped_index_state_for_lookup<'a>(
        &'a self,
        index_id: &str,
        lookup_key: &[Vec<u8>],
    ) -> Option<(&'a str, &'a RuntimeIndexState)> {

        self.indexes
            .iter()
            .filter_map(|(scoped_id, state)| {
                scoped_id
                    .rsplit_once("::")
                    .filter(|(_, scoped_index_id)| *scoped_index_id == index_id)
                    .map(|(scope_id, _)| (scope_id, state))
            })
            .find(|(_, state)| state.contains(lookup_key))

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
        let mut prefix = String::with_capacity(table_scope_id.len() + 2);
        prefix.push_str(table_scope_id);
        prefix.push_str("::");
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
            entries: AHashMap::new(),
        });

    }

    pub fn register_index_for_table(&mut self, table_scope_id: &str, index: &DatabaseIndex) {

        if !self.should_track_index(index) {
            return;
        }

        let index_id = scoped_index_id(table_scope_id, &index.index_id.0);
        self.indexes.entry(index_id).or_insert_with(|| RuntimeIndexState {
            index: Some(index.clone()),
            entries: AHashMap::new(),
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
        row_ref: Option<u64>,
    ) {

        if !self.should_track_index(index) {
            return;
        }

        let key = index_value_tuple(index, row_map);
        let state = self.index_mut_for_table(table_scope_id, &index.index_id.0);
        state.index = Some(index.clone());
        state.insert_with_row_ref(key, row_ref);

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
        row_ref: Option<u64>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            let key = index_value_tuple(index, row_map);
            let state = self.index_mut_for_table(table_scope_id, &index.index_id.0);
            state.index = Some(index.clone());
            state.insert_with_row_ref(key, row_ref);

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
            state.index = Some((*index).clone());

            state.reserve_entries(row_maps.len());

            for row_map in row_maps {
                let key = index_value_tuple(index, row_map.borrow());
                state.insert(key);
            }
        
        }

    }

    pub fn record_table_rows_batch_with_first_row_ref<R>(
        &mut self,
        table_scope_id: &str,
        indexes: &[&DatabaseIndex],
        first_row_ref: u64,
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
            state.index = Some((*index).clone());

            state.reserve_entries(row_maps.len());

            let mut row_ref = first_row_ref;
            for row_map in row_maps {
                let key = index_value_tuple(index, row_map.borrow());
                state.insert_with_row_ref(key, Some(row_ref));
                row_ref = row_ref.saturating_add(1);
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

            let mut key_scratch = Vec::with_capacity(if index.field_names.is_empty() {
                1
            } else {
                index.field_names.len()
            });

            for row_map in row_maps {
                write_index_value_tuple(index, row_map.borrow(), &mut key_scratch);
                state.remove(&key_scratch);
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
        latest_tx_id: u64,
        row_map: &HashMap<String, Vec<u8>>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {

        match kind {
            
            TransactionKind::Ignore => {},

            TransactionKind::Delete => self.remove_table_row_for_table(table_scope_id, indexes, row_map),

            TransactionKind::Insert |
            TransactionKind::Update => {
                self.record_table_row_for_table(table_scope_id, indexes, row_map, Some(latest_tx_id))
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
        let preload_accessors_on_bootstrap = runtime_index_preload_accessors_on_bootstrap();

        log::info!(
            "runtime index bootstrap mode materialize_non_primary={} preload_accessors_on_bootstrap={} preload_accessor_max_live_rows={} warm_equality_cache_on_bootstrap={} non_primary_field_allowlist={} non_primary_index_allowlist={}",
            self.materialize_non_primary,
            preload_accessors_on_bootstrap,
            runtime_index_bootstrap_accessor_preload_max_live_rows(),
            preload_accessors_on_bootstrap,
            
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

        let tables_total = catalogs
            .values()
            .map(|catalog| catalog.table_ids().len())
            .sum::<usize>();

        set_runtime_index_bootstrap_progress(|progress| {
            let now = epoch_ms!();
            progress.phase = "runtime_index_bootstrap".to_string();
            progress.tables_total = tables_total;
            progress.tables_completed = 0;
            progress.current_database_id.clear();
            progress.current_table_id.clear();
            progress.current_table_started_epoch_ms = 0;
            progress.done = false;
            progress.started_epoch_ms = now;
            progress.last_update_epoch_ms = now;
        });

        let snapshot_data_dir = wal.data_dir_path();

        for (database_id, catalog) in catalogs {
            
            for table_id in catalog.table_ids() {

                set_runtime_index_bootstrap_progress(|progress| {
                    let now = epoch_ms!();
                    progress.current_database_id.clone_from(database_id);
                    progress.current_table_id.clone_from(&table_id);
                    progress.current_table_started_epoch_ms = now;
                    progress.last_update_epoch_ms = now;
                });

                let table_started_at = Instant::now();

                let Some(table) = catalog
                    .table_handle(&table_id)
                    .and_then(|handle| handle.table_snapshot()) else {
                    mark_runtime_index_bootstrap_table_complete();
                    continue;
                };

                let table_stream_id = resolve_table_stream_id_for_bootstrap(catalog, &table_id, wal);
                if table.indexes.is_empty() {
                    mark_runtime_index_bootstrap_table_complete();
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
                    mark_runtime_index_bootstrap_table_complete();
                    continue;
                }

                for index in &tracked_indexes {
                    self.register_index_for_table(&table_stream_id, index);
                }

                let wal_fingerprint = snapshot_data_dir
                    .as_ref()
                    .and_then(|data_dir| RuntimeIndexSnapshotService::wal_stream_fingerprint(data_dir, &table_stream_id));

                let mut warm_fields = Vec::with_capacity(tracked_indexes.len());

                for index in &tracked_indexes {
                    if index.field_names.len() == 1 {
                        let normalized = common::normalize_identifier!(&index.field_names[0]);
                        if !normalized.is_empty() {
                            warm_fields.push(normalized);
                        }
                    } else if index.field_names.is_empty() && !index.field_name.is_empty() {
                        let normalized = common::normalize_identifier!(&index.field_name);
                        if !normalized.is_empty() {
                            warm_fields.push(normalized);
                        }
                    }
                }

                warm_fields.sort();
                warm_fields.dedup();

                if let Some(snapshot_info) = snapshot_data_dir
                    .as_ref()
                    .and_then(|data_dir| {
                        RuntimeIndexSnapshotService::load_runtime_index_snapshot(
                            data_dir,
                            &table,
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
                        .map(|item| item.entries.len())
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

                    for item in restored {
                        let state = self.index_mut_for_table(&table_stream_id, &item.index_id);
                        state.index = tracked_indexes
                            .iter()
                            .find(|index| index.index_id.0 == item.index_id)
                            .cloned();
                        state.rebuild_with_row_refs(item.entries, item.row_refs);
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
                        let _ = persist_runtime_index_snapshot(
                            self,
                            data_dir,
                            &table,
                            &table_stream_id,
                            snapshot.latest_tx_id,
                            snapshot.live_row_count,
                            wal_fingerprint,
                            &tracked_indexes,
                        );
                    } else if snapshot_info.legacy_plain_encoding {
                        log::info!(
                            "runtime index legacy snapshot detected table={} migration_deferred=true env=DISTDB_RUNTIME_INDEX_MIGRATE_LEGACY_ON_BOOTSTRAP",
                            table_id,
                        );
                    }

                    if preload_accessors_on_bootstrap && !warm_fields.is_empty() {

                        if !should_preload_accessors_for_bootstrap(snapshot.live_row_count) {
                            log::info!(
                                "runtime index bootstrap accessor preload skipped database={} table={} live_rows={} max_live_rows={}",
                                database_id,
                                table_id,
                                snapshot.live_row_count,
                                runtime_index_bootstrap_accessor_preload_max_live_rows(),
                            );

                            if runtime_index_background_prewarm_skipped_accessors()
                                && let Some(data_dir) = snapshot_data_dir.as_ref()
                            {
                                spawn_background_accessor_prewarm_from_checkpoint(
                                    data_dir.clone(),
                                    wal.cache_scope_id(),
                                    database_id.to_string(),
                                    table_id.clone(),
                                    table_stream_id.clone(),
                                    table.schema.clone(),
                                    warm_fields.clone(),
                                );

                                log::info!(
                                    "runtime index bootstrap accessor background prewarm scheduled database={} table={} source=live_row_checkpoint warm_fields={}",
                                    database_id,
                                    table_id,
                                    warm_fields.len(),
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

                            mark_runtime_index_bootstrap_table_complete();

                            continue;
                        }
                        
                        let preload_started_at = Instant::now();

                        if let Some(data_dir) = snapshot_data_dir.as_ref()
                            && let Some(accessor_snapshot) = RuntimeIndexSnapshotService::load_accessor_cache_snapshot(
                                data_dir,
                                &table,
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

                            mark_runtime_index_bootstrap_table_complete();

                            continue;

                        }

                        let (latest_tx_id, live_rows, source, load_elapsed_ms) =
                            load_bootstrap_live_rows(
                                snapshot_data_dir.as_ref(),
                                wal,
                                &table,
                                &table_stream_id,
                                wal_fingerprint,
                                snapshot.latest_tx_id,
                            );

                        persist_live_row_checkpoint_if_from_wal(
                            snapshot_data_dir.as_ref(),
                            &table,
                            &table_stream_id,
                            latest_tx_id,
                            wal_fingerprint,
                            source,
                            &live_rows,
                            &table_id,
                        );

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
                            && let Err(err) = RuntimeIndexSnapshotService::save_accessor_cache_snapshot(
                                data_dir,
                                &table,
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

                    mark_runtime_index_bootstrap_table_complete();

                    continue;

                }

                let latest_tx_id = wal
                    .latest_transaction_id(&table_stream_id)
                    .map(|tx| tx.0)
                    .unwrap_or(0);

                let (latest_tx_id, live_rows, live_rows_mode, live_rows_elapsed_ms) =
                    load_bootstrap_live_rows(
                        snapshot_data_dir.as_ref(),
                        wal,
                        &table,
                        &table_stream_id,
                        wal_fingerprint,
                        latest_tx_id,
                    );
                    
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
                rebuild_bootstrap_indexes_from_live_rows(
                    self,
                    &table_stream_id,
                    &tracked_indexes,
                    &live_rows,
                );
                let rebuild_elapsed_ms = rebuild_started_at.elapsed().as_millis();

                persist_live_row_checkpoint_if_from_wal(
                    snapshot_data_dir.as_ref(),
                    &table,
                    &table_stream_id,
                    latest_tx_id,
                    wal_fingerprint,
                    live_rows_mode,
                    &live_rows,
                    &table_id,
                );

                let warm_elapsed_ms = if preload_accessors_on_bootstrap
                    && should_preload_accessors_for_bootstrap(live_row_count)
                {
                    let warm_started_at = Instant::now();
                    warm_equality_cache_from_live_rows(
                        wal.cache_scope_id(),
                        &table_stream_id,
                        &table.schema,
                        latest_tx_id,
                        live_rows,
                        &warm_fields,
                    );

                    if let Some(data_dir) = snapshot_data_dir.as_ref()
                        && let Err(err) = RuntimeIndexSnapshotService::save_accessor_cache_snapshot(
                            data_dir,
                            &table,
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

                    warm_started_at.elapsed().as_millis()
                } else {
                    if !preload_accessors_on_bootstrap {
                        log::debug!(
                            "runtime index bootstrap equality warm skipped database={} table={} reason=preload_disabled",
                            database_id,
                            table_id,
                        );
                    } else {
                        log::info!(
                            "runtime index bootstrap equality warm skipped database={} table={} live_rows={} max_live_rows={}",
                            database_id,
                            table_id,
                            live_row_count,
                            runtime_index_bootstrap_accessor_preload_max_live_rows(),
                        );
                    }
                    0
                };

                if let Some(data_dir) = snapshot_data_dir.as_ref()
                    && let Err(err) = persist_runtime_index_snapshot(
                        self,
                        data_dir,
                        &table,
                        &table_stream_id,
                        latest_tx_id,
                        live_row_count,
                        wal_fingerprint,
                        &tracked_indexes,
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

                mark_runtime_index_bootstrap_table_complete();
            
            }
        
        }

        set_runtime_index_bootstrap_progress(|progress| {
            progress.phase = "ready".to_string();
            progress.tables_total = tables_total;
            progress.tables_completed = tables_total;
            progress.current_database_id.clear();
            progress.current_table_id.clear();
            progress.current_table_started_epoch_ms = 0;
            progress.done = true;
            progress.last_update_epoch_ms = epoch_ms!();
        });

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

                let Some(table_handle) = catalog.table_handle(&table_id) else {
                    continue;
                };

                let table_stream_id = catalog
                    .entity_wal_stream_id(&table_id)
                    .unwrap_or_else(|| table_id.clone());

                table_handle.read_table(|table| {
                    for index in table.indexes.values() {
                        if let Some(state) = self.index_for_table(&table_stream_id, &index.index_id.0) {
                            let scoped_id = scoped_index_id(&table_stream_id, &index.index_id.0);
                            scoped.indexes.insert(scoped_id, state.clone());
                        }
                    }
                });

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

        let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(&data_dir, table_stream_id);

        let latest_tx_id = wal
            .latest_transaction_id(table_stream_id)
            .map(|tx| tx.0)
            .unwrap_or(0);

        let table_scope_id = table_stream_id;
        
        for index in &tracked_indexes {
            self.register_index_for_table(table_scope_id, index);
        }

        let live_row_count = primary_key_index(table)
            .and_then(|index| self.cardinality_for_table(table_scope_id, &index.index_id.0))
            .unwrap_or_else(|| {
                tracked_indexes
                    .iter()
                    .filter_map(|index| self.cardinality_for_table(table_scope_id, &index.index_id.0))
                    .max()
                    .unwrap_or(0)
            });

        let min_interval_ms = runtime_index_incremental_persistence_min_interval_ms()
            .max(runtime_index_incremental_persistence_large_table_interval_ms(
                live_row_count,
            ));
        let now_ms = epoch_ms!();

        if min_interval_ms > 0
            && let Some(last_persist_ms) = self.incremental_persist_last_saved_ms.get(table_stream_id)
            && now_ms.saturating_sub(*last_persist_ms) < min_interval_ms
        {
            return Ok(());
        }

        let snapshot_store = runtime_index_store_for_table(self, table_stream_id, &tracked_indexes);
        let table_owned = table.clone();
        let table_stream_id_owned = table_stream_id.to_string();
        let tracked_indexes_owned = tracked_indexes.clone();

        std::thread::spawn(move || {
            
            if let Err(err) = persist_runtime_index_snapshot(
                &snapshot_store,
                &data_dir,
                &table_owned,
                &table_stream_id_owned,
                latest_tx_id,
                live_row_count,
                wal_fingerprint,
                &tracked_indexes_owned,
            ) {
                log::warn!(
                    "runtime index snapshot save skipped table={} reason={}",
                    table_owned.table_id,
                    err,
                );
            }

        });

        self.incremental_persist_last_saved_ms
            .insert(table_stream_id.to_string(), now_ms);

        Ok(())

    }

}

fn runtime_index_store_for_table(
    store: &RuntimeIndexStore,
    table_stream_id: &str,
    tracked_indexes: &[DatabaseIndex],
) -> RuntimeIndexStore {

    let mut scoped = RuntimeIndexStore {
        indexes: AHashMap::new(),
        materialize_non_primary: store.materialize_non_primary,
        non_primary_field_allowlist: store.non_primary_field_allowlist.clone(),
        non_primary_index_allowlist: store.non_primary_index_allowlist.clone(),
        incremental_persist_last_saved_ms: AHashMap::new(),
    };

    for index in tracked_indexes {
        let scoped_id = scoped_index_id(table_stream_id, &index.index_id.0);

        if let Some(state) = store.index_for_table(table_stream_id, &index.index_id.0) {
            scoped.indexes.insert(scoped_id, state.clone());
            continue;
        }

        if let Some(state) = store.index(&index.index_id.0) {
            scoped.indexes.insert(scoped_id, state.clone());
        }
    }

    scoped

}

#[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows, source, elapsed_ms)")]
fn load_bootstrap_live_rows(
    snapshot_data_dir: Option<&std::path::PathBuf>,
    wal: &ConcurrentWalManager,
    table: &DatabaseTable,
    table_stream_id: &str,
    wal_fingerprint: Option<(u64, u64)>,
    fallback_latest_tx_id: u64,
) -> (u64, Vec<(u64, HashMap<String, Vec<u8>>)>, &'static str, u128) {

    let checkpoint_started_at = Instant::now();
    let checkpoint_rows = snapshot_data_dir
        .and_then(|data_dir| {

            let live_row_checkpoint_max_rows = runtime_index_bootstrap_live_row_checkpoint_max_rows();
            if live_row_checkpoint_max_rows > 0
                && let Some((_latest_tx_id, live_row_count)) = RuntimeIndexSnapshotService::load_live_row_count_checkpoint(
                    data_dir,
                    table_stream_id,
                    &table.table_id,
                    &table.schema,
                )
                && live_row_count > live_row_checkpoint_max_rows
            {
                log::info!(
                    "runtime index bootstrap live-row checkpoint skipped table={} stream={} live_rows={} max_live_rows={} source=count_checkpoint",
                    table.table_id,
                    table_stream_id,
                    live_row_count,
                    live_row_checkpoint_max_rows,
                );

                return None;
            }

            RuntimeIndexSnapshotService::load_live_row_checkpoint(
                data_dir,
                table,
                table_stream_id,
                wal_fingerprint,
            )
        });

    let checkpoint_elapsed_ms = checkpoint_started_at.elapsed().as_millis();

    if let Some(checkpoint) = checkpoint_rows {
        return (
            checkpoint.latest_tx_id,
            checkpoint.live_rows,
            "checkpoint",
            checkpoint_elapsed_ms,
        );
    }

    let live_rows_started_at = Instant::now();
    let live_rows = load_live_rows_in_place(
        wal,
        table_stream_id,
        &table.schema,
    );
    let live_rows_elapsed_ms = live_rows_started_at.elapsed().as_millis();

    (
        fallback_latest_tx_id,
        live_rows,
        "wal",
        live_rows_elapsed_ms,
    )

}

#[expect(clippy::too_many_arguments, reason="this is a utility function for persisting live-row checkpoints")]
fn persist_live_row_checkpoint_if_from_wal(
    snapshot_data_dir: Option<&std::path::PathBuf>,
    table: &DatabaseTable,
    table_stream_id: &str,
    latest_tx_id: u64,
    wal_fingerprint: Option<(u64, u64)>,
    source: &str,
    live_rows: &[(u64, HashMap<String, Vec<u8>>)],
    table_id: &str,
) {

    if source != "wal" {
        return;
    }

    if let Some(data_dir) = snapshot_data_dir
        && let Err(err) = RuntimeIndexSnapshotService::save_live_row_checkpoint(
            data_dir,
            table,
            table_stream_id,
            latest_tx_id,
            wal_fingerprint,
            live_rows,
        )
    {
        log::warn!(
            "live-row checkpoint save skipped table={} reason={}",
            table_id,
            err,
        );
    }

}

#[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows)")]
pub fn load_live_row_checkpoint_rows(
    data_dir: &std::path::Path,
    table_stream_id: &str,
    table_id: &str,
    schema: &crate::TableSchema,
) -> Option<(u64, Vec<(u64, HashMap<String, Vec<u8>>)>)> {
    RuntimeIndexSnapshotService::load_live_row_checkpoint_rows(data_dir, table_stream_id, table_id, schema)
}

pub fn load_live_row_count_checkpoint(
    data_dir: &std::path::Path,
    table_stream_id: &str,
    table_id: &str,
    schema: &crate::TableSchema,
) -> Option<(u64, usize)> {
    RuntimeIndexSnapshotService::load_live_row_count_checkpoint(data_dir, table_stream_id, table_id, schema)
}

#[expect(clippy::too_many_arguments, reason="this is a utility function for persisting runtime index snapshots")]
fn persist_runtime_index_snapshot(
    store: &RuntimeIndexStore,
    data_dir: &std::path::Path,
    table: &DatabaseTable,
    table_stream_id: &str,
    latest_tx_id: u64,
    live_row_count: usize,
    wal_fingerprint: Option<(u64, u64)>,
    tracked_indexes: &[DatabaseIndex],
) -> Result<(), String> {

    let indexes = snapshot_indexes_for_table(store, table_stream_id, tracked_indexes)?;

    let snapshot_path = RuntimeIndexSnapshotService::runtime_index_snapshot_path(data_dir, table_stream_id);

    RuntimeIndexSnapshotService::save_runtime_index_snapshot(
        data_dir,
        table,
        table_stream_id,
        latest_tx_id,
        live_row_count,
        wal_fingerprint,
        indexes,
    )?;

    if !snapshot_path.exists() {
        return Err(format!(
            "snapshot write reported success but file missing at {}",
            snapshot_path.display()
        ));
    }

    log::info!(
        "runtime index snapshot persisted table={} path={}",
        table.table_id,
        snapshot_path.display(),
    );

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        data_dir,
        table,
        table_stream_id,
        latest_tx_id,
        wal_fingerprint,
        live_row_count,
    )
    
}

fn snapshot_indexes_for_table(
    store: &RuntimeIndexStore,
    table_scope_id: &str,
    tracked_indexes: &[DatabaseIndex],
) -> Result<Vec<RuntimeIndexSnapshotIndex>, String> {

    let mut indexes = Vec::with_capacity(tracked_indexes.len());

    for index in tracked_indexes {
        let state = store
            .index_for_table(table_scope_id, &index.index_id.0)
            .or_else(|| store.index(&index.index_id.0))
            .ok_or_else(|| {
                format!(
                    "missing runtime index state '{}' (scope '{}')",
                    index.index_id.0,
                    table_scope_id,
                )
            })?;

        indexes.push(RuntimeIndexSnapshotIndex {
            index_id: index.index_id.0.clone(),
            entries: state.entries.keys().cloned().collect::<Vec<_>>(),
            row_refs: state
                .entries
                .iter()
                .filter_map(|(key, row_ref)| row_ref.map(|row_ref| (key.clone(), row_ref)))
                .collect(),
        });
    }

    Ok(indexes)

}

fn rebuild_bootstrap_indexes_from_live_rows(
    store: &mut RuntimeIndexStore,
    table_stream_id: &str,
    tracked_indexes: &[DatabaseIndex],
    live_rows: &[(u64, HashMap<String, Vec<u8>>)],
) {

    let chunk_rows = runtime_index_bootstrap_index_build_chunk_rows();

    for index in tracked_indexes {

        let mut entries = AHashSet::with_capacity(live_rows.len());
        let mut row_refs = if index.is_unique_key() {
            Some(AHashMap::with_capacity(live_rows.len()))
        } else {
            None
        };

        for live_rows_chunk in live_rows.chunks(chunk_rows) {
            for (row_id, row_map) in live_rows_chunk {
                let key = index_value_tuple(index, row_map);
                if let Some(row_refs) = row_refs.as_mut() {
                    row_refs.insert(key.clone(), *row_id);
                }
                entries.insert(key);
            }
        }

        let state = store.index_mut_for_table(table_stream_id, &index.index_id.0);
        state.index = Some(index.clone());
        state.rebuild_with_row_refs(entries, row_refs.unwrap_or_default());

    }

}

#[expect(clippy::type_complexity, reason="returning per-index bootstrap state for rebuild")]
fn build_snapshot_index_entries(
    tracked_indexes: &[DatabaseIndex],
    snapshot: &RuntimeIndexTableSnapshot,
) -> Vec<RuntimeIndexRebuildItem> {

    let available = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);

    let should_parallel = available > 1
        && tracked_indexes.len() > 1
        && snapshot.live_row_count >= runtime_index_parallel_build_min_rows();

    if !should_parallel {
        return tracked_indexes
            .iter()
            .filter_map(|index| {

                snapshot
                    .indexes
                    .iter()
                    .find(|item| item.index_id == index.index_id.0)
                    .map(|item| RuntimeIndexRebuildItem {
                        index_id: index.index_id.0.clone(),
                        entries: item.entries.iter().cloned().collect::<AHashSet<_>>(),
                        row_refs: item.row_refs.iter().cloned().collect::<AHashMap<_, _>>(),
                    })

            })
            .collect();
    }

    let workers = std::cmp::min(
        std::cmp::min(available, runtime_index_parallel_build_max_workers()),
        tracked_indexes.len(),
    );

    let chunk_size = tracked_indexes.len().div_ceil(workers);

    let rebuilt = std::thread::scope(|scope| {
        
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

                    chunk.push(RuntimeIndexRebuildItem {
                        index_id: index.index_id.0.clone(),
                        entries: item.entries.iter().cloned().collect::<AHashSet<_>>(),
                        row_refs: item.row_refs.iter().cloned().collect::<AHashMap<_, _>>(),
                    });
                }

                (start, chunk)
                
            }));

        }

        let mut rebuilt = Vec::with_capacity(tracked_indexes.len());

        for handle in handles {
            if let Ok(chunk) = handle.join() {
                let (_, mut items) = chunk;
                rebuilt.append(&mut items);
            }
        }

        rebuilt
        
    });

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
        .unwrap_or(false)

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

    let mut values = Vec::with_capacity(if index.field_names.is_empty() {
        1
    } else {
        index.field_names.len()
    });

    write_index_value_tuple(index, row_map, &mut values);

    values

}

fn write_index_value_tuple(
    index: &DatabaseIndex,
    row_map: &HashMap<String, Vec<u8>>,
    out: &mut Vec<Vec<u8>>,
) {

    out.clear();

    if index.field_names.is_empty() && !index.field_name.is_empty() {
        out.push(row_map.get(&index.field_name).cloned().unwrap_or_default());
        return;
    }

    for field_name in &index.field_names {
        out.push(row_map.get(field_name).cloned().unwrap_or_default());
    }

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
