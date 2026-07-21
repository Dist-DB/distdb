use ahash::{AHashMap, AHashSet};
use common::epoch_ms;
use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
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

const RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS: usize = 250_000;
const RUNTIME_INDEX_PARALLEL_BUILD_MAX_WORKERS: usize = 32;

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
            entries: AHashSet::new(),
        });

    }

    pub fn register_index_for_table(&mut self, table_scope_id: &str, index: &DatabaseIndex) {

        if !self.should_track_index(&index) {
            return;
        }

        let index_id = scoped_index_id(table_scope_id, &index.index_id.0);
        self.indexes.entry(index_id).or_insert_with(|| RuntimeIndexState {
            index: Some(index.clone()),
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

            if !self.should_track_index(index) {
                continue;
            }

            let key = index_value_tuple(index, row_map);
            self.index_mut_for_table(table_scope_id, &index.index_id.0)
                .insert(key);

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

                let Some(table) = catalog
                    .table_handle(&table_id)
                    .and_then(|handle| handle.table_snapshot()) else {
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
                    self.register_index_for_table(&table_stream_id, index);
                }

                let wal_fingerprint = snapshot_data_dir
                    .as_ref()
                    .and_then(|data_dir| RuntimeIndexSnapshotService::wal_stream_fingerprint(data_dir, &table_stream_id));

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
                        .map(|(_, entries)| entries.len())
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

                    for (index_id, entries) in restored {
                        let state = self.index_mut_for_table(&table_stream_id, &index_id);
                        state.rebuild(entries);
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

                    if runtime_index_preload_accessors_on_bootstrap() && !warm_fields.is_empty() {
                        
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

                            continue;

                        }

                        let checkpoint_started_at = Instant::now();
                        let checkpoint_rows = snapshot_data_dir
                            .as_ref()
                            .and_then(|data_dir| {
                                RuntimeIndexSnapshotService::load_live_row_checkpoint(
                                    data_dir,
                                    &table,
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
                            && let Err(err) = RuntimeIndexSnapshotService::save_live_row_checkpoint(
                                data_dir,
                                &table,
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
                        RuntimeIndexSnapshotService::load_live_row_checkpoint(
                            data_dir,
                            &table,
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

                for (index_id, entries) in rebuilt {
                    let state = self.index_mut_for_table(&table_stream_id, &index_id);
                    state.rebuild(entries);
                }

                if let Some(data_dir) = snapshot_data_dir.as_ref()
                    && live_rows_mode == "wal"
                    && let Err(err) = RuntimeIndexSnapshotService::save_live_row_checkpoint(
                        data_dir,
                        &table,
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

        let min_interval_ms = runtime_index_incremental_persistence_min_interval_ms();
        let now_ms = epoch_ms!();

        if min_interval_ms > 0
            && let Some(last_persist_ms) = self.incremental_persist_last_saved_ms.get(table_stream_id)
            && now_ms.saturating_sub(*last_persist_ms) < min_interval_ms
        {
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

        persist_runtime_index_snapshot(
            self,
            &data_dir,
            table,
            table_stream_id,
            latest_tx_id,
            live_row_count,
            wal_fingerprint,
            &tracked_indexes,
        )?;

        self.incremental_persist_last_saved_ms
            .insert(table_stream_id.to_string(), now_ms);

        Ok(())

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

    RuntimeIndexSnapshotService::save_runtime_index_snapshot(
        data_dir,
        table,
        table_stream_id,
        latest_tx_id,
        live_row_count,
        wal_fingerprint,
        indexes,
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
            entries: state.entries.iter().cloned().collect(),
        });
    }

    Ok(indexes)
}

#[expect(clippy::type_complexity, reason="returning a tuple of (latest_tx_id, live_rows)")]
fn build_bootstrap_index_entries(
    tracked_indexes: &[DatabaseIndex],
    live_rows: &[(u64, HashMap<String, Vec<u8>>)],
) -> Vec<(String, AHashSet<Vec<Vec<u8>>>)> {

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
                (index.index_id.0.clone(), entries)
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
                    chunk.push((index.index_id.0.clone(), entries));
                
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

#[expect(clippy::type_complexity, reason="returning a tuple of (index_id, entries)")]
fn build_snapshot_index_entries(
    tracked_indexes: &[DatabaseIndex],
    snapshot: &RuntimeIndexTableSnapshot,
) -> Vec<(String, AHashSet<Vec<Vec<u8>>>)> {

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
