use ahash::{AHashMap, AHashSet};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::time::Instant;

use super::table::DatabaseTable;
use crate::engine::execution::access::load_live_rows_in_place;
use crate::{
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
}

impl RuntimeIndexStore {

    pub fn new() -> Self {
        Self {
            indexes: AHashMap::new(),
            materialize_non_primary: runtime_index_materialize_non_primary(),
            non_primary_field_allowlist: runtime_index_non_primary_field_allowlist(),
            non_primary_index_allowlist: runtime_index_non_primary_index_allowlist(),
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

    #[expect(clippy::should_implement_trait, reason="Index access by string ID, not by reference")]
    pub fn index_mut(&mut self, index_id: &str) -> &mut RuntimeIndexState {
        match self.indexes.entry(index_id.to_string()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(RuntimeIndexState::default()),
        }
    }

    pub fn cardinality(&self, index_id: &str) -> Option<usize> {
        self.index(index_id).map(|state| state.cardinality())
    }

    pub fn stats(&self, index_id: &str) -> Option<(usize, usize)> {
        self.index(index_id)
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

    pub fn record_row(&mut self, index: &DatabaseIndex, row_map: &HashMap<String, Vec<u8>>) {
        if !self.should_track_index(index) {
            return;
        }

        let key = index_value_tuple(index, row_map);
        self.index_mut(&index.index_id.0).insert(key);
    }

    pub fn record_table_row<'a, I>(&mut self, indexes: I, row_map: &HashMap<String, Vec<u8>>)
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        for index in indexes {
            self.record_row(index, row_map);
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

    pub fn record_table_rows_batch(
        &mut self,
        indexes: &[&DatabaseIndex],
        row_maps: &[HashMap<String, Vec<u8>>],
    ) {
        if row_maps.is_empty() {
            return;
        }

        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            let state = self.index_mut(&index.index_id.0);
            state.reserve_entries(row_maps.len());

            for row_map in row_maps {
                let key = index_value_tuple(index, row_map);
                state.insert(key);
            }
        }
    }

    pub fn remove_table_rows_batch(
        &mut self,
        indexes: &[&DatabaseIndex],
        row_maps: &[HashMap<String, Vec<u8>>],
    ) {
        if row_maps.is_empty() {
            return;
        }

        for index in indexes {

            if !self.should_track_index(index) {
                continue;
            }

            let state = self.index_mut(&index.index_id.0);
            for row_map in row_maps {
                let key = index_value_tuple(index, row_map);
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
        indexes: I,
        kind: TransactionKind,
        row_map: &HashMap<String, Vec<u8>>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {
        match kind {
            TransactionKind::Ignore => {}
            TransactionKind::Delete => self.remove_table_row(indexes, row_map),
            TransactionKind::Insert | TransactionKind::Update => {
                self.record_table_row(indexes, row_map)
            }
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
            "runtime index bootstrap mode materialize_non_primary={} warm_equality_cache_on_bootstrap=true non_primary_field_allowlist={} non_primary_index_allowlist={}",
            self.materialize_non_primary,
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

        for (database_id, catalog) in catalogs {
            
            for table_id in catalog.table_ids() {

                let table_started_at = Instant::now();

                let Some(table) = catalog.table(&table_id) else {
                    continue;
                };

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
                    self.register_index(index.clone());
                }

                let live_rows_started_at = Instant::now();
                let live_rows = load_live_rows_in_place(wal, &table_id, &table.schema);
                let live_rows_elapsed_ms = live_rows_started_at.elapsed().as_millis();
                let live_row_count = live_rows.len();

                let latest_tx_id = wal
                    .latest_transaction_id(&table_id)
                    .map(|tx| tx.0)
                    .unwrap_or(0);

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

                if live_rows_elapsed_ms >= 1_000 {
                    log::info!(
                        "runtime index bootstrap live-row materialization database={} table={} live_rows={} elapsed_ms={}",
                        database_id,
                        table_id,
                        live_row_count,
                        live_rows_elapsed_ms,
                    );
                }

                let rebuild_started_at = Instant::now();
                let rebuilt = build_bootstrap_index_entries(&tracked_indexes, &live_rows);
                let rebuild_elapsed_ms = rebuild_started_at.elapsed().as_millis();

                for (index_id, index, entries) in rebuilt {
                    let state = self.index_mut(&index_id);
                    state.rebuild(entries);
                    state.index = Some(index);
                }

                let warm_started_at = Instant::now();
                warm_equality_cache_from_live_rows(
                    wal.cache_scope_id(),
                    &table_id,
                    latest_tx_id,
                    live_rows,
                    &warm_fields,
                );
                let warm_elapsed_ms = warm_started_at.elapsed().as_millis();

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

                for index in table.indexes.values() {
                    if let Some(state) = self.index(&index.index_id.0) {
                        scoped.indexes.insert(index.index_id.0.clone(), state.clone());
                    }
                }

            }

        }

        scoped
        
    }

}

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

fn parse_runtime_index_allowlist_env(var_name: &str) -> AHashSet<String> {

    let Some(value) = std::env::var(var_name).ok() else {
        return AHashSet::new();
    };

    value
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| common::normalize_identifier!(entry))
        .collect()

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
