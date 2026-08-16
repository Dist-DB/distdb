use ahash::{AHashMap, AHashSet};
use common::epoch_ms;
use std::borrow::Borrow;
use std::collections::BTreeSet;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::ops::Bound;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use super::runtime_index_key_codec::{
    decode_runtime_index_entry_key,
    encode_runtime_index_entry_key,
    encode_sortable_numeric,
    RuntimeIndexNumericKind,
    normalize_runtime_index_string_key,
};
use super::runtime_index_snapshot::{
    RuntimeIndexSnapshotIndex,
    RuntimeIndexSnapshotService,
    RuntimeIndexTableSnapshot,
};
use super::runtime_indexors::DatatypeIndexor;
use super::super::table::DatabaseTable;
use crate::engine::execution::access::{
    load_live_rows_in_place,
    warm_string_like_cache_for_fields,
};
use crate::{
    restore_equality_cache_from_snapshot,
    render_stored_field_value,
    warm_equality_cache_from_live_rows, ConcurrentWalManager, DatabaseCatalog, DatabaseIndex,
    DatabaseIndexOrigin, FieldKind, FieldType, TableSchema, TransactionKind,
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
    common::settings::positive_usize(
        common::settings::RUNTIME_INDEX_BUILD_WORKERS,
        RUNTIME_INDEX_PARALLEL_BUILD_MAX_WORKERS,
    )
}

fn runtime_index_parallel_build_min_rows() -> usize {
    common::settings::positive_usize(
        common::settings::RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS,
        RUNTIME_INDEX_PARALLEL_BUILD_MIN_ROWS,
    )
}

fn runtime_index_migrate_legacy_snapshot_on_bootstrap() -> bool {
    common::settings::flag(
        common::settings::RUNTIME_INDEX_MIGRATE_LEGACY_ON_BOOTSTRAP,
        false,
    )
}

fn runtime_index_incremental_persistence_on_commit() -> bool {
    common::settings::flag(
        common::settings::RUNTIME_INDEX_INCREMENTAL_PERSIST_ON_COMMIT,
        true,
    )
}

fn runtime_index_incremental_persistence_min_interval_ms() -> u64 {
    common::settings::u64_allowing_zero(
        common::settings::RUNTIME_INDEX_INCREMENTAL_PERSIST_MIN_INTERVAL_MS,
        1_000,
    )
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
    common::settings::flag(
        common::settings::RUNTIME_INDEX_PRELOAD_ACCESSORS_ON_BOOTSTRAP,
        false,
    )
}

fn runtime_index_realign_wal_records_on_bootstrap() -> bool {
    common::settings::flag(common::settings::REALIGN_WAL_RECORDS, false)
}

fn runtime_index_background_prewarm_skipped_accessors() -> bool {
    common::settings::flag(
        common::settings::RUNTIME_INDEX_BACKGROUND_PREWARM_SKIPPED_ACCESSORS,
        false,
    )
}

fn numeric_kind_for_index(index: &DatabaseIndex, schema: &TableSchema) -> Option<RuntimeIndexNumericKind> {
    let field_name = if index.field_names.len() == 1 {
        index.field_names.first()?
    } else if index.field_names.is_empty() && !index.field_name.is_empty() {
        &index.field_name
    } else {
        return None;
    };

    match schema.field(field_name)?.field_type {
        FieldType::Int(_) => Some(RuntimeIndexNumericKind::Signed),
        FieldType::UInt(_) => Some(RuntimeIndexNumericKind::Unsigned),
        _ => None,
    }
}

fn runtime_index_bootstrap_live_row_checkpoint_max_rows() -> usize {
    common::settings::usize_allowing_zero(
        common::settings::RUNTIME_INDEX_BOOTSTRAP_LIVE_ROW_CHECKPOINT_MAX_ROWS,
        RUNTIME_INDEX_BOOTSTRAP_LIVE_ROW_CHECKPOINT_MAX_ROWS_DEFAULT,
    )
}

fn runtime_index_bootstrap_index_build_chunk_rows() -> usize {
    common::settings::positive_usize(
        common::settings::RUNTIME_INDEX_BOOTSTRAP_INDEX_BUILD_CHUNK_ROWS,
        RUNTIME_INDEX_BOOTSTRAP_INDEX_BUILD_CHUNK_ROWS_DEFAULT,
    )
}

fn runtime_index_probe_paging_debug_enabled() -> bool {
    common::settings::flag(common::settings::RUNTIME_INDEX_PAGING_DEBUG, false)
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
    numeric_kind: Option<RuntimeIndexNumericKind>,
    string_case_insensitive: bool,
    entries: AHashMap<IndexKey, Option<NonZeroU64>>,
    non_unique_row_refs: AHashMap<IndexKey, PostingPages>,
    ordered_entry_keys: BTreeSet<IndexKey>,
}

const RUNTIME_INDEX_POSTING_PAGE_SIZE: usize = 1_024;

/// Index keys are shared between the entry map, the postings map and the ordered
/// key set, so the bytes are allocated once per distinct key rather than three times.
type IndexKey = Arc<[u8]>;

#[derive(Debug, Clone, Default)]
struct PostingPages {
    inline_row_ref: Option<NonZeroU64>,
    pages: Vec<Vec<NonZeroU64>>,
    len: usize,
}

impl PostingPages {
    fn insert_unique_sorted(&mut self, row_ref: NonZeroU64) {
        if self.len == 0 {
            self.inline_row_ref = Some(row_ref);
            self.len = 1;
            return;
        }

        if self.len == 1 {
            let existing = self.inline_row_ref.expect("single posting should be inline");
            if existing == row_ref {
                return;
            }

            let mut page = Vec::with_capacity(2);
            if existing < row_ref {
                page.push(existing);
                page.push(row_ref);
            } else {
                page.push(row_ref);
                page.push(existing);
            }
            self.inline_row_ref = None;
            self.pages.push(page);
            self.len = 2;
            return;
        }

        if let Some(last_page) = self.pages.last_mut()
            && last_page.last().is_some_and(|last| *last < row_ref)
        {
            if last_page.len() == RUNTIME_INDEX_POSTING_PAGE_SIZE {
                self.pages.push(Vec::new());
                self.pages.last_mut().expect("posting page should exist").push(row_ref);
            } else {
                last_page.push(row_ref);
            }
            self.len = self.len.saturating_add(1);
            return;
        }

        let page_index = self.pages.partition_point(|page| {
            page.last().is_some_and(|last| *last < row_ref)
        });

        // Pages grow on demand: most non-unique keys hold a handful of row refs,
        // so reserving a full page per key costs orders of magnitude more than it saves.
        if page_index == self.pages.len() {
            self.pages.push(Vec::new());
        }

        let search_result = self.pages[page_index].binary_search(&row_ref);
        match search_result {
            Ok(_) => {}
            Err(insert_at) => {
                let split_page = {
                    let page = &mut self.pages[page_index];
                    page.insert(insert_at, row_ref);
                    (page.len() > RUNTIME_INDEX_POSTING_PAGE_SIZE)
                        .then(|| page.split_off(page.len() / 2))
                };
                self.len = self.len.saturating_add(1);
                if let Some(split_page) = split_page {
                    self.pages.insert(page_index + 1, split_page);
                }
            }
        }
    }

    fn remove(&mut self, row_ref: NonZeroU64) -> bool {
        if self.len == 1 {
            if self.inline_row_ref == Some(row_ref) {
                self.inline_row_ref = None;
                self.len = 0;
                return true;
            }
            return false;
        }

        for page_index in 0..self.pages.len() {
            let page = &mut self.pages[page_index];
            if let Ok(remove_at) = page.binary_search(&row_ref) {
                page.remove(remove_at);
                self.len = self.len.saturating_sub(1);
                if page.is_empty() {
                    self.pages.remove(page_index);
                }

                if self.len == 1 {
                    self.inline_row_ref = self
                        .pages
                        .iter()
                        .find_map(|page| page.first().copied());
                    self.pages.clear();
                }
                return true;
            }
        }

        false
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn len(&self) -> usize {
        self.len
    }

    fn append_row_refs(&self, row_refs: &mut Vec<u64>, limit: Option<usize>) {
        if let Some(row_ref) = self.inline_row_ref {
            if let Some(row_ref) = unpack_row_ref(Some(row_ref)) {
                row_refs.push(row_ref);
            }
            return;
        }

        for page in &self.pages {
            for row_ref in page {
                if limit.is_some_and(|limit| row_refs.len() >= limit) {
                    return;
                }
                if let Some(row_ref) = unpack_row_ref(Some(*row_ref)) {
                    row_refs.push(row_ref);
                }
            }
        }
    }

    fn iter(&self) -> impl Iterator<Item = NonZeroU64> + '_ {
        self.inline_row_ref
            .into_iter()
            .chain(self.pages.iter().flat_map(|page| page.iter().copied()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeIndexRangeBound {
    pub key: Vec<Vec<u8>>,
    pub inclusive: bool,
}

fn pack_row_ref(row_ref: u64) -> Option<NonZeroU64> {
    row_ref
        .checked_add(1)
        .and_then(NonZeroU64::new)
}

fn unpack_row_ref(row_ref: Option<NonZeroU64>) -> Option<u64> {
    row_ref.map(|row_ref| row_ref.get().saturating_sub(1))
}

fn collect_row_refs_for_encoded_key(
    state: &RuntimeIndexState,
    encoded_key: &[u8],
    row_refs: &mut Vec<u64>,
) {
    if let Some(row_ref) = state
        .entries
        .get(encoded_key)
        .copied()
        .flatten()
        .and_then(|row_ref| unpack_row_ref(Some(row_ref)))
    {
        row_refs.push(row_ref);
    }

    if let Some(non_unique_row_refs) = state.non_unique_row_refs.get(encoded_key) {
        non_unique_row_refs.append_row_refs(row_refs, None);
    }
}

fn log_runtime_index_bootstrap_table_memory_profile(
    store: &RuntimeIndexStore,
    table_scope_id: &str,
    database_id: &str,
    table_id: &str,
    tracked_indexes: &[DatabaseIndex],
) {
    let mut index_profiles = Vec::with_capacity(tracked_indexes.len());

    for index in tracked_indexes {
        let Some(state) = store.index_for_table(table_scope_id, &index.index_id.0) else {
            continue;
        };

        let entry_count = state.entries.len();
        let row_ref_count = state
            .entries
            .values()
            .filter(|row_ref| row_ref.is_some())
            .count();
        let key_bytes = state
            .entries
            .keys()
            .map(|key| key.len())
            .sum::<usize>();

        index_profiles.push((
            index.index_id.0.clone(),
            entry_count,
            row_ref_count,
            key_bytes,
        ));
    }

    if index_profiles.is_empty() {
        return;
    }

    let total_entries = index_profiles
        .iter()
        .map(|(_, entry_count, _, _)| *entry_count)
        .sum::<usize>();
    
    let total_row_refs = index_profiles
        .iter()
        .map(|(_, _, row_ref_count, _)| *row_ref_count)
        .sum::<usize>();

    let total_key_bytes = index_profiles
        .iter()
        .map(|(_, _, _, key_bytes)| *key_bytes)
        .sum::<usize>();

    #[expect(clippy::unnecessary_sort_by, reason="Sorting by key_bytes descending for logging purposes")]
    index_profiles.sort_by(|left, right| right.3.cmp(&left.3));

    let top_indexes = index_profiles
        .iter()
        .take(5)
        .map(|(index_id, entry_count, row_ref_count, key_bytes)| {
            format!(
                "{}:entries={} row_refs={} key_bytes={}",
                index_id,
                entry_count,
                row_ref_count,
                key_bytes,
            )
        })
        .collect::<Vec<_>>()
        .join(" | ");

    log::info!(
        "runtime index bootstrap memory profile database={} table={} indexes={} entries={} row_refs={} key_bytes={} top_indexes={}",
        database_id,
        table_id,
        index_profiles.len(),
        total_entries,
        total_row_refs,
        total_key_bytes,
        top_indexes,
    );
}

impl RuntimeIndexState {

    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_numeric_kind(&mut self, numeric_kind: Option<RuntimeIndexNumericKind>) {
        self.numeric_kind = numeric_kind;
    }

    pub fn set_string_case_insensitive(&mut self, enabled: bool) {
        self.string_case_insensitive = enabled;
    }

    fn encode_key(&self, key: &[Vec<u8>]) -> Option<Vec<u8>> {
        let key = if self.string_case_insensitive {
            key.iter()
                .map(|value| normalize_runtime_index_string_key(value, true))
                .collect::<Vec<_>>()
        } else {
            key.to_vec()
        };

        if key.len() == 1
            && let Some(numeric_kind) = self.numeric_kind
            && let Some(value) = encode_sortable_numeric(
                &render_stored_field_value(&key[0]),
                numeric_kind,
            )
        {
            return encode_runtime_index_entry_key(&[value]);
        }

        encode_runtime_index_entry_key(&key)
    }

    pub fn contains(&self, pk_val: &[Vec<u8>]) -> bool {
        self.encode_key(pk_val)
            .as_deref()
            .is_some_and(|encoded| self.entries.contains_key(encoded))
    }

    pub fn insert(&mut self, pk_val: Vec<Vec<u8>>) {
        self.insert_with_row_ref(pk_val, None);
    }

    pub fn insert_with_row_ref(&mut self, pk_val: Vec<Vec<u8>>, row_ref: Option<u64>) {
        let Some(encoded_key) = self.encode_key(&pk_val) else {
            return;
        };

        let is_unique_key = self
            .index
            .as_ref()
            .map(|index| index.is_unique_key())
            .unwrap_or(true);

        let stored_row_ref = if is_unique_key {
            row_ref.and_then(pack_row_ref)
        } else {
            None
        };

        let shared_key = self.intern_key(encoded_key);

        self.entries.insert(Arc::clone(&shared_key), stored_row_ref);
        self.ordered_entry_keys.insert(Arc::clone(&shared_key));

        if !is_unique_key
            && let Some(row_ref) = row_ref.and_then(pack_row_ref)
        {
            let postings = self.non_unique_row_refs.entry(shared_key).or_default();
            postings.insert_unique_sorted(row_ref);
        }
    }

    fn intern_key(&self, encoded_key: Vec<u8>) -> IndexKey {
        match self.entries.get_key_value(encoded_key.as_slice()) {
            Some((existing, _)) => Arc::clone(existing),
            None => Arc::from(encoded_key.into_boxed_slice()),
        }
    }

    pub fn remove(&mut self, pk_val: &[Vec<u8>]) {
        self.remove_with_row_ref(pk_val, None);
    }

    pub fn remove_with_row_ref(&mut self, pk_val: &[Vec<u8>], row_ref: Option<u64>) {
        if let Some(encoded_key) = self.encode_key(pk_val) {
            let encoded_key = encoded_key.as_slice();
            let is_unique_key = self
                .index
                .as_ref()
                .map(|index| index.is_unique_key())
                .unwrap_or(true);

            if is_unique_key {
                self.entries.remove(encoded_key);
                self.ordered_entry_keys.remove(encoded_key);
                return;
            }

            let should_remove_key = if let Some(non_unique_row_refs) = self.non_unique_row_refs.get_mut(encoded_key) {
                if let Some(row_ref) = row_ref.and_then(pack_row_ref) {
                    non_unique_row_refs.remove(row_ref);
                    non_unique_row_refs.is_empty()
                } else {
                    true
                }
            } else {
                true
            };

            if should_remove_key {
                self.non_unique_row_refs.remove(encoded_key);
                self.entries.remove(encoded_key);
                self.ordered_entry_keys.remove(encoded_key);
            }
        }
    }

    pub fn cardinality(&self) -> usize {
        self.entries.len()
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn rebuild(&mut self, entries: AHashSet<Vec<Vec<u8>>>) {
        self.non_unique_row_refs.clear();
        self.ordered_entry_keys.clear();
        self.entries = entries
            .into_iter()
            .filter_map(|key| {
                let encoded: IndexKey = Arc::from(self.encode_key(&key)?.into_boxed_slice());
                self.ordered_entry_keys.insert(Arc::clone(&encoded));
                Some((encoded, None))
            })
            .collect();
    }

    pub fn rebuild_with_row_refs(
        &mut self,
        entries: AHashSet<Vec<Vec<u8>>>,
        mut row_refs: AHashMap<Vec<Vec<u8>>, Vec<u64>>,
    ) {
        self.non_unique_row_refs.clear();
        self.ordered_entry_keys.clear();
        row_refs.retain(|key, _| entries.contains(key));

        let is_unique_key = self
            .index
            .as_ref()
            .is_some_and(|index| index.is_unique_key());

        self.entries = entries
            .into_iter()
            .filter_map(|key| {
                let encoded: IndexKey = Arc::from(self.encode_key(&key)?.into_boxed_slice());
                self.ordered_entry_keys.insert(Arc::clone(&encoded));
                let stored_row_ref = if is_unique_key {
                    row_refs
                        .get(&key)
                        .and_then(|refs| refs.first())
                        .copied()
                        .and_then(pack_row_ref)
                } else {
                    None
                };
                Some((encoded, stored_row_ref))
            })
            .collect();

        if !is_unique_key {
            // Multiple rows can legitimately share the same non-unique key; keep
            // every row ref instead of collapsing to a single entry per key.
            for (key, refs) in row_refs {
                let Some(encoded) = self.encode_key(&key) else {
                    continue;
                };
                let Some(shared_key) = self
                    .entries
                    .get_key_value(encoded.as_slice())
                    .map(|(existing, _)| Arc::clone(existing))
                else {
                    continue;
                };
                for row_ref in refs {
                    let Some(packed) = pack_row_ref(row_ref) else {
                        continue;
                    };
                    let postings = self
                        .non_unique_row_refs
                        .entry(Arc::clone(&shared_key))
                        .or_default();
                    postings.insert_unique_sorted(packed);
                }
            }
        }
    }

    pub fn row_ref(&self, pk_val: &[Vec<u8>]) -> Option<u64> {
        let encoded_key = self.encode_key(pk_val)?;
        unpack_row_ref(self.entries.get(encoded_key.as_slice()).copied().flatten())
    }

    pub fn row_refs_for_key(&self, pk_val: &[Vec<u8>], limit: Option<usize>) -> Vec<u64> {

        let Some(encoded_key) = self.encode_key(pk_val) else {
            return Vec::new();
        };

        if let Some(row_ref) = unpack_row_ref(self.entries.get(encoded_key.as_slice()).copied().flatten()) {
            return vec![row_ref];
        }

        let Some(non_unique_row_refs) = self.non_unique_row_refs.get(encoded_key.as_slice()) else {
            return Vec::new();
        };

        let posting_count = non_unique_row_refs.len();
        let mut row_refs = Vec::with_capacity(limit.unwrap_or(posting_count).min(posting_count));
        non_unique_row_refs.append_row_refs(&mut row_refs, limit);

        row_refs

    }

    pub fn row_ref_count_for_key(&self, pk_val: &[Vec<u8>]) -> Option<usize> {
        let encoded_key = self.encode_key(pk_val)?;

        if self.entries.get(encoded_key.as_slice()).copied().flatten().is_some() {
            return Some(1);
        }

        self.non_unique_row_refs
            .get(encoded_key.as_slice())
            .filter(|postings| !postings.is_empty())
            .map(PostingPages::len)
    }

    pub fn row_refs_for_key_range(
        &self,
        lower: Option<&RuntimeIndexRangeBound>,
        upper: Option<&RuntimeIndexRangeBound>,
        limit: Option<usize>,
    ) -> Vec<u64> {

        let lower = match lower {
            Some(bound) => {
                let Some(encoded) = self.encode_key(&bound.key) else {
                    return Vec::new();
                };

                if bound.inclusive {
                    Bound::Included(encoded)
                } else {
                    Bound::Excluded(encoded)
                }
            }
            None => Bound::Unbounded,
        };

        let upper = match upper {

            Some(bound) => {
                let Some(encoded) = self.encode_key(&bound.key) else {
                    return Vec::new();
                };

                if bound.inclusive {
                    Bound::Included(encoded)
                } else {
                    Bound::Excluded(encoded)
                }
            },

            None => Bound::Unbounded,

        };

        if let (Bound::Included(lower_key) | 
            Bound::Excluded(lower_key), Bound::Included(upper_key) | 
            Bound::Excluded(upper_key)) = (&lower, &upper) {
            
            if lower_key > upper_key {
                return Vec::new();
            }

            if lower_key == upper_key
                && (!matches!(lower, Bound::Included(_)) || !matches!(upper, Bound::Included(_)))
            {
                return Vec::new();
            }

        }

        let mut row_refs = Vec::new();

        let lower_bound = match &lower {
            Bound::Included(key) => Bound::Included(key.as_slice()),
            Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let upper_bound = match &upper {
            Bound::Included(key) => Bound::Included(key.as_slice()),
            Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
            Bound::Unbounded => Bound::Unbounded,
        };

        for encoded_key in self.ordered_entry_keys.range::<[u8], _>((lower_bound, upper_bound)) {
            collect_row_refs_for_encoded_key(self, encoded_key, &mut row_refs);
        }

        row_refs.sort_unstable();
        row_refs.dedup();

        if let Some(limit) = limit {
            row_refs.truncate(limit);
        }

        row_refs

    }

    pub fn row_refs_for_probe_keys_paged(
        &self,
        probe_keys: &[Vec<Vec<u8>>],
        key_page_size: usize,
        max_pages_per_probe: usize,
        limit: Option<usize>,
    ) -> Vec<u64> {

        if probe_keys.is_empty() || key_page_size == 0 || max_pages_per_probe == 0 {
            return Vec::new();
        }

        let paging_debug_enabled = runtime_index_probe_paging_debug_enabled();
        let index_id = self
            .index
            .as_ref()
            .map(|index| index.index_id.0.as_str())
            .unwrap_or("unknown");

        if paging_debug_enabled {
            log::debug!(
                "runtime indexor paging begin index_id={} probe_keys={} page_size={} max_pages_per_probe={} row_limit={:?} ordered_keys={} entries={} non_unique_postings={}",
                index_id,
                probe_keys.len(),
                key_page_size,
                max_pages_per_probe,
                limit,
                self.ordered_entry_keys.len(),
                self.entries.len(),
                self.non_unique_row_refs.len(),
            );
        }

        let mut row_refs = Vec::new();
        let mut seen_keys = AHashSet::<IndexKey>::new();

        for (probe_idx, probe_key) in probe_keys.iter().enumerate() {
            let Some(encoded_probe_key) = self.encode_key(probe_key) else {
                continue;
            };

            let mut next_lower_bound = Bound::Included(encoded_probe_key);
            let mut pages_visited = 0usize;

            while pages_visited < max_pages_per_probe {
                let row_refs_before_page = row_refs.len();
                let seen_keys_before_page = seen_keys.len();
                let lower_bound = match &next_lower_bound {
                    Bound::Included(key) => Bound::Included(key.as_slice()),
                    Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let page_keys = self
                    .ordered_entry_keys
                    .range::<[u8], _>((lower_bound, Bound::Unbounded))
                    .take(key_page_size)
                    .cloned()
                    .collect::<Vec<_>>();

                if page_keys.is_empty() {
                    if paging_debug_enabled {
                        log::debug!(
                            "runtime indexor paging page_empty index_id={} probe_idx={} page={} next_bound=unbounded_or_exhausted",
                            index_id,
                            probe_idx,
                            pages_visited.saturating_add(1),
                        );
                    }
                    break;
                }

                for encoded_key in &page_keys {
                    if !seen_keys.insert(Arc::clone(encoded_key)) {
                        continue;
                    }

                    collect_row_refs_for_encoded_key(self, encoded_key, &mut row_refs);
                }

                pages_visited = pages_visited.saturating_add(1);

                if paging_debug_enabled {
                    log::debug!(
                        "runtime indexor paging page_result index_id={} probe_idx={} page={} page_keys={} new_keys={} row_refs_added={} cumulative_row_refs={}",
                        index_id,
                        probe_idx,
                        pages_visited,
                        page_keys.len(),
                        seen_keys.len().saturating_sub(seen_keys_before_page),
                        row_refs.len().saturating_sub(row_refs_before_page),
                        row_refs.len(),
                    );
                }

                if page_keys.len() < key_page_size {
                    if paging_debug_enabled {
                        log::debug!(
                            "runtime indexor paging end_probe index_id={} probe_idx={} reason=partial_page pages_visited={} total_row_refs={}",
                            index_id,
                            probe_idx,
                            pages_visited,
                            row_refs.len(),
                        );
                    }
                    break;
                }

                let Some(last_key) = page_keys.last().map(|key| key.to_vec()) else {
                    break;
                };

                next_lower_bound = Bound::Excluded(last_key);

                if let Some(limit) = limit
                    && row_refs.len() >= limit
                {
                    if paging_debug_enabled {
                        log::debug!(
                            "runtime indexor paging end_probe index_id={} probe_idx={} reason=row_limit_reached limit={} pages_visited={} total_row_refs={}",
                            index_id,
                            probe_idx,
                            limit,
                            pages_visited,
                            row_refs.len(),
                        );
                    }
                    break;
                }
            }

            if let Some(limit) = limit
                && row_refs.len() >= limit
            {
                break;
            }
        }

        let raw_row_refs = row_refs.len();

        row_refs.sort_unstable();
        row_refs.dedup();

        if paging_debug_enabled {
            log::debug!(
                "runtime indexor paging finalize index_id={} raw_row_refs={} deduped_row_refs={} row_limit={:?}",
                index_id,
                raw_row_refs,
                row_refs.len(),
                limit,
            );
        }

        if let Some(limit) = limit {
            row_refs.truncate(limit);
        }

        row_refs

    }

    pub fn first_row_refs(&self, limit: usize) -> Vec<u64> {

        if limit == 0 {
            return Vec::new();
        }

        let mut row_refs = self
            .entries
            .values()
            .filter_map(|row_ref| unpack_row_ref(*row_ref))
            .collect::<Vec<_>>();

        row_refs.extend(
            self.non_unique_row_refs
                .values()
                .flat_map(PostingPages::iter)
                .filter_map(|row_ref| unpack_row_ref(Some(row_ref))),
        );

        row_refs.sort_unstable();
        row_refs.dedup();
        row_refs.truncate(limit);
        row_refs

    }

    pub fn row_ref_postings_count(&self) -> usize {

        let unique_row_refs = self
            .entries
            .values()
            .filter(|row_ref| row_ref.is_some())
            .count();

        let non_unique_row_refs = self
            .non_unique_row_refs
            .values()
            .map(PostingPages::len)
            .sum::<usize>();

        unique_row_refs.saturating_add(non_unique_row_refs)

    }

    pub fn has_row_ref_postings(&self) -> bool {
        self.row_ref_postings_count() > 0
    }

    /// Build a scoped clone containing only postings for the given raw field
    /// values, avoiding an O(table rows) deep clone of the full posting map
    /// when the caller already knows the exact equality lookup value(s).
    pub fn clone_scoped_to_field_values(&self, raw_values: &HashSet<Vec<u8>>) -> Self {

        let mut encoded_keys: HashSet<Vec<u8>> = HashSet::new();

        for raw_value in raw_values {
            if let Some(encoded) = self.encode_key(std::slice::from_ref(raw_value)) {
                encoded_keys.insert(encoded);
            }

            let rendered = crate::render_stored_field_value(raw_value);
            if &rendered != raw_value
                && let Some(encoded) = self.encode_key(&[rendered])
            {
                encoded_keys.insert(encoded);
            }
        }

        let mut scoped = RuntimeIndexState {
            index: self.index.clone(),
            numeric_kind: self.numeric_kind,
            string_case_insensitive: self.string_case_insensitive,
            entries: AHashMap::new(),
            non_unique_row_refs: AHashMap::new(),
            ordered_entry_keys: BTreeSet::new(),
        };

        for encoded_key in &encoded_keys {

            let Some((shared_key, value)) = self
                .entries
                .get_key_value(encoded_key.as_slice())
                .map(|(key, value)| (Arc::clone(key), *value))
            else {
                continue;
            };

            scoped.entries.insert(Arc::clone(&shared_key), value);
            scoped.ordered_entry_keys.insert(Arc::clone(&shared_key));

            if let Some(row_refs) = self.non_unique_row_refs.get(encoded_key.as_slice()) {
                scoped.non_unique_row_refs.insert(shared_key, row_refs.clone());
            }

        }

        scoped

    }

    /// Build a clone carrying only the index metadata (no entries/postings),
    /// used when a table's index isn't referenced by the current query's
    /// known equality values; avoids an O(table rows) deep clone while still
    /// leaving a present-but-empty state for downstream miss-fallback paths.
    pub fn metadata_only_clone(&self) -> Self {
        RuntimeIndexState {
            index: self.index.clone(),
            numeric_kind: self.numeric_kind,
            string_case_insensitive: self.string_case_insensitive,
            entries: AHashMap::new(),
            non_unique_row_refs: AHashMap::new(),
            ordered_entry_keys: BTreeSet::new(),
        }
    }

    pub fn reserve_entries(&mut self, additional: usize) {
        if additional == 0 {
            return;
        }

        // The map already grows geometrically, so reserve the known shortfall and
        // let it size itself rather than projecting runway it may never use.
        self.entries.reserve(additional);
    }

}

/// Runtime indexes for all tables across all databases.
#[derive(Debug, Clone)]
pub struct RuntimeIndexStore {
    indexes: AHashMap<String, DatatypeIndexor>,
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
            materialize_non_primary: true,
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
        self.indexes.get(index_id).map(DatatypeIndexor::state)
    }

    pub fn index_for_table(&self, table_scope_id: &str, index_id: &str) -> Option<&RuntimeIndexState> {
        let scoped = scoped_index_id(table_scope_id, index_id);
        self.indexes.get(&scoped).map(DatatypeIndexor::state)
    }

    pub fn find_scoped_index_state_for_lookup<'a>(
        &'a self,
        index_id: &str,
        lookup_key: &[Vec<u8>],
    ) -> Option<(&'a str, &'a RuntimeIndexState)> {

        let normalized_index_id = common::normalize_identifier!(index_id);

        self.indexes
            .iter()
            .filter_map(|(scoped_id, state)| {
                
                if let Some((scope_id, scoped_index_id)) = scoped_id.rsplit_once("::")
                    && common::normalize_identifier!(scoped_index_id) == normalized_index_id {
                        return Some((scope_id, state));
                    }

                let normalized_scoped_id = common::normalize_identifier!(scoped_id);
                if normalized_scoped_id == normalized_index_id
                    || normalized_scoped_id.ends_with(&normalized_index_id)
                    || normalized_scoped_id.contains(&normalized_index_id)
                {
                    return Some((scoped_id.as_str(), state));
                }

                None
            })
            .find(|(_, state)| state.state().contains(lookup_key))
            .map(|(scope_id, state)| (scope_id, state.state()))

    }

    pub fn has_scoped_index_state(&self, index_id: &str) -> bool {

        let normalized_index_id = common::normalize_identifier!(index_id);

        self.indexes
            .keys()
            .any(|scoped_id| {

                if let Some((_, scoped_index_id)) = scoped_id.rsplit_once("::")
                    && common::normalize_identifier!(scoped_index_id) == normalized_index_id {
                        return true;
                    }

                let normalized_scoped_id = common::normalize_identifier!(scoped_id);
                
                normalized_scoped_id == normalized_index_id ||
                normalized_scoped_id.ends_with(&normalized_index_id) ||
                normalized_scoped_id.contains(&normalized_index_id)
                
            })

    }

    #[expect(clippy::should_implement_trait, reason="Index access by string ID, not by reference")]
    pub fn index_mut(&mut self, index_id: &str) -> &mut RuntimeIndexState {
        
        match self.indexes.entry(index_id.to_string()) {
            Entry::Occupied(entry) => entry.into_mut().state_mut(),
            Entry::Vacant(entry) => entry.insert(DatatypeIndexor::from_state(RuntimeIndexState::default())).state_mut(),
        }

    }

    pub fn index_mut_for_table(&mut self, table_scope_id: &str, index_id: &str) -> &mut RuntimeIndexState {
        
        let scoped = scoped_index_id(table_scope_id, index_id);

        match self.indexes.entry(scoped) {
            Entry::Occupied(entry) => entry.into_mut().state_mut(),
            Entry::Vacant(entry) => entry.insert(DatatypeIndexor::from_state(RuntimeIndexState::default())).state_mut(),
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
        self.indexes.entry(index_id).or_insert_with(|| DatatypeIndexor::from_state(RuntimeIndexState {
            index: Some(index),
            numeric_kind: None,
            string_case_insensitive: false,
            entries: AHashMap::new(),
            non_unique_row_refs: AHashMap::new(),
            ordered_entry_keys: BTreeSet::new(),
        }));

    }

    pub fn register_index_for_table(&mut self, table_scope_id: &str, index: &DatabaseIndex) {

        if !self.should_track_index(index) {
            return;
        }

        let index_id = scoped_index_id(table_scope_id, &index.index_id.0);
        self.indexes.entry(index_id).or_insert_with(|| DatatypeIndexor::from_state(RuntimeIndexState {
            index: Some(index.clone()),
            numeric_kind: None,
            string_case_insensitive: false,
            entries: AHashMap::new(),
            non_unique_row_refs: AHashMap::new(),
            ordered_entry_keys: BTreeSet::new(),
        }));

    }

    pub fn select_indexor_for_table(
        &mut self,
        table_scope_id: &str,
        index: &DatabaseIndex,
        field_kind: &FieldKind,
    ) {
        if !self.should_track_index(index) {
            return;
        }

        let scoped_id = scoped_index_id(table_scope_id, &index.index_id.0);
        self.indexes
            .insert(scoped_id, DatatypeIndexor::for_field_kind(index.clone(), field_kind));
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
            self.index_mut_for_table(table_scope_id, &index.index_id.0)
                .remove_with_row_ref(&key, row_ref);

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
        row_ref: Option<u64>,
    )
    where
        I: IntoIterator<Item = &'a DatabaseIndex>,
    {

        match kind {
            
            TransactionKind::Ignore => {},

            TransactionKind::Delete => self.remove_table_row_for_table(table_scope_id, indexes, row_map, row_ref),

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
        self.bootstrap_from_catalogs_filtered(catalogs, wal, None);
    }

    /// Adopt every index built by `other`, replacing any state held for the same
    /// scoped index id. Used to install a table bootstrapped on a worker thread.
    pub fn merge_from(&mut self, other: Self) {
        for (scoped_index_id, indexor) in other.indexes {
            self.indexes.insert(scoped_index_id, indexor);
        }
    }

    /// When `only_table_ids` is set, restrict the bootstrap to those tables so
    /// callers can materialize tables individually rather than as one batch.
    pub fn bootstrap_from_catalogs_filtered(
        &mut self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        wal: &ConcurrentWalManager,
        only_table_ids: Option<&HashSet<String>>,
    ) {

        let bootstrap_started_at = Instant::now();
        let preload_accessors_on_bootstrap = runtime_index_preload_accessors_on_bootstrap();
        let realign_wal_records = runtime_index_realign_wal_records_on_bootstrap();
        let snapshot_data_dir = wal.data_dir_path();

        if realign_wal_records
            && let Some(data_dir) = snapshot_data_dir.as_ref()
        {
            let derived_dir = data_dir.join("runtime-index");
            if let Err(err) = std::fs::remove_dir_all(&derived_dir)
                && err.kind() != std::io::ErrorKind::NotFound
            {
                log::warn!(
                    "runtime index realignment could not clear derived state path={} error={}",
                    derived_dir.display(),
                    err,
                );
            }
            log::warn!(
                "runtime index WAL realignment enabled; derived snapshots/caches will be rebuilt"
            );
        }

        log::info!(
            "runtime index bootstrap mode materialize_non_primary={} preload_accessors_on_bootstrap={} non_primary_field_allowlist={} non_primary_index_allowlist={}",
            self.materialize_non_primary,
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
            .map(|catalog| {
                catalog
                    .table_ids()
                    .into_iter()
                    .filter(|table_id| {
                        only_table_ids.is_none_or(|only| only.contains(table_id))
                    })
                    .count()
            })
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

        for (database_id, catalog) in catalogs {
            
            for table_id in catalog.table_ids() {

                if only_table_ids.is_some_and(|only| !only.contains(&table_id)) {
                    continue;
                }

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

                if realign_wal_records {
                    match wal.validate_stream_record_positions(&table_stream_id) {
                        Ok(()) => log::info!(
                            "runtime index WAL positions already aligned database={} table={} stream={}",
                            database_id,
                            table_id,
                            table_stream_id,
                        ),
                        Err(err) => log::warn!(
                            "runtime index WAL positions misaligned database={} table={} stream={} reason={}",
                            database_id,
                            table_id,
                            table_stream_id,
                            err,
                        ),
                    }

                    if let Err(err) = wal.realign_stream_records(&table_stream_id) {
                        log::warn!(
                            "runtime index WAL realignment skipped database={} table={} stream={} reason={}",
                            database_id,
                            table_id,
                            table_stream_id,
                            err,
                        );
                    } else {
                        match wal.validate_stream_record_positions(&table_stream_id) {
                            Ok(()) => log::info!(
                                "runtime index WAL realigned and positions validated database={} table={} stream={}",
                                database_id,
                                table_id,
                                table_stream_id,
                            ),
                            Err(err) => log::error!(
                                "runtime index WAL realignment validation failed database={} table={} stream={} reason={}",
                                database_id,
                                table_id,
                                table_stream_id,
                                err,
                            ),
                        }
                    }
                }

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
                    let field_kind = if index.field_names.len() == 1 {
                        table.schema.field(&index.field_names[0]).map(|field| field.field_type.clone())
                    } else if index.field_names.is_empty() && !index.field_name.is_empty() {
                        table.schema.field(&index.field_name).map(|field| field.field_type.clone())
                    } else {
                        None
                    };
                    if let Some(field_kind) = field_kind {
                        self.select_indexor_for_table(&table_stream_id, index, &field_kind);
                    }
                    let state = self.index_mut_for_table(&table_stream_id, &index.index_id.0);
                    state.set_numeric_kind(numeric_kind_for_index(index, &table.schema));
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
                    let mut effective_live_row_count = snapshot.live_row_count;
                    let mut snapshot_table_mode = "snapshot";

                    let mut restored_index_count = 0usize;
                    let mut restored_entry_count = 0usize;
                    let mut snapshot_postings_incomplete = false;

                    for index in &tracked_indexes {
                        let Some(snapshot_index) = snapshot
                            .indexes
                            .iter()
                            .find(|item| item.index_id == index.index_id.0) else {
                            continue;
                        };

                        restored_index_count = restored_index_count.saturating_add(1);
                        restored_entry_count = restored_entry_count.saturating_add(snapshot_index.entries.len());

                        let has_aligned_postings =
                            snapshot_index.postings_by_entry.len() == snapshot_index.entries.len();

                        if !index.is_unique_key()
                            && !snapshot_index.entries.is_empty()
                            && !has_aligned_postings
                            && snapshot_index.row_refs.len() < snapshot_index.entries.len()
                        {
                            snapshot_postings_incomplete = true;
                        }

                        let state = self.index_mut_for_table(&table_stream_id, &index.index_id.0);
                        state.index = Some(index.clone());
                        state.entries.clear();
                        state.non_unique_row_refs.clear();
                        state.reserve_entries(snapshot_index.entries.len());

                        if index.is_unique_key()
                            && snapshot_index.row_refs_by_entry.len() == snapshot_index.entries.len()
                        {
                            for (key, packed_row_ref) in snapshot_index
                                .entries
                                .iter()
                                .zip(snapshot_index.row_refs_by_entry.iter())
                            {
                                let row_ref = if *packed_row_ref == 0 {
                                    None
                                } else {
                                    Some(packed_row_ref.saturating_sub(1))
                                };
                                state.insert_with_row_ref(key.clone(), row_ref);
                            }
                        } else {
                            if index.is_unique_key() {
                                let row_refs_lookup = snapshot_index
                                    .row_refs
                                    .iter()
                                    .map(|(key, row_ref)| (key, *row_ref))
                                    .collect::<AHashMap<_, _>>();

                                for key in &snapshot_index.entries {
                                    let row_ref = row_refs_lookup.get(key).copied();
                                    state.insert_with_row_ref(key.clone(), row_ref);
                                }
                            } else if has_aligned_postings {
                                for (key, row_refs) in snapshot_index
                                    .entries
                                    .iter()
                                    .zip(snapshot_index.postings_by_entry.iter())
                                {
                                    if row_refs.is_empty() {
                                        state.insert_with_row_ref(key.clone(), None);
                                        continue;
                                    }

                                    for row_ref in row_refs {
                                        state.insert_with_row_ref(key.clone(), Some(*row_ref));
                                    }
                                }
                            } else {
                                let mut row_refs_lookup = AHashMap::<Vec<Vec<u8>>, Vec<u64>>::new();

                                for (key, row_ref) in &snapshot_index.row_refs {
                                    row_refs_lookup
                                        .entry(key.clone())
                                        .or_default()
                                        .push(*row_ref);
                                }

                                for key in &snapshot_index.entries {
                                    if let Some(row_refs) = row_refs_lookup.get(key) {
                                        for row_ref in row_refs {
                                            state.insert_with_row_ref(key.clone(), Some(*row_ref));
                                        }
                                    } else {
                                        state.insert_with_row_ref(key.clone(), None);
                                    }
                                }
                            }
                        }
                    }

                    if restored_index_count != tracked_indexes.len() || snapshot_postings_incomplete {
                        log::warn!(
                            "runtime index snapshot restore backfill required database={} table={} expected_indexes={} restored_indexes={} snapshot_postings_incomplete={}",
                            database_id,
                            table_id,
                            tracked_indexes.len(),
                            restored_index_count,
                            snapshot_postings_incomplete,
                        );

                        let (latest_tx_id, live_rows, live_rows_mode, live_rows_elapsed_ms) =
                            load_bootstrap_live_rows(
                                snapshot_data_dir.as_ref(),
                                wal,
                                &table,
                                &table_stream_id,
                                wal_fingerprint,
                                snapshot.latest_tx_id,
                            );

                        let rebuild_started_at = Instant::now();
                        rebuild_bootstrap_indexes_from_live_rows(
                            self,
                            &table_stream_id,
                            &tracked_indexes,
                            &live_rows,
                        );
                        let rebuild_elapsed_ms = rebuild_started_at.elapsed().as_millis();

                        effective_live_row_count = live_rows.len();
                        snapshot_table_mode = "snapshot_backfill";

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

                        if let Some(data_dir) = snapshot_data_dir.as_ref()
                            && let Err(err) = persist_runtime_index_snapshot(
                                self,
                                data_dir,
                                &table,
                                &table_stream_id,
                                latest_tx_id,
                                effective_live_row_count,
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

                        log::warn!(
                            "runtime index snapshot restore backfill database={} table={} source={} live_rows={} live_row_materialization_ms={} index_rebuild_ms={}",
                            database_id,
                            table_id,
                            live_rows_mode,
                            effective_live_row_count,
                            live_rows_elapsed_ms,
                            rebuild_elapsed_ms,
                        );
                    }

                    bootstrapped_rows += effective_live_row_count;

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
                                effective_live_row_count,
                                table_started_at.elapsed().as_millis(),
                            );

                            log_runtime_index_bootstrap_table_memory_profile(
                                self,
                                &table_stream_id,
                                database_id,
                                &table_id,
                                &tracked_indexes,
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
                        "runtime index bootstrap table complete database={} table={} indexes={} live_rows={} mode={} elapsed_ms={}",
                        database_id,
                        table_id,
                        tracked_indexes.len(),
                        effective_live_row_count,
                        snapshot_table_mode,
                        table_started_at.elapsed().as_millis(),
                    );

                    log_runtime_index_bootstrap_table_memory_profile(
                        self,
                        &table_stream_id,
                        database_id,
                        &table_id,
                        &tracked_indexes,
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
                    log::debug!(
                        "runtime index bootstrap equality warm skipped database={} table={} reason=preload_disabled",
                        database_id,
                        table_id,
                    );
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

                log_runtime_index_bootstrap_table_memory_profile(
                    self,
                    &table_stream_id,
                    database_id,
                    &table_id,
                    &tracked_indexes,
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
                            scoped.indexes.insert(scoped_id, DatatypeIndexor::from_state(state.clone()));
                        }
                    }
                });

            }

        }

        scoped
        
    }

    pub fn clone_for_tables_unique_indexes(
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
                        if !index.is_unique_key() {
                            continue;
                        }

                        if let Some(state) = self.index_for_table(&table_stream_id, &index.index_id.0) {
                            let scoped_id = scoped_index_id(&table_stream_id, &index.index_id.0);
                            scoped.indexes.insert(scoped_id, DatatypeIndexor::from_state(state.clone()));
                        }
                    }
                });

            }

        }

        scoped

    }

    pub fn clone_for_tables_unique_and_selected_single_field_indexes(
        &self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        table_ids: &HashSet<String>,
        selected_fields_by_table: &HashMap<String, HashSet<String>>,
    ) -> Self {
        self.clone_for_tables_unique_and_selected_single_field_indexes_with_values(
            catalogs,
            table_ids,
            selected_fields_by_table,
            &HashMap::new(),
        )
    }

    /// Same as `clone_for_tables_unique_and_selected_single_field_indexes`, but when the
    /// caller already knows the exact equality lookup value(s) for a selected non-unique
    /// field, only the matching postings are cloned instead of the entire index's
    /// row-ref map (which is O(table rows) to deep-clone on large tables).
    pub fn clone_for_tables_unique_and_selected_single_field_indexes_with_values(
        &self,
        catalogs: &HashMap<String, DatabaseCatalog>,
        table_ids: &HashSet<String>,
        selected_fields_by_table: &HashMap<String, HashSet<String>>,
        selected_field_values_by_table: &HashMap<String, HashMap<String, HashSet<Vec<u8>>>>,
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

                let selected_fields = selected_fields_by_table
                    .get(&table_id)
                    .or_else(|| {
                        selected_fields_by_table
                            .get(&common::normalize_identifier!(&table_id))
                    });

                let selected_field_values = selected_field_values_by_table
                    .get(&table_id)
                    .or_else(|| {
                        selected_field_values_by_table
                            .get(&common::normalize_identifier!(&table_id))
                    });

                table_handle.read_table(|table| {
                    for index in table.indexes.values() {
                        let index_single_field = if index.field_names.len() == 1 {
                            index.field_names.first()
                        } else if index.field_names.is_empty() {
                            Some(&index.field_name)
                        } else {
                            None
                        };

                        let field_is_selected = index_single_field.is_some_and(|index_field| {
                            let normalized_index_field = common::normalize_identifier!(index_field);
                            selected_fields.is_some_and(|fields| {
                                fields.contains(&normalized_index_field) || fields.contains(index_field)
                            })
                        });

                        let include_index = if index.is_unique_key() {
                            true
                        } else {
                            field_is_selected
                        };

                        if !include_index {
                            continue;
                        }

                        if let Some(state) = self.index_for_table(&table_stream_id, &index.index_id.0) {

                            let known_values = index_single_field.and_then(|index_field| {
                                let normalized_index_field = common::normalize_identifier!(index_field);
                                selected_field_values.and_then(|values| {
                                    values
                                        .get(&normalized_index_field)
                                        .or_else(|| values.get(index_field))
                                })
                            });

                            // When we know the exact lookup value(s), scope-clone first and
                            // gate on postings in the (tiny) scoped result instead of
                            // scanning the full O(table rows) index up front.
                            if !index.is_unique_key()
                                && let Some(values) = known_values
                                && !values.is_empty()
                            {
                                let scoped_state = state.clone_scoped_to_field_values(values);

                                if !scoped_state.has_row_ref_postings() {
                                    log::debug!(
                                        "runtime index scoped clone skipped selected non-unique index without postings table={} stream={} index_id={} entries={}",
                                        table.table_id,
                                        table_stream_id,
                                        index.index_id.0,
                                        scoped_state.cardinality(),
                                    );
                                    continue;
                                }

                                let scoped_id = scoped_index_id(&table_stream_id, &index.index_id.0);
                                scoped.indexes.insert(scoped_id, DatatypeIndexor::from_state(scoped_state));
                                continue;
                            }

                            if !index.is_unique_key() && !state.has_row_ref_postings() {
                                log::debug!(
                                    "runtime index scoped clone skipped selected non-unique index without postings table={} stream={} index_id={} entries={}",
                                    table.table_id,
                                    table_stream_id,
                                    index.index_id.0,
                                    state.cardinality(),
                                );
                                continue;
                            }

                            let scoped_state = if index.is_unique_key()
                                && index_single_field.is_some()
                                && !field_is_selected
                            {
                                // Single-field unique/primary index not referenced by any
                                // known equality filter for this request: keep metadata
                                // only rather than deep-cloning the full O(table rows)
                                // posting map. Composite-key unique indexes still fall
                                // through to a full clone since we can't cheaply scope
                                // a multi-field match here.
                                state.metadata_only_clone()
                            } else {
                                state.clone()
                            };

                            let scoped_id = scoped_index_id(&table_stream_id, &index.index_id.0);
                            scoped.indexes.insert(scoped_id, DatatypeIndexor::from_state(scoped_state));
                        }
                    }
                });

            }

        }

        scoped

    }

    pub fn clone_for_tables_index_metadata_only(
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
                        let scoped_id = scoped_index_id(&table_stream_id, &index.index_id.0);
                        scoped.indexes.insert(
                            scoped_id,
                            DatatypeIndexor::from_state(RuntimeIndexState {
                                index: Some(index.clone()),
                                numeric_kind: None,
                                string_case_insensitive: false,
                                entries: AHashMap::new(),
                                non_unique_row_refs: AHashMap::new(),
                                ordered_entry_keys: BTreeSet::new(),
                            }),
                        );
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
            scoped.indexes.insert(scoped_id, DatatypeIndexor::from_state(state.clone()));
            continue;
        }

        if let Some(state) = store.index(&index.index_id.0) {
            scoped.indexes.insert(scoped_id, DatatypeIndexor::from_state(state.clone()));
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

        if checkpoint.live_rows.is_empty()
            && let Some((wal_size_bytes, _)) = wal_fingerprint
            && wal_size_bytes > 0
        {
            let wal_probe_started_at = Instant::now();
            let wal_rows = load_live_rows_in_place(
                wal,
                table_stream_id,
                &table.schema,
            );

            if !wal_rows.is_empty() {
                let wal_probe_elapsed_ms = wal_probe_started_at.elapsed().as_millis();

                log::warn!(
                    "runtime index bootstrap live-row checkpoint mismatch table={} stream={} checkpoint_rows=0 wal_rows={} checkpoint_ms={} wal_probe_ms={} source=wal",
                    table.table_id,
                    table_stream_id,
                    wal_rows.len(),
                    checkpoint_elapsed_ms,
                    wal_probe_elapsed_ms,
                );

                return (
                    fallback_latest_tx_id,
                    wal_rows,
                    "wal",
                    checkpoint_elapsed_ms.saturating_add(wal_probe_elapsed_ms),
                );
            }
        }

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

        let mut entries = Vec::with_capacity(state.entries.len());
        let mut row_refs_by_entry = Vec::with_capacity(state.entries.len());
        let mut postings_by_entry = Vec::with_capacity(state.entries.len());

        for (key, row_ref) in &state.entries {
            if let Some(decoded_key) = decode_runtime_index_entry_key(key) {
                let postings = if index.is_unique_key() {
                    Vec::new()
                } else {
                    state.row_refs_for_key(&decoded_key, None)
                };

                entries.push(decoded_key);
                let packed_row_ref = unpack_row_ref(*row_ref)
                    .and_then(|row_ref| row_ref.checked_add(1))
                    .unwrap_or(0);
                row_refs_by_entry.push(packed_row_ref);
                postings_by_entry.push(postings);
            }
        }

        indexes.push(RuntimeIndexSnapshotIndex {
            index_id: index.index_id.0.clone(),
            entries,
            row_refs_by_entry,
            postings_by_entry,
            row_refs: Vec::new(),
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
        let state = store.index_mut_for_table(table_stream_id, &index.index_id.0);
        state.index = Some(index.clone());

        // Rebuild directly into index state to avoid temporary duplicate key
        // structures during bootstrap (set + row-ref map + final map).
        state.entries.clear();
        state.reserve_entries(live_rows.len());

        for live_rows_chunk in live_rows.chunks(chunk_rows) {
            for (row_id, row_map) in live_rows_chunk {
                let key = index_value_tuple(index, row_map);
                state.insert_with_row_ref(key, Some(*row_id));
            }
        }

    }

}

impl Default for RuntimeIndexStore {
    
    fn default() -> Self {
        Self::new()
    }

}

fn runtime_index_non_primary_field_allowlist() -> AHashSet<String> {
    parse_runtime_index_allowlist_env(common::settings::RUNTIME_INDEX_NON_PRIMARY_FIELDS)
}

fn runtime_index_non_primary_index_allowlist() -> AHashSet<String> {
    parse_runtime_index_allowlist_env(common::settings::RUNTIME_INDEX_NON_PRIMARY_INDEX_IDS)
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

    let Some(value) = common::settings::text(var_name) else {
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
