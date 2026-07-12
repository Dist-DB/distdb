
use std::collections::HashMap;

use super::hypercube::Hypercube;

/// The materialization state of a cached cube entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CubeStatus {
    /// The cube is current relative to the source table's latest committed tx.
    Current,
    /// The cube was built from an older snapshot and needs rebuilding before
    /// query results can be trusted. It may still be served stale with a
    /// watermark disclosure if the query layer chooses to do so.
    Stale,
    /// The cube has been explicitly invalidated (e.g. after `DROP OLAPVIEW` or
    /// a structural schema change to the source table) and must not be served.
    Invalidated,
}

/// A single entry in the `HypercubeCache`.
pub struct HypercubeCacheEntry {
    /// The materialized cube.
    pub cube: Hypercube,
    /// Current materialization state.
    pub status: CubeStatus,
}

impl HypercubeCacheEntry {

    pub fn new(cube: Hypercube) -> Self {
        Self { cube, status: CubeStatus::Current }
    }

    /// Mark this entry stale without evicting it. Stale cubes can still be
    /// read under an explicit stale-read policy while a rebuild is pending.
    pub fn mark_stale(&mut self) {
        if self.status == CubeStatus::Current {
            self.status = CubeStatus::Stale;
        }
    }

    /// Mark this entry invalidated. Invalidated entries must not be served
    /// and should be removed or replaced on the next rebuild.
    pub fn invalidate(&mut self) {
        self.status = CubeStatus::Invalidated;
    }

}

/// In-process cache of materialized hypercubes, keyed by OLAP view name.
///
/// The cache is the runtime owner of all `Hypercube` instances. It holds no
/// persistent state — on restart the `HypercubeBuilder` repopulates it from
/// warm live rows using each `DatabaseOlapView` definition in the catalog.
///
/// Lifecycle:
/// - **Insert**: called after a successful `HypercubeBuilder::build`.
/// - **Stale**: called when a WAL commit advances the source table's tx id.
/// - **Invalidate**: called when the `DatabaseOlapView` is dropped or the
///   source table's schema changes incompatibly.
/// - **Remove**: removes the entry entirely (post-invalidation cleanup).
pub struct HypercubeCache {
    entries: HashMap<String, HypercubeCacheEntry>,
}

impl HypercubeCache {

    pub fn new() -> Self {
        Self { entries: HashMap::new() }
    }

    /// Insert or replace the cube for the given OLAP view name.
    pub fn insert(&mut self, view_id: impl Into<String>, cube: Hypercube) {

        self.entries.insert(
            view_id.into().trim().to_ascii_lowercase(),
            HypercubeCacheEntry::new(cube),
        );

    }

    /// Return a reference to the cache entry for the given view, if present.
    pub fn entry(&self, view_id: &str) -> Option<&HypercubeCacheEntry> {
        self.entries.get(&view_id.trim().to_ascii_lowercase())
    }

    /// Return the materialized cube for the given view if it is `Current`.
    /// Returns `None` when the entry is absent, stale, or invalidated.
    pub fn current_cube(&self, view_id: &str) -> Option<&Hypercube> {

        self.entry(view_id).and_then(|e| {
            if e.status == CubeStatus::Current {
                Some(&e.cube)
            } else {
                None
            }
        })

    }

    /// Return the cube for the given view regardless of staleness status,
    /// provided the entry has not been explicitly invalidated.
    pub fn cube_or_stale(&self, view_id: &str) -> Option<(&Hypercube, CubeStatus)> {

        self.entry(view_id).and_then(|e| {
            if e.status != CubeStatus::Invalidated {
                Some((&e.cube, e.status))
            } else {
                None
            }
        })

    }

    /// Mark the cube for `view_id` as stale. No-op when no entry exists.
    pub fn mark_stale(&mut self, view_id: &str) {

        if let Some(entry) = self.entries.get_mut(&view_id.trim().to_ascii_lowercase()) {
            entry.mark_stale();
        }

    }

    /// Invalidate the cube for `view_id`. No-op when no entry exists.
    pub fn invalidate(&mut self, view_id: &str) {

        if let Some(entry) = self.entries.get_mut(&view_id.trim().to_ascii_lowercase()) {
            entry.invalidate();
        }

    }

    /// Remove the entry for `view_id` entirely. Used after `DROP OLAPVIEW`.
    pub fn remove(&mut self, view_id: &str) {
        self.entries.remove(&view_id.trim().to_ascii_lowercase());
    }

    /// Returns the names of all views whose entries are currently stale.
    pub fn stale_view_ids(&self) -> Vec<String> {
        
        self.entries
            .iter()
            .filter_map(|(id, e)| {
                if e.status == CubeStatus::Stale {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect()

    }

    /// Total number of entries (current, stale, and invalidated).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

}

impl Default for HypercubeCache {
    
    fn default() -> Self {
        Self::new()
    }

}
