
use std::collections::HashMap;

use super::coordinate::DimensionCoordinate;
use super::dimension::CubeDimension;
use super::measure::CubeMeasure;

/// One cell in the hypercube at a fully-specified coordinate.
///
/// A cell is the intersection of a unique combination of dimension coordinates.
/// It holds the ids of all source rows that contributed to it, plus the
/// pre-aggregated measure values derived from those rows.
///
/// Source row ids are retained so that drill-through queries can recover the
/// original rows from the `RuntimeIndex` without re-scanning the full table.
#[derive(Debug, Clone)]
pub struct HypercubeCell {
    /// Row ids from the source live-row store that map to this coordinate.
    pub source_row_ids: Vec<u64>,
    /// Pre-aggregated measure values in the same order as `Hypercube.measures`.
    /// `None` indicates the measure value was NULL / not computable for this cell.
    pub measures: Vec<Option<f64>>,
}

/// A memory-resident, coordinate-addressed view over a snapshot of committed
/// live rows from a single source table.
///
/// ## Invariants
///
/// - The hypercube does NOT own row data. It holds `source_row_ids` that
///   reference rows in the `RuntimeIndex` snapshot it was built from.
/// - The cube becomes stale when `snapshot_tx_id` is older than the
///   `RuntimeIndex`'s current `latest_tx_id`. The `HypercubeCache` manages
///   invalidation and rebuild scheduling.
/// - The cube is never persisted. On restart it is rebuilt from warm live rows
///   using the `DatabaseOlapView` definition loaded from the catalog.
/// - Only committed rows are visible. Staged (in-flight transaction) rows are
///   never present in a cube.
#[derive(Debug, Clone)]
pub struct Hypercube {
    /// Source table this cube was built from.
    pub table_id: String,
    /// Name of the `DatabaseOlapView` that defines this cube.
    pub view_id: String,
    /// The transaction id of the live-row snapshot used at construction time.
    /// Used to determine staleness relative to `RuntimeIndex.latest_tx_id`.
    pub snapshot_tx_id: u64,
    /// Ordered axis definitions. `dimensions[0]` is always the z-dimension
    /// pivot declared in `CREATE OLAPVIEW ... USING <column>`.
    pub dimensions: Vec<CubeDimension>,
    /// Measure definitions in the same order as `HypercubeCell.measures`.
    pub measures: Vec<CubeMeasure>,
    /// Coordinate map. The key is a `Vec` of `DimensionCoordinate` with one
    /// entry per axis (same length and order as `dimensions`).
    cells: HashMap<Vec<DimensionCoordinate>, HypercubeCell>,
}

impl Hypercube {

    /// Construct an empty hypercube shell. Cells are populated by
    /// `HypercubeBuilder::build`.
    pub fn new(
        table_id: impl Into<String>,
        view_id: impl Into<String>,
        snapshot_tx_id: u64,
        dimensions: Vec<CubeDimension>,
        measures: Vec<CubeMeasure>,
    ) -> Self {
        Self {
            table_id: table_id.into(),
            view_id: view_id.into(),
            snapshot_tx_id,
            dimensions,
            measures,
            cells: HashMap::new(),
        }
    }

    /// Insert or replace a cell at the given coordinate key.
    pub(super) fn insert_cell(&mut self, key: Vec<DimensionCoordinate>, cell: HypercubeCell) {
        self.cells.insert(key, cell);
    }

    /// Look up the cell at an exact coordinate.
    pub fn cell(&self, key: &[DimensionCoordinate]) -> Option<&HypercubeCell> {
        self.cells.get(key)
    }

    /// Return all cells whose coordinate matches on the primary z-axis value.
    pub fn slice_z(&self, z: &DimensionCoordinate) -> Vec<(&Vec<DimensionCoordinate>, &HypercubeCell)> {

        self.cells
            .iter()
            .filter(|(key, _)| key.first() == Some(z))
            .collect()

    }

    /// Total number of populated cells.
    pub fn cell_count(&self) -> usize {
        
        self.cells.len()

    }

    /// Total number of source rows represented across all cells.
    pub fn row_count(&self) -> usize {
        
        self.cells.values().map(|c| c.source_row_ids.len()).sum()

    }

    /// Returns `true` when this cube was built from an older snapshot than
    /// the provided current transaction id.
    pub fn is_stale(&self, current_tx_id: u64) -> bool {
        
        self.snapshot_tx_id < current_tx_id
        
    }

}
