
use std::collections::HashMap;

use super::coordinate::DimensionCoordinate;
use super::dimension::CubeDimension;
use super::hypercube::{Hypercube, HypercubeCell};
use super::measure::{CubeMeasure, MeasureAggregation};

/// Error type for hypercube construction failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HypercubeBuildError {
    /// The nominated z-dimension column was not found in the row data.
    DimensionColumnNotFound(String),
    /// A dimension coordinate could not be decoded from the raw field bytes.
    CoordinateDecodeError { field_name: String },
    /// No dimensions were provided — a cube requires at least one axis.
    NoDimensions,
}

impl std::fmt::Display for HypercubeBuildError {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        match self {

            Self::DimensionColumnNotFound(col) => {
                write!(f, "z-dimension column '{col}' not found in row data")
            },

            Self::CoordinateDecodeError { field_name } => {
                write!(f, "failed to decode coordinate for field '{field_name}'")
            },

            Self::NoDimensions => write!(f, "hypercube requires at least one dimension axis"),

        }

    }
    
}

/// Constructs a `Hypercube` from a snapshot of committed live rows.
///
/// The builder reads from the live-row map already held by `RuntimeIndex` and
/// organises rows into cells keyed by their dimension coordinates.
/// It does not write to any persistent store.
pub struct HypercubeBuilder {
    table_id: String,
    view_id: String,
    snapshot_tx_id: u64,
    dimensions: Vec<CubeDimension>,
    measures: Vec<CubeMeasure>,
}

impl HypercubeBuilder {

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
        }
    }

    /// Build a `Hypercube` from the provided live rows.
    ///
    /// `live_rows` is the decoded row map: `row_id → field_name → raw_bytes`.
    /// Row data is never consumed or cloned in bulk — only the decoded scalar
    /// values needed for coordinates and measures are extracted per row.
    pub fn build(
        &self,
        live_rows: &HashMap<u64, HashMap<String, Vec<u8>>>,
    ) -> Result<Hypercube, HypercubeBuildError> {

        if self.dimensions.is_empty() {
            return Err(HypercubeBuildError::NoDimensions);
        }

        let mut cube = Hypercube::new(
            self.table_id.clone(),
            self.view_id.clone(),
            self.snapshot_tx_id,
            self.dimensions.clone(),
            self.measures.clone(),
        );

        // Accumulate raw values per cell before computing aggregates.
        // key: coordinate vec  →  (row_ids, measure_raw_values[measure_idx])
        let mut acc: HashMap<Vec<DimensionCoordinate>, (Vec<u64>, Vec<Vec<f64>>)> =
            HashMap::new();

        for (row_id, fields) in live_rows {

            let key = self.extract_coordinate_key(fields)?;

            let entry = acc.entry(key).or_insert_with(|| {
                (Vec::new(), vec![Vec::new(); self.measures.len()])
            });

            entry.0.push(*row_id);

            for (idx, measure) in self.measures.iter().enumerate() {
                if let Some(raw) = fields.get(&measure.field_name) {
                    if let Some(val) = decode_f64(raw) {
                        entry.1[idx].push(val);
                    }
                }
            }

        }

        for (key, (row_ids, raw_measures)) in acc {

            let measures = raw_measures
                .iter()
                .zip(self.measures.iter())
                .map(|(vals, def)| aggregate(vals, def.aggregation))
                .collect();

            cube.insert_cell(key, HypercubeCell { source_row_ids: row_ids, measures });

        }

        Ok(cube)

    }

    fn extract_coordinate_key(
        &self,
        fields: &HashMap<String, Vec<u8>>,
    ) -> Result<Vec<DimensionCoordinate>, HypercubeBuildError> {

        let mut key = Vec::with_capacity(self.dimensions.len());

        for dim in &self.dimensions {

            let coord = match fields.get(&dim.field_name) {
                None => DimensionCoordinate::Null,
                Some(raw) => decode_coordinate(raw).ok_or_else(|| {
                    HypercubeBuildError::CoordinateDecodeError {
                        field_name: dim.field_name.clone(),
                    }
                })?,
            };

            key.push(coord);

        }

        Ok(key)

    }

}

/// Decode raw field bytes into a `DimensionCoordinate`.
///
/// Field values are stored as UTF-8 text representations by the row payload
/// layer. Integers and booleans are attempted first; anything else becomes
/// `Text`. A completely empty/absent value yields `Null`.
fn decode_coordinate(raw: &[u8]) -> Option<DimensionCoordinate> {

    if raw.is_empty() {
        return Some(DimensionCoordinate::Null);
    }

    let s = std::str::from_utf8(raw).ok()?;

    if let Ok(n) = s.parse::<i64>() {
        return Some(DimensionCoordinate::Integer(n));
    }

    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "1"  => return Some(DimensionCoordinate::Boolean(true)),
        "false" | "0" => return Some(DimensionCoordinate::Boolean(false)),
        _ => {}
    }

    Some(DimensionCoordinate::Text(s.to_string()))

}

/// Decode raw field bytes as a floating-point measure value.
fn decode_f64(raw: &[u8]) -> Option<f64> {
    std::str::from_utf8(raw).ok()?.trim().parse::<f64>().ok()
}

/// Apply a `MeasureAggregation` to a slice of raw f64 values.
fn aggregate(vals: &[f64], agg: MeasureAggregation) -> Option<f64> {

    if vals.is_empty() {
        return None;
    }

    Some(match agg {

        MeasureAggregation::Sum     => vals.iter().sum(),

        MeasureAggregation::Count   => vals.len() as f64,

        MeasureAggregation::Min     => vals.iter().cloned().fold(f64::INFINITY, f64::min),

        MeasureAggregation::Max     => vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max),

        MeasureAggregation::Avg     => {
            let sum: f64 = vals.iter().sum();
            sum / vals.len() as f64
        }

    })

}
