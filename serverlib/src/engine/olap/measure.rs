
/// The aggregation function applied to a measure column within a cube cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasureAggregation {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

impl std::fmt::Display for MeasureAggregation {

    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sum   => write!(f, "SUM"),
            Self::Count => write!(f, "COUNT"),
            Self::Min   => write!(f, "MIN"),
            Self::Max   => write!(f, "MAX"),
            Self::Avg   => write!(f, "AVG"),
        }
    }

}

/// A column that contributes a numeric aggregate to each hypercube cell.
///
/// Measures are the numeric leaves of the cube — the values you aggregate
/// (sum, count, average, etc.) once the dimension coordinates have partitioned
/// the data into cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubeMeasure {
    /// Column name in the source `TableSchema` (normalized, lower-case).
    pub field_name: String,
    /// Field sequence number (`FieldDef.seqno`) for fast lookup in the row map.
    pub field_seqno: u32,
    /// The aggregation to apply when accumulating row values into a cell.
    pub aggregation: MeasureAggregation,
}

impl CubeMeasure {

    pub fn new(
        field_name: impl Into<String>,
        field_seqno: u32,
        aggregation: MeasureAggregation,
    ) -> Self {
        Self {
            field_name: field_name.into().trim().to_ascii_lowercase(),
            field_seqno,
            aggregation,
        }
    }

}
