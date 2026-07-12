
/// A column from the source schema nominated as a cube axis.
///
/// The z-dimension is the primary pivot axis declared in
/// `CREATE OLAPVIEW ... USING <column>`. Additional dimensions may be
/// nominated at query time for multi-dimensional slicing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CubeDimension {
    /// Column name in the source `TableSchema` (normalized, lower-case).
    pub field_name: String,
    /// Field sequence number (`FieldDef.seqno`) for fast lookup in the row map.
    pub field_seqno: u32,
    /// Axis ordinal within the cube (z = 0 for the primary pivot; additional
    /// axes are numbered from 1 in declaration order).
    pub axis: usize,
}

impl CubeDimension {

    pub fn new(field_name: impl Into<String>, field_seqno: u32, axis: usize) -> Self {
        Self {
            field_name: field_name.into().trim().to_ascii_lowercase(),
            field_seqno,
            axis,
        }
    }

    /// Returns `true` when this is the primary z-dimension pivot axis.
    pub fn is_primary(&self) -> bool {
        self.axis == 0
    }

}
