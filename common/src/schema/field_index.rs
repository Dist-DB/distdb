
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldIndex {
    None,
    Indexed,
    PrimaryKey,
}
