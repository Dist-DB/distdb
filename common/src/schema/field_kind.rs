
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldKind {
    Int(u8),
    UInt(u8),
    Float(u8),
    StringFixed(usize),
    Text,
    Enum(Vec<String>),
    Spatial,
    Blob,
}
