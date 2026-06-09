
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldType {
    Int(u8),
    UInt(u8),
    Float(u8),
    StringFixed(usize),
    Text,
    Enum(Vec<String>),
    Spatial,
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldDef {
    pub seqno: u32,
    pub field_name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub indexed: bool,
    pub default_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchema {
    pub fields: Vec<FieldDef>,
}