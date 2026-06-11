use super::field_types::{FieldIndex, FieldType};
use common::schema::FieldMetadata;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldDef {
    pub seqno: u32,
    pub field_name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub indexed: FieldIndex,
    pub default_value: Option<Vec<u8>>,
    #[serde(default)]
    pub metadata: Option<FieldMetadata>,
}
