
use super::schema_def::TableSchema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableSchemaRevision {
    pub revision: u64,
    pub schema: TableSchema,
}
