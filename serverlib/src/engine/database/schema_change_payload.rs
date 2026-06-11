use super::table_schema::TableSchema;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaChangePayload {
    pub table_id: String,
    pub schema_revision: u64,
    #[serde(default)]
    pub schema_epoch: u64,
    pub schema: TableSchema,
}

impl SchemaChangePayload {
    
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        bincode::serialize(self).map_err(|_| "failed to serialize schema change payload")
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        bincode::deserialize(payload).map_err(|_| "failed to deserialize schema change payload")
    }

}
