use crate::engine::database::table::schema::TableSchema;
use crate::engine::database::transaction::payload::TransactionPayloadCodec;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaChangePayload {
    pub table_id: String,
    pub schema_revision: u64,
    #[serde(default)]
    pub schema_epoch: u64,
    #[serde(default)]
    pub entity_id: Option<String>,
    pub schema: TableSchema,
}

impl SchemaChangePayload {
    
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        <Self as TransactionPayloadCodec>::encode_payload(self)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        <Self as TransactionPayloadCodec>::decode_payload(payload)
    }

}
