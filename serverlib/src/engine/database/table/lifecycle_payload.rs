
use crate::engine::database::table::schema::TableSchema;
use crate::engine::database::transaction::payload::TransactionPayloadCodec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TableLifecycleAction {
    Create,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableLifecyclePayload {
    pub table_id: String,
    pub action: TableLifecycleAction,
    #[serde(default)]
    pub schema_epoch: u64,
    #[serde(default)]
    pub entity_id: Option<String>,
    pub schema: Option<TableSchema>,
}

impl TableLifecyclePayload {

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        <Self as TransactionPayloadCodec>::encode_payload(self)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        <Self as TransactionPayloadCodec>::decode_payload(payload)
    }
    
}
