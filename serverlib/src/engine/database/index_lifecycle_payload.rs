use crate::engine::database::index::DatabaseIndex;
use crate::engine::database::transaction::payload::TransactionPayloadCodec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum IndexLifecycleAction {
    Create,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct IndexLifecyclePayload {
    pub table_id: String,
    pub index_id: String,
    pub action: IndexLifecycleAction,
    #[serde(default)]
    pub schema_epoch: u64,
    pub index: Option<DatabaseIndex>,
}

impl IndexLifecyclePayload {
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        <Self as TransactionPayloadCodec>::encode_payload(self)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        <Self as TransactionPayloadCodec>::decode_payload(payload)
    }
}
