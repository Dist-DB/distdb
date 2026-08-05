use crate::engine::database::databaseindex::DatabaseIndex;
use crate::engine::database::transaction::payload::SerdeTransactionPayloadCodec;

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
        self.encode_payload_serde()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        Self::decode_payload_serde(payload)
    }

}


