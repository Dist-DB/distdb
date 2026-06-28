
use super::transaction_payload::TransactionPayloadCodec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlObjectKind {
    View,
    Trigger,
    StoredProcedure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlDefinitionAction {
    Upsert,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SqlDefinitionPayload {
    pub object_id: String,
    pub object_kind: SqlObjectKind,
    pub action: SqlDefinitionAction,
    #[serde(default)]
    pub schema_epoch: u64,
    pub sql: String,
    pub dependencies: Vec<String>,
}

impl SqlDefinitionPayload {
    
    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        <Self as TransactionPayloadCodec>::encode_payload(self)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        <Self as TransactionPayloadCodec>::decode_payload(payload)
    }

}
