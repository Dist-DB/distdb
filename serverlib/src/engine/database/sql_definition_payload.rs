
use super::transaction::payload::SerdeTransactionPayloadCodec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SqlObjectKind {
    View,
    OlapView,
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
        self.encode_payload_serde()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        Self::decode_payload_serde(payload)
    }

}

