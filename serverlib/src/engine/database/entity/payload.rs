
use crate::engine::database::entity::metadata::EntityMetadata;
use crate::engine::database::transaction::payload::SerdeTransactionPayloadCodec;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntityMetadataPayload {
    pub entity_id: String,
    pub metadata: EntityMetadata,
}

impl EntityMetadataPayload {

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        self.encode_payload_serde()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        Self::decode_payload_serde(payload)
    }

}

