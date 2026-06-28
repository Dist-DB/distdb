
use super::entity_metadata::EntityMetadata;
use super::transaction::transaction_payload::TransactionPayloadCodec;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntityMetadataPayload {
    pub entity_id: String,
    pub metadata: EntityMetadata,
}

impl EntityMetadataPayload {

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        <Self as TransactionPayloadCodec>::encode_payload(self)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        <Self as TransactionPayloadCodec>::decode_payload(payload)
    }

}
