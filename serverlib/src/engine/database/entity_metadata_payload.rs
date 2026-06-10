
use super::entity_metadata::EntityMetadata;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct EntityMetadataPayload {
    pub entity_id: String,
    pub metadata: EntityMetadata,
}

impl EntityMetadataPayload {

    pub fn encode(&self) -> Result<Vec<u8>, &'static str> {
        bincode::serialize(self).map_err(|_| "failed to serialize entity metadata payload")
    }

    pub fn decode(payload: &[u8]) -> Result<Self, &'static str> {
        bincode::deserialize(payload).map_err(|_| "failed to deserialize entity metadata payload")
    }

}
