use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::engine::database::entity::payload::EntityMetadataPayload;
use crate::engine::database::index_lifecycle_payload::IndexLifecyclePayload;
use crate::engine::database::schema::change_payload::SchemaChangePayload;
use crate::engine::database::sql_definition_payload::SqlDefinitionPayload;
use crate::engine::database::table::lifecycle_payload::TableLifecyclePayload;
use super::kind::TransactionKind;


pub trait TransactionPayloadCodec: Sized {

    const KIND: TransactionKind;

    fn encode_payload(&self) -> Result<Vec<u8>, &'static str>;

    fn decode_payload(payload: &[u8]) -> Result<Self, &'static str>;
    
}

pub trait SerdeTransactionPayload: Sized + Serialize + DeserializeOwned {
    const KIND: TransactionKind;
    const ENCODE_ERROR: &'static str;
    const DECODE_ERROR: &'static str;
}

pub trait SerdeTransactionPayloadCodec: SerdeTransactionPayload {

    fn encode_payload_serde(&self) -> Result<Vec<u8>, &'static str> {
        common::helpers::bincode_compat::serialize(self).map_err(|_| Self::ENCODE_ERROR)
    }

    fn decode_payload_serde(payload: &[u8]) -> Result<Self, &'static str> {
        common::helpers::bincode_compat::deserialize(payload).map_err(|_| Self::DECODE_ERROR)
    }
    
}

impl<T> TransactionPayloadCodec for T
where
    T: SerdeTransactionPayloadCodec,
{
    const KIND: TransactionKind = T::KIND;

    fn encode_payload(&self) -> Result<Vec<u8>, &'static str> {
        self.encode_payload_serde()
    }

    fn decode_payload(payload: &[u8]) -> Result<Self, &'static str> {
        Self::decode_payload_serde(payload)
    }
}

impl<T> SerdeTransactionPayloadCodec for T where T: SerdeTransactionPayload {}

impl SerdeTransactionPayload for SchemaChangePayload {
    const KIND: TransactionKind = TransactionKind::SchemaChange;
    const ENCODE_ERROR: &'static str = "failed to serialize schema change payload";
    const DECODE_ERROR: &'static str = "failed to deserialize schema change payload";
}

impl SerdeTransactionPayload for TableLifecyclePayload {
    const KIND: TransactionKind = TransactionKind::TableLifecycle;
    const ENCODE_ERROR: &'static str = "failed to serialize table lifecycle payload";
    const DECODE_ERROR: &'static str = "failed to deserialize table lifecycle payload";
}

impl SerdeTransactionPayload for IndexLifecyclePayload {
    const KIND: TransactionKind = TransactionKind::IndexLifecycle;
    const ENCODE_ERROR: &'static str = "failed to serialize index lifecycle payload";
    const DECODE_ERROR: &'static str = "failed to deserialize index lifecycle payload";
}

impl SerdeTransactionPayload for EntityMetadataPayload {
    const KIND: TransactionKind = TransactionKind::MetadataChange;
    const ENCODE_ERROR: &'static str = "failed to serialize entity metadata payload";
    const DECODE_ERROR: &'static str = "failed to deserialize entity metadata payload";
}

impl SerdeTransactionPayload for SqlDefinitionPayload {
    const KIND: TransactionKind = TransactionKind::SqlDefinitionChange;
    const ENCODE_ERROR: &'static str = "failed to serialize sql definition payload";
    const DECODE_ERROR: &'static str = "failed to deserialize sql definition payload";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedTransactionPayload {
    SchemaChange(SchemaChangePayload),
    TableLifecycle(TableLifecyclePayload),
    IndexLifecycle(IndexLifecyclePayload),
    EntityMetadata(EntityMetadataPayload),
    SqlDefinition(SqlDefinitionPayload),
}

impl DecodedTransactionPayload {

    pub fn decode(kind: TransactionKind, payload: &[u8]) -> Result<Self, &'static str> {

        match kind {
            
            TransactionKind::SchemaChange => SchemaChangePayload::decode_payload(payload)
                .map(Self::SchemaChange),

            TransactionKind::TableLifecycle => TableLifecyclePayload::decode_payload(payload)
                .map(Self::TableLifecycle),

            TransactionKind::IndexLifecycle => IndexLifecyclePayload::decode_payload(payload)
                .map(Self::IndexLifecycle),

            TransactionKind::MetadataChange | 
            TransactionKind::SecurityChange => EntityMetadataPayload::decode_payload(payload)
                .map(Self::EntityMetadata),

            TransactionKind::SqlDefinitionChange => SqlDefinitionPayload::decode_payload(payload)
                .map(Self::SqlDefinition),
            
            _ => Err("transaction kind does not define a structured payload codec"),

        }

    }

}

#[cfg(test)]
#[path = "payload_test.rs"]
mod tests;