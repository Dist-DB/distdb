use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::engine::database::entity::payload::EntityMetadataPayload;
use crate::engine::database::schema::change_payload::SchemaChangePayload;
use crate::engine::database::sql_definition_payload::SqlDefinitionPayload;
use crate::engine::database::table::lifecycle_payload::TableLifecyclePayload;
use super::kind::TransactionKind;


pub trait TransactionPayloadCodec: Sized {

    const KIND: TransactionKind;

    fn encode_payload(&self) -> Result<Vec<u8>, &'static str>;

    fn decode_payload(payload: &[u8]) -> Result<Self, &'static str>;
    
}

trait SerdeTransactionPayload: Sized + Serialize + DeserializeOwned {
    const KIND: TransactionKind;
    const ENCODE_ERROR: &'static str;
    const DECODE_ERROR: &'static str;
}

impl<T> TransactionPayloadCodec for T
where
    T: SerdeTransactionPayload,
{
    const KIND: TransactionKind = T::KIND;

    fn encode_payload(&self) -> Result<Vec<u8>, &'static str> {
        bincode::serialize(self).map_err(|_| T::ENCODE_ERROR)
    }

    fn decode_payload(payload: &[u8]) -> Result<Self, &'static str> {
        bincode::deserialize(payload).map_err(|_| T::DECODE_ERROR)
    }
}

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
            TransactionKind::MetadataChange | TransactionKind::SecurityChange => {
                EntityMetadataPayload::decode_payload(payload).map(Self::EntityMetadata)
            }
            TransactionKind::SqlDefinitionChange => SqlDefinitionPayload::decode_payload(payload)
                .map(Self::SqlDefinition),
            _ => Err("transaction kind does not define a structured payload codec"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{engine::database::entity::metadata::EntityMetadata, FieldDef, FieldIndex, FieldType, TableSchema};

    #[test]
    fn schema_change_payload_round_trips_through_common_codec() {
        let payload = SchemaChangePayload {
            table_id: "users".to_string(),
            schema_revision: 1,
            schema_epoch: 1,
            schema: TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::UInt(64),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        };

        let encoded = payload.encode_payload().expect("payload should encode");
        let decoded = SchemaChangePayload::decode_payload(&encoded).expect("payload should decode");

        assert_eq!(decoded, payload);
    }

    #[test]
    fn kind_dispatch_decodes_entity_metadata_payload() {
        let payload = EntityMetadataPayload {
            entity_id: "users".to_string(),
            metadata: EntityMetadata::default(),
        };

        let encoded = payload.encode_payload().expect("payload should encode");
        let decoded = DecodedTransactionPayload::decode(TransactionKind::MetadataChange, &encoded)
            .expect("dispatch should decode metadata payload");

        assert_eq!(decoded, DecodedTransactionPayload::EntityMetadata(payload));
    }
}