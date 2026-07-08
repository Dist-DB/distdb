use super::*;
use crate::{engine::database::entity::metadata::EntityMetadata, FieldDef, FieldIndex, FieldType, TableSchema};

#[test]
fn schema_change_payload_round_trips_through_common_codec() {

    let payload = SchemaChangePayload {
        table_id: "users".to_string(),
        schema_revision: 1,
        schema_epoch: 1,
        entity_id: None,
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
