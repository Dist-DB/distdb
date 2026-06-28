use super::*;

use crate::engine::database::table_schema::{FieldDef, FieldIndex, FieldType};
use crate::render_stored_field_value;

fn test_schema() -> TableSchema {
    TableSchema::new(vec![
        FieldDef {
            seqno: 2,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 1,
            field_name: "id".to_string(),
            field_type: FieldType::UInt(64),
            nullable: false,
            indexed: FieldIndex::PrimaryKey,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 3,
            field_name: "nickname".to_string(),
            field_type: FieldType::Text,
            nullable: true,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ])
}

#[test]
fn encode_uses_seqno_ordinal_and_null_slots() {
    let schema = test_schema();
    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());
    row.insert("email".to_string(), b"sam@example.com".to_vec());

    let encoded = encode_row_payload(&schema, &row).expect("row should encode");
    let decoded: Vec<Option<Vec<u8>>> =
        bincode::deserialize(&encoded).expect("ordinal row should decode");

    assert_eq!(decoded.len(), 3);
    assert_ne!(decoded[0], Some(b"1".to_vec()));
    assert_eq!(decoded[0].as_deref().map(render_stored_field_value), Some(b"1".to_vec()));
    assert_eq!(decoded[1], Some(b"sam@example.com".to_vec()));
    assert_eq!(decoded[2], None);
}

#[test]
fn decode_round_trips_ordinal_with_nulls() {
    let schema = test_schema();
    let payload = vec![Some(b"1".to_vec()), Some(b"sam@example.com".to_vec()), None];

    let encoded = bincode::serialize(&payload).expect("payload should encode");
    let row = decode_row_payload(&schema, &encoded).expect("row should decode");

    assert_eq!(row.get("id"), Some(&b"1".to_vec()));
    assert_eq!(row.get("email"), Some(&b"sam@example.com".to_vec()));
    assert!(!row.contains_key("nickname"));
}

#[test]
fn decode_accepts_legacy_name_map() {
    let schema = test_schema();
    let mut legacy = HashMap::new();
    legacy.insert("id".to_string(), b"2".to_vec());
    legacy.insert("email".to_string(), b"legacy@example.com".to_vec());

    let encoded = bincode::serialize(&legacy).expect("legacy row should encode");
    let row = decode_row_payload(&schema, &encoded).expect("legacy row should decode");

    assert_eq!(row.get("id").cloned(), Some(b"2".to_vec()));
    assert_eq!(
        row.get("email").cloned(),
        Some(b"legacy@example.com".to_vec())
    );
}

#[test]
fn encrypted_row_payload_envelope_roundtrip() {
    let encoded = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        vec![9, 8, 7],
    )
    .expect("envelope should encode");

    let decoded = decode_encrypted_row_payload_envelope(&encoded)
        .expect("envelope decode should succeed")
        .expect("envelope should be detected");

    assert_eq!(decoded.key_version, 1);
    assert_eq!(decoded.nonce, vec![1; 12]);
    assert_eq!(decoded.auth_tag, vec![5; 16]);
    assert_eq!(decoded.ciphertext, vec![9, 8, 7]);
}

#[test]
fn decode_rejects_encrypted_payload_without_decryption() {
    let schema = test_schema();
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        vec![9, 8, 7],
    )
    .expect("envelope should encode");

    let err = decode_row_payload(&schema, &payload).expect_err("decode should reject encrypted payload");
    assert!(err.contains("encrypted at rest"));
}
