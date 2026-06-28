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

#[test]
fn encrypted_row_payload_transform_can_preserve_opaque_payloads() {
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        vec![9, 8, 7],
    )
    .expect("envelope should encode");

    let transformed = EncryptedRowPayloadTransform::preserve_opaque()
        .transform_payload(&payload, &crate::TransactionPayloadContext::default())
        .expect("opaque preserve should succeed")
        .expect("encrypted payload should be detected");

    assert_eq!(transformed, payload);
}

#[test]
fn encrypted_row_payload_transform_can_reject_encrypted_payloads() {
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        vec![9, 8, 7],
    )
    .expect("envelope should encode");

    let err = EncryptedRowPayloadTransform::reject_encrypted()
        .transform_payload(&payload, &crate::TransactionPayloadContext::default())
        .expect_err("reject policy should fail");

    assert_eq!(err, PayloadTransformError::DecryptFailed);
}

#[test]
fn encrypted_row_payload_transform_ignores_plaintext_payloads() {
    let result = EncryptedRowPayloadTransform::preserve_opaque()
        .transform_payload(
            b"plain-text-payload",
            &crate::TransactionPayloadContext::default(),
        )
        .expect("plaintext should pass");

    assert_eq!(result, None);
}

#[test]
fn encrypted_row_payload_write_transform_can_preserve_opaque_payloads() {
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        vec![9, 8, 7],
    )
    .expect("envelope should encode");
    let record = crate::TransactionRecord::with_payload(
        crate::TransactionId(1),
        None,
        None,
        1,
        crate::UserId("system".to_string()),
        crate::TransactionKind::Insert,
        payload.clone(),
    );

    let transformed = EncryptedRowPayloadTransform::preserve_opaque()
        .transform_payload_for_write(
            &record,
            &payload,
            &crate::TransactionPayloadContext::default(),
        )
        .expect("opaque preserve should succeed")
        .expect("encrypted payload should be detected");

    assert_eq!(transformed, payload);
}

#[test]
fn encrypted_row_payload_write_transform_can_reject_encrypted_payloads() {
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        vec![9, 8, 7],
    )
    .expect("envelope should encode");
    let record = crate::TransactionRecord::with_payload(
        crate::TransactionId(1),
        None,
        None,
        1,
        crate::UserId("system".to_string()),
        crate::TransactionKind::Insert,
        payload.clone(),
    );

    let err = EncryptedRowPayloadTransform::reject_encrypted()
        .transform_payload_for_write(
            &record,
            &payload,
            &crate::TransactionPayloadContext::default(),
        )
        .expect_err("reject policy should fail");

    assert_eq!(err, PayloadTransformError::InvalidEncryptedPayload);
}

#[test]
fn row_payload_encryption_write_transform_ignores_plaintext_without_encryption_context() {
    let record = crate::TransactionRecord::with_payload(
        crate::TransactionId(1),
        None,
        None,
        1,
        crate::UserId("system".to_string()),
        crate::TransactionKind::Insert,
        b"plain-text-payload".to_vec(),
    );

    let result = RowPayloadEncryptionWriteTransform::new(UnconfiguredRowPayloadEncryptionProvider)
        .transform_payload_for_write(
            &record,
            b"plain-text-payload",
            &crate::TransactionPayloadContext::default(),
        )
        .expect("plaintext without encryption context should pass through");

    assert_eq!(result, None);
}

#[test]
fn row_payload_encryption_write_transform_reports_unconfigured_provider_when_context_requires_encryption() {
    let record = crate::TransactionRecord::with_payload(
        crate::TransactionId(1),
        None,
        None,
        1,
        crate::UserId("system".to_string()),
        crate::TransactionKind::Insert,
        b"plain-text-payload".to_vec(),
    );
    let context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-main", 1);

    let err = RowPayloadEncryptionWriteTransform::new(UnconfiguredRowPayloadEncryptionProvider)
        .transform_payload_for_write(&record, b"plain-text-payload", &context)
        .expect_err("configured encryption context without provider should fail");

    assert_eq!(err, PayloadTransformError::EncryptionNotConfigured);
}

#[test]
fn row_payload_encryption_write_transform_uses_provider_when_encryption_context_is_present() {
    struct StubEncryptionProvider;

    impl RowPayloadEncryptionProvider for StubEncryptionProvider {
        fn encrypt_row_payload(
            &self,
            context: &crate::TransactionPayloadContext,
            plaintext: &[u8],
        ) -> Result<Vec<u8>, PayloadTransformError> {
            encode_encrypted_row_payload_envelope(
                context.at_rest_encryption_key_version().unwrap_or(0),
                vec![1; 12],
                vec![2; 16],
                plaintext.to_vec(),
            )
            .map_err(PayloadTransformError::InternalTransformError)
        }
    }

    let record = crate::TransactionRecord::with_payload(
        crate::TransactionId(1),
        None,
        None,
        1,
        crate::UserId("system".to_string()),
        crate::TransactionKind::Insert,
        b"plain-text-payload".to_vec(),
    );
    let context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-main", 7);

    let encrypted = RowPayloadEncryptionWriteTransform::new(StubEncryptionProvider)
        .transform_payload_for_write(&record, b"plain-text-payload", &context)
        .expect("provider-backed encryption should succeed")
        .expect("provider should encrypt payload");

    let decoded = decode_encrypted_row_payload_envelope(&encrypted)
        .expect("encrypted envelope should decode")
        .expect("encrypted payload should be detected");

    assert_eq!(decoded.key_version, 7);
    assert_eq!(decoded.ciphertext, b"plain-text-payload".to_vec());
}

#[test]
fn row_payload_decryption_transform_ignores_encrypted_payload_without_encryption_context() {
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        b"ciphertext".to_vec(),
    )
    .expect("envelope should encode");

    let result = RowPayloadDecryptionTransform::new(UnconfiguredRowPayloadDecryptionProvider)
        .transform_payload(&payload, &crate::TransactionPayloadContext::default())
        .expect("encrypted payload without context should pass through");

    assert_eq!(result, None);
}

#[test]
fn row_payload_decryption_transform_reports_unconfigured_provider_when_context_requires_encryption() {
    let payload = encode_encrypted_row_payload_envelope(
        1,
        vec![1; 12],
        vec![5; 16],
        b"ciphertext".to_vec(),
    )
    .expect("envelope should encode");
    let context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-main", 1);

    let err = RowPayloadDecryptionTransform::new(UnconfiguredRowPayloadDecryptionProvider)
        .transform_payload(&payload, &context)
        .expect_err("configured encryption context without provider should fail");

    assert_eq!(err, PayloadTransformError::EncryptionNotConfigured);
}

#[test]
fn row_payload_decryption_transform_uses_provider_when_encryption_context_is_present() {
    struct StubDecryptionProvider;

    impl RowPayloadDecryptionProvider for StubDecryptionProvider {
        fn decrypt_row_payload(
            &self,
            _context: &crate::TransactionPayloadContext,
            envelope: &EncryptedRowPayloadEnvelope,
        ) -> Result<Vec<u8>, PayloadTransformError> {
            Ok(envelope.ciphertext.clone())
        }
    }

    let payload = encode_encrypted_row_payload_envelope(
        7,
        vec![1; 12],
        vec![5; 16],
        b"plain-text-payload".to_vec(),
    )
    .expect("envelope should encode");
    let context = crate::TransactionPayloadContext::new()
        .with_database_id("main")
        .with_table_id("users")
        .with_at_rest_encryption("enc:node-main:db-main", 7);

    let plaintext = RowPayloadDecryptionTransform::new(StubDecryptionProvider)
        .transform_payload(&payload, &context)
        .expect("provider-backed decryption should succeed")
        .expect("provider should decrypt payload");

    assert_eq!(plaintext, b"plain-text-payload".to_vec());
}
