use super::*;
use common::helpers::write_bytes;
use crate::engine::database::catalog::DatabaseCatalog;
use crate::engine::database::transaction::{TransactionId, TransactionKind, TransactionRecord};
use crate::core::identity::UserId;

#[test]
fn stream_key_for_table_normalizes_identifier() {
    let key = stream_key_for_table("USERS").expect("should get key");
    let key2 = stream_key_for_table("users").expect("should get key");
    assert_eq!(key, key2);
}

#[test]
fn stream_key_for_empty_table_fails() {
    let result = stream_key_for_table("");
    assert!(result.is_err());
}

#[test]
fn load_records_from_nonexistent_path_returns_empty() {
    let result = load_records_from_path(Path::new("/nonexistent/path")).expect("should load empty");
    assert_eq!(result.len(), 0);
}

#[test]
fn payload_context_for_table_includes_catalog_encryption_metadata() {
    let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb")
        .expect("catalog should be created");
    catalog
        .configure_at_rest_encryption_key_ref("enc:node-main:db-main")
        .expect("encryption ref should configure");

    let context = payload_context_for_table(&catalog, "users").expect("context should build");

    assert_eq!(context.database_id(), Some(catalog.database_id.0.as_str()));
    assert_eq!(context.table_id(), Some("users"));
    assert!(context.stream_id().is_some());
    assert_eq!(
        context.at_rest_encryption_key_ref(),
        Some("enc:node-main:db-main")
    );
    assert_eq!(context.at_rest_encryption_key_version(), Some(1));
}

#[test]
fn load_records_from_path_with_context_rejects_encrypted_payload_without_provider() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-schema-io-encrypted-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should exist");

    let mut catalog = DatabaseCatalog::create_empty_from_name("MainDb")
        .expect("catalog should be created");
    catalog
        .configure_at_rest_encryption_key_ref("enc:node-main:db-main")
        .expect("encryption ref should configure");

    let stream_key = stream_key_for_table("users").expect("stream key should build");
    let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));
    let actor = UserId::from_username("tester");
    let encrypted_payload = crate::engine::database::row_payload::
        encode_encrypted_row_payload_envelope(
            1,
            vec![7; 12],
            vec![9; 16],
            b"ciphertext".to_vec(),
        )
        .expect("encrypted payload should encode");
    let records = vec![TransactionRecord::with_payload(
        TransactionId(1),
        None,
        None,
        1,
        actor,
        TransactionKind::Insert,
        encrypted_payload,
    )];
    let file_bytes = frame_records_as_wal_file(&records).expect("wal file should frame");
    write_bytes(&wal_path, &file_bytes).expect("wal file should write");

    let context = payload_context_for_table(&catalog, "users").expect("context should build");
    let result = load_records_from_path_with_context(&wal_path, &context);

    assert!(matches!(result, Err(DatabaseError::CatalogDeserialize)));

    let _ = std::fs::remove_dir_all(temp_root);
}
