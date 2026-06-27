
use super::*;

fn make_record(id: u64, kind: TransactionKind, actor: &UserId) -> TransactionRecord {
    TransactionRecord {
        id: TransactionId(id),
        groupid: None,
        refid: None,
        timestamp_epoch_ms: id,
        actor: actor.clone(),
        kind,
        payload: vec![id as u8],
    }
}

#[test]
fn compact_keeps_latest_schema_metadata_and_appends_truncate_marker() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");
    wal.append(
        "users",
        make_record(2, TransactionKind::SchemaChange, &actor),
    )
    .expect("append should succeed");
    wal.append("users", make_record(3, TransactionKind::Update, &actor))
        .expect("append should succeed");
    wal.append(
        "users",
        make_record(4, TransactionKind::SecurityChange, &actor),
    )
    .expect("append should succeed");
    wal.append("users", make_record(5, TransactionKind::Delete, &actor))
        .expect("append should succeed");

    wal.compact_stream_to_latest_schema_and_metadata("users", actor.clone(), 99)
        .expect("compact should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].kind, TransactionKind::SchemaChange);
    assert_eq!(records[0].id, TransactionId(2));
    assert_eq!(records[1].kind, TransactionKind::SecurityChange);
    assert_eq!(records[1].id, TransactionId(4));
    assert_eq!(records[2].kind, TransactionKind::Truncate);
    assert_eq!(records[2].id, TransactionId(6));
    assert_eq!(records[2].refid, None);
    assert_eq!(records[2].timestamp_epoch_ms, 99);
}

#[test]
fn compact_clears_refids_to_removed_records() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append(
        "users",
        make_record(1, TransactionKind::SchemaChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        make_record(2, TransactionKind::MetadataChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        TransactionRecord {
            id: TransactionId(3),
            groupid: None,
            refid: Some(TransactionId(1)),
            timestamp_epoch_ms: 3,
            actor: actor.clone(),
            kind: TransactionKind::SchemaChange,
            payload: vec![3],
        },
    )
    .expect("append should succeed");

    wal.compact_stream_to_latest_schema_and_metadata("users", actor, 100)
        .expect("compact should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].id, TransactionId(2));
    assert_eq!(records[0].refid, None);
    assert_eq!(records[1].id, TransactionId(3));
    assert_eq!(records[1].refid, None);
    assert_eq!(records[2].kind, TransactionKind::Truncate);
    assert_eq!(records[2].id, TransactionId(4));
    assert_eq!(records[2].refid, Some(TransactionId(3)));
}

#[test]
fn compact_prefers_latest_metadata_change_record_when_present() {
    let wal = ConcurrentWalManager::new();
    let actor = UserId::from_username("tester");

    wal.append(
        "users",
        make_record(1, TransactionKind::SchemaChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        make_record(2, TransactionKind::SecurityChange, &actor),
    )
    .expect("append should succeed");
    wal.append(
        "users",
        make_record(3, TransactionKind::MetadataChange, &actor),
    )
    .expect("append should succeed");

    wal.compact_stream_to_latest_schema_and_metadata("users", actor, 101)
        .expect("compact should succeed");

    let records = wal.since("users", None);
    assert_eq!(records.len(), 3);
    assert_eq!(records[1].kind, TransactionKind::MetadataChange);
}

#[test]
fn delete_stream_removes_in_memory_and_disk_state() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-wal-delete-stream-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp wal dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let actor = UserId::from_username("tester");
    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    let stream_key = super::obfuscated_stream_key("users").expect("stream key should resolve");
    let wal_file = temp_root.join(FileKind::Data.file_name(&stream_key));
    assert!(wal_file.exists());

    wal.delete_stream("users")
        .expect("delete stream should succeed");

    assert!(wal.since("users", None).is_empty());
    assert!(!wal_file.exists());

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn in_memory_mode_appends_without_filesystem_backing() {
    let wal = ConcurrentWalManager::in_memory();
    let actor = UserId::from_username("tester");

    wal.append("users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    assert!(wal.data_dir.is_none());

    let records = wal.since("users", None);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, TransactionKind::Insert);
}

#[test]
fn stream_mode_defaults_to_durable_and_can_be_set_ephemeral() {
    let wal = ConcurrentWalManager::new();

    assert_eq!(wal.stream_mode("users"), WalStreamMode::Durable);
    assert!(wal.is_stream_replicable("users"));

    wal.set_stream_mode("users", WalStreamMode::Ephemeral)
        .expect("setting stream mode should succeed");

    assert_eq!(wal.stream_mode("users"), WalStreamMode::Ephemeral);
    assert!(!wal.is_stream_replicable("users"));
}

#[test]
fn ephemeral_stream_in_file_mode_keeps_data_in_memory_only() {
    let temp_root = std::env::temp_dir().join(format!(
        "distdb-wal-ephemeral-stream-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp wal dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let actor = UserId::from_username("tester");

    wal.set_stream_mode("temp_users", WalStreamMode::Ephemeral)
        .expect("setting stream mode should succeed");

    wal.append("temp_users", make_record(1, TransactionKind::Insert, &actor))
        .expect("append should succeed");

    let stream_key = super::obfuscated_stream_key("temp_users")
        .expect("stream key should resolve");
    let wal_file = temp_root.join(FileKind::Data.file_name(&stream_key));

    assert!(!wal_file.exists());
    assert_eq!(wal.stream_mode("temp_users"), WalStreamMode::Ephemeral);
    assert!(!wal.is_stream_replicable("temp_users"));
    assert_eq!(wal.since("temp_users", None).len(), 1);

    let _ = std::fs::remove_dir_all(temp_root);
}
