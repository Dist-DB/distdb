use crate::engine::database::transaction::TransactionLog;
use crate::{
    ConcurrentWalManager, DatabaseCatalog, FieldDef, FieldIndex, FieldType,
    TableSchema, TransactionId, TransactionKind, TransactionRecord, UserId,
    WalStreamMode,
};

use super::{
    create_scoped_ephemeral_table, release_scoped_ephemeral_table,
    ScopedEphemeralTableScope,
};

fn users_schema() -> TableSchema {

    TableSchema::new(vec![
        
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
            seqno: 2,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    
    ])

}

#[test]
fn create_scoped_ephemeral_table_registers_table_and_marks_stream_ephemeral() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    let handle = create_scoped_ephemeral_table(
        &mut catalog,
        &wal,
        "tmp_users",
        users_schema(),
    )
    .expect("scoped table should be created");

    assert_eq!(handle.table_id(), "tmp_users");
    assert!(!handle.released());
    assert!(catalog.table("tmp_users").is_some());
    assert!(catalog.table("tmp_users").is_some_and(|table| table.is_temporary()));
    let stream_id = catalog
        .entity_wal_stream_id("tmp_users")
        .expect("temporary table stream id should exist");
    assert_eq!(wal.stream_mode(&stream_id), WalStreamMode::Ephemeral);
    assert!(!wal.is_stream_replicable(&stream_id));
}

#[test]
fn release_scoped_ephemeral_table_clears_wal_and_drops_temporary_table() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    let mut handle = create_scoped_ephemeral_table(
        &mut catalog,
        &wal,
        "tmp_users",
        users_schema(),
    )
    .expect("scoped table should be created");

    let stream_id = catalog
        .entity_wal_stream_id("tmp_users")
        .expect("temporary table stream id should exist");

    wal.append(
        &stream_id,
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            UserId::from_username("tester"),
            TransactionKind::Insert,
            vec![1],
        ),
    )
    .expect("append should succeed");

    assert_eq!(wal.since(&stream_id, None).len(), 1);

    release_scoped_ephemeral_table(&mut catalog, &wal, &mut handle)
        .expect("scoped table should release");

    assert!(handle.released());
    assert!(catalog.table("tmp_users").is_none());
    assert!(wal.since(&stream_id, None).is_empty());
}

#[test]
fn release_scoped_ephemeral_table_is_idempotent() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    let mut handle = create_scoped_ephemeral_table(
        &mut catalog,
        &wal,
        "tmp_users",
        users_schema(),
    )
    .expect("scoped table should be created");

    release_scoped_ephemeral_table(&mut catalog, &wal, &mut handle)
        .expect("first release should succeed");
    release_scoped_ephemeral_table(&mut catalog, &wal, &mut handle)
        .expect("second release should succeed");

    assert!(handle.released());
}

#[test]
fn temporary_table_recreate_reuses_stream_identity_after_release() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    let mut handle = create_scoped_ephemeral_table(
        &mut catalog,
        &wal,
        "tmp_users",
        users_schema(),
    )
    .expect("scoped table should be created");

    let first_stream_id = catalog
        .entity_wal_stream_id("tmp_users")
        .expect("temporary table stream id should exist");

    wal.append(
        &first_stream_id,
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            UserId::from_username("tester"),
            TransactionKind::Insert,
            vec![1],
        ),
    )
    .expect("append should succeed");

    assert_eq!(wal.active_worker_count(), 1);

    release_scoped_ephemeral_table(&mut catalog, &wal, &mut handle)
        .expect("scoped table should release");

    let _ = create_scoped_ephemeral_table(
        &mut catalog,
        &wal,
        "tmp_users",
        users_schema(),
    )
    .expect("scoped table should recreate");

    let second_stream_id = catalog
        .entity_wal_stream_id("tmp_users")
        .expect("temporary table stream id should exist after recreate");

    assert_eq!(first_stream_id, second_stream_id);

    wal.append(
        &second_stream_id,
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            2,
            UserId::from_username("tester"),
            TransactionKind::Insert,
            vec![2],
        ),
    )
    .expect("append after recreate should succeed");

    assert_eq!(wal.since(&second_stream_id, None).len(), 1);

    assert_eq!(wal.active_worker_count(), 1);
}

#[test]
fn scoped_ephemeral_table_scope_generates_unique_table_ids() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    let mut scope_a = ScopedEphemeralTableScope::new("proc_sessiona");
    let mut scope_b = ScopedEphemeralTableScope::new("proc_sessionb");

    let table_a = scope_a
        .create_table(&mut catalog, &wal, "tmp_users", users_schema())
        .expect("scope a table should be created");
    let table_b = scope_b
        .create_table(&mut catalog, &wal, "tmp_users", users_schema())
        .expect("scope b table should be created");

    assert_ne!(table_a, table_b);
    assert!(catalog.table(&table_a).is_some());
    assert!(catalog.table(&table_b).is_some());
}

#[test]
fn scoped_ephemeral_table_scope_cleanup_is_isolated_per_scope() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    let mut scope_a = ScopedEphemeralTableScope::new("proc_sessiona");
    let mut scope_b = ScopedEphemeralTableScope::new("proc_sessionb");

    let table_a = scope_a
        .create_table(&mut catalog, &wal, "tmp_users", users_schema())
        .expect("scope a table should be created");
    let table_b = scope_b
        .create_table(&mut catalog, &wal, "tmp_users", users_schema())
        .expect("scope b table should be created");

    let stream_a = catalog
        .entity_wal_stream_id(&table_a)
        .expect("scope a stream id should resolve");
    let stream_b = catalog
        .entity_wal_stream_id(&table_b)
        .expect("scope b stream id should resolve");

    wal.append(
        &stream_a,
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            UserId::from_username("tester"),
            TransactionKind::Insert,
            vec![1],
        ),
    )
    .expect("scope a append should succeed");

    wal.append(
        &stream_b,
        TransactionRecord::with_payload(
            TransactionId(2),
            None,
            None,
            1,
            UserId::from_username("tester"),
            TransactionKind::Insert,
            vec![2],
        ),
    )
    .expect("scope b append should succeed");

    scope_a
        .cleanup(&mut catalog, &wal)
        .expect("scope a cleanup should succeed");

    assert!(catalog.table(&table_a).is_some_and(|table| table.is_temporary()));
    assert!(catalog.table(&table_b).is_some());
    assert!(wal.since(&stream_a, None).is_empty());
    assert_eq!(wal.since(&stream_b, None).len(), 1);

    scope_b
        .cleanup(&mut catalog, &wal)
        .expect("scope b cleanup should succeed");
    assert!(catalog.table(&table_b).is_some_and(|table| table.is_temporary()));
    assert!(wal.since(&stream_b, None).is_empty());
}
