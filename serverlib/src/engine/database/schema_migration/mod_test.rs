use super::*;
use crate::core::identity::UserId;
use crate::engine::database::core::ObjectStatus;
use crate::engine::database::table_schema::TableSchema;
use crate::engine::database::transaction::{TransactionId, TransactionKind, TransactionRecord};
use common::helpers::format::FileKind;
use common::helpers::write_bytes;
use std::collections::HashMap;

#[derive(Default)]
struct SpyExecutor {
    calls: std::sync::Mutex<Vec<&'static str>>,
}

impl SpyExecutor {
    fn calls(&self) -> Vec<&'static str> {
        self.calls.lock().expect("mutex should lock").clone()
    }
}

impl SchemaMigrationExecutor for SpyExecutor {
    fn rewrite_rows(
        &self,
        _catalog: &DatabaseCatalog,
        _table_id: &str,
    ) -> DatabaseResult<SchemaMigrationProgress> {
        self.calls
            .lock()
            .expect("mutex should lock")
            .push("rewrite");
        Ok(SchemaMigrationProgress {
            rows_rewritten: 10,
            rows_total: Some(20),
            resume_token: Some("pk:users:10".to_string()),
        })
    }

    fn rebuild_indexes(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
        self.calls
            .lock()
            .expect("mutex should lock")
            .push("reindex");
        Ok(())
    }

    fn flush_temp_image(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
        self.calls.lock().expect("mutex should lock").push("flush");
        Ok(())
    }

    fn cutover(&self, _catalog: &DatabaseCatalog, _table_id: &str) -> DatabaseResult<()> {
        self.calls
            .lock()
            .expect("mutex should lock")
            .push("cutover");
        Ok(())
    }
}

#[test]
fn run_schema_migration_updates_progress_and_order() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("schema change should begin");

    let executor = SpyExecutor::default();
    run_schema_migration(&mut catalog, "users", &executor).expect("migration should succeed");

    let calls = executor.calls();
    assert_eq!(calls, vec!["rewrite", "reindex", "flush", "cutover"]);

    let active = catalog
        .active_schema_change()
        .expect("active schema change should exist");
    assert_eq!(active.phase, SchemaChangePhase::Cutover);
    assert_eq!(active.rows_rewritten, 10);
    assert_eq!(active.rows_total, Some(20));
    assert_eq!(active.resume_token.as_deref(), Some("pk:users:10"));
}

#[test]
fn run_schema_migration_requires_active_lock() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");

    let err = run_schema_migration(&mut catalog, "users", &NoopSchemaMigrationExecutor)
        .expect_err("migration should fail when table is not locked");

    assert_eq!(err, DatabaseError::TableNotLocked);
}

#[test]
fn disk_executor_rewrites_flushes_and_cuts_over() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("schema change should begin");

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-schema-migration-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let stream_key = stream_key_for_table("users").expect("stream key should resolve");
    let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));

    let actor = UserId::from_username("migrator");
    let seed_records = vec![
        TransactionRecord {
            id: TransactionId(1),
            groupid: None,
            refid: None,
            timestamp_epoch_ms: 1,
            actor: actor.clone(),
            kind: TransactionKind::Insert,
            payload: vec![1],
        },
        TransactionRecord {
            id: TransactionId(2),
            groupid: None,
            refid: Some(TransactionId(1)),
            timestamp_epoch_ms: 2,
            actor: actor.clone(),
            kind: TransactionKind::Delete,
            payload: vec![2],
        },
        TransactionRecord {
            id: TransactionId(3),
            groupid: None,
            refid: Some(TransactionId(2)),
            timestamp_epoch_ms: 3,
            actor,
            kind: TransactionKind::Update,
            payload: vec![3],
        },
    ];

    let wal_file = frame_records_as_wal_file(&seed_records).expect("wal file should frame");
    write_bytes(&wal_path, &wal_file).expect("seed wal should write");

    let executor = DiskToMemorySchemaMigrationExecutor::new(temp_root.clone());

    run_schema_migration(&mut catalog, "users", &executor).expect("migration should succeed");

    let rewritten = load_records_from_path(&wal_path).expect("rewritten wal should load");
    assert_eq!(rewritten.len(), 2);
    assert!(rewritten
        .iter()
        .all(|record| record.kind != TransactionKind::Delete));

    let active = catalog
        .active_schema_change()
        .expect("active schema change should exist");
    assert_eq!(active.phase, SchemaChangePhase::Cutover);

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn disk_executor_applies_schema_mutation_rules_to_row_payloads() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("schema change should begin");

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-schema-rules-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let stream_key = stream_key_for_table("users").expect("stream key should resolve");
    let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));

    let actor = UserId::from_username("migrator");
    let mut row = HashMap::new();
    row.insert("first_name".to_string(), b"sam".to_vec());
    row.insert("legacy".to_string(), b"drop".to_vec());

    let seed_records = vec![TransactionRecord {
        id: TransactionId(1),
        groupid: None,
        refid: None,
        timestamp_epoch_ms: 1,
        actor,
        kind: TransactionKind::Insert,
        payload: bincode::serialize(&row).expect("row should encode"),
    }];

    let wal_file = frame_records_as_wal_file(&seed_records).expect("wal file should frame");
    write_bytes(&wal_path, &wal_file).expect("seed wal should write");

    let executor = DiskToMemorySchemaMigrationExecutor::new(temp_root.clone());
    executor
        .set_rules_for_table(
            "users",
            SchemaMutationRuleSet {
                renames: vec![("first_name".to_string(), "given_name".to_string())],
                removals: vec!["legacy".to_string()],
                additions: vec![("status".to_string(), b"active".to_vec())],
                type_changes: Vec::new(),
                conversion_policy: TypeConversionPolicy::Safe,
            },
        )
        .expect("rules should be set");

    run_schema_migration(&mut catalog, "users", &executor).expect("migration should succeed");

    let rewritten = load_records_from_path(&wal_path).expect("rewritten wal should load");
    assert_eq!(rewritten.len(), 1);

    let out_row: HashMap<String, Vec<u8>> =
        bincode::deserialize(&rewritten[0].payload).expect("payload should decode");

    assert_eq!(out_row.get("given_name"), Some(&b"sam".to_vec()));
    assert_eq!(out_row.get("status"), Some(&b"active".to_vec()));
    assert!(!out_row.contains_key("first_name"));
    assert!(!out_row.contains_key("legacy"));

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn disk_executor_safe_type_change_rejects_invalid_value() {
    use crate::engine::database::table_schema::FieldType;

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("schema change should begin");

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-schema-type-safe-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let stream_key = stream_key_for_table("users").expect("stream key should resolve");
    let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));

    let actor = UserId::from_username("migrator");
    let mut row = HashMap::new();
    row.insert("age".to_string(), b"not-a-number".to_vec());

    let seed_records = vec![TransactionRecord {
        id: TransactionId(1),
        groupid: None,
        refid: None,
        timestamp_epoch_ms: 1,
        actor,
        kind: TransactionKind::Insert,
        payload: bincode::serialize(&row).expect("row should encode"),
    }];

    let wal_file = frame_records_as_wal_file(&seed_records).expect("wal file should frame");
    write_bytes(&wal_path, &wal_file).expect("seed wal should write");

    let executor = DiskToMemorySchemaMigrationExecutor::new(temp_root.clone());
    executor
        .set_rules_for_table(
            "users",
            SchemaMutationRuleSet {
                renames: Vec::new(),
                removals: Vec::new(),
                additions: Vec::new(),
                type_changes: vec![FieldTypeChangeRule {
                    field_name: "age".to_string(),
                    target_type: FieldType::UInt(32),
                }],
                conversion_policy: TypeConversionPolicy::Safe,
            },
        )
        .expect("rules should be set");

    let result = run_schema_migration(&mut catalog, "users", &executor);
    assert!(result.is_err());

    let _ = std::fs::remove_dir_all(temp_root);
}

#[test]
fn disk_executor_force_type_change_coerces_invalid_value() {
    use crate::engine::database::table_schema::FieldType;

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    catalog
        .create_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");
    catalog
        .transition_status(ObjectStatus::Sync)
        .expect("load->sync");
    catalog
        .transition_status(ObjectStatus::Ready)
        .expect("sync->ready");

    let _tx = catalog
        .begin_schema_change("users")
        .expect("schema change should begin");

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-schema-type-force-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let stream_key = stream_key_for_table("users").expect("stream key should resolve");
    let wal_path = temp_root.join(FileKind::Data.file_name(&stream_key));

    let actor = UserId::from_username("migrator");
    let mut row = HashMap::new();
    row.insert("age".to_string(), b"not-a-number".to_vec());

    let seed_records = vec![TransactionRecord {
        id: TransactionId(1),
        groupid: None,
        refid: None,
        timestamp_epoch_ms: 1,
        actor,
        kind: TransactionKind::Insert,
        payload: bincode::serialize(&row).expect("row should encode"),
    }];

    let wal_file = frame_records_as_wal_file(&seed_records).expect("wal file should frame");
    write_bytes(&wal_path, &wal_file).expect("seed wal should write");

    let executor = DiskToMemorySchemaMigrationExecutor::new(temp_root.clone());
    executor
        .set_rules_for_table(
            "users",
            SchemaMutationRuleSet {
                renames: Vec::new(),
                removals: Vec::new(),
                additions: Vec::new(),
                type_changes: vec![FieldTypeChangeRule {
                    field_name: "age".to_string(),
                    target_type: FieldType::UInt(32),
                }],
                conversion_policy: TypeConversionPolicy::Force,
            },
        )
        .expect("rules should be set");

    run_schema_migration(&mut catalog, "users", &executor)
        .expect("migration should succeed under force policy");

    let rewritten = load_records_from_path(&wal_path).expect("rewritten wal should load");
    let out_row: HashMap<String, Vec<u8>> =
        bincode::deserialize(&rewritten[0].payload).expect("payload should decode");
    assert_eq!(out_row.get("age"), Some(&b"0".to_vec()));

    let _ = std::fs::remove_dir_all(temp_root);
}
