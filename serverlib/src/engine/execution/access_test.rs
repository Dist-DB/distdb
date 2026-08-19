use std::sync::Arc;
use std::path::PathBuf;
use std::fs;
use std::time::{Duration, Instant};


use super::*;
use crate::engine::database::indexing::runtime_index_snapshot::RuntimeIndexSnapshotService;
use crate::engine::database::transaction::TransactionLog;
use crate::{
    DatabaseIndex, DatabaseIndexKind, DatabaseIndexOrigin,
    encode_row_payload, ConcurrentWalManager, DatabaseCatalog, FieldDef, FieldIndex, FieldType,
    RuntimeIndexStore, SelectComparisonOp, SelectCondition, SelectPredicate, TableSchema,
    TransactionId, TransactionKind, TransactionRecord, UserId,
};

fn table_schema(fields: Vec<(&str, u32, FieldType, FieldIndex, bool)>) -> TableSchema {

    TableSchema::new(
        fields
            .into_iter()
            .map(
                |(field_name, seqno, field_type, indexed, nullable)| FieldDef {
                    seqno,
                    field_name: field_name.to_string(),
                    field_type,
                    nullable,
                    indexed,
                    default_value: None,
                    metadata: None,
                },
            )
            .collect(),
    )

}

fn unique_temp_dir(prefix: &str) -> PathBuf {

    let now_nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);

    std::env::temp_dir().join(format!(
        "distdb-{}-{}-{}",
        prefix,
        std::process::id(),
        now_nanos,
    ))

}

fn seed_users_table(catalog: &mut DatabaseCatalog, wal: &ConcurrentWalManager) -> TableSchema {

    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::Indexed, false),
        ("nickname", 3, FieldType::Text, FieldIndex::None, true),
    ]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");

    let actor = UserId("test-user".to_string());

    for (id, email, nickname, tx_id) in [
        (
            b"1".as_slice(),
            b"sam@example.com".as_slice(),
            Some(b"sam".as_slice()),
            1,
        ),
        (b"2".as_slice(), b"alex@example.com".as_slice(), None, 2),
    ] {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), id.to_vec());
        row.insert("email".to_string(), email.to_vec());
        if let Some(value) = nickname {
            row.insert("nickname".to_string(), value.to_vec());
        }

        wal.append(
            "users",
            TransactionRecord::with_payload(
                TransactionId(tx_id),
                None,
                None,
                tx_id,
                actor.clone(),
                TransactionKind::Insert,
                encode_row_payload(&schema, &row).expect("row should encode"),
            ),
        )
        .expect("row should append");
    }

    let delete_record = TransactionRecord::without_payload(
        TransactionId(3),
        None,
        Some(TransactionId(2)),
        3,
        actor,
        TransactionKind::Delete,
    );

    wal.append("users", delete_record)
        .expect("delete should append");

    schema

}

fn users_filter_condition() -> SelectCondition {

    SelectCondition::And(vec![

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "email".to_string(),
            op: SelectComparisonOp::Eq,
            value: b"sam@example.com".to_vec(),
        }),

        SelectCondition::Or(vec![
            SelectCondition::Predicate(SelectPredicate::Comparison {
                field_name: "id".to_string(),
                op: SelectComparisonOp::Eq,
                value: b"1".to_vec(),
            }),
            SelectCondition::Predicate(SelectPredicate::Comparison {
                field_name: "nickname".to_string(),
                op: SelectComparisonOp::Eq,
                value: b"sam".to_vec(),
            }),
        ]),

    ])
}

#[test]
fn case_insensitive_string_like_uses_index_when_available() {
    let mut rows_by_id = AHashMap::new();
    rows_by_id.insert(1, HashMap::from([("email".to_string(), b"Sam@Example.com".to_vec())]));
    rows_by_id.insert(2, HashMap::from([("email".to_string(), b"alex@example.com".to_vec())]));

    let mut string_index_ci_by_field = AHashMap::new();
    let mut index = TPHashSet::new();
    index.insert("sam@example.com".to_string(), vec![1]);
    index.insert("alex@example.com".to_string(), vec![2]);
    string_index_ci_by_field.insert("email".to_string(), index);

    let entry = EqualityTableCacheEntry {
        latest_tx_id: 42,
        rows_by_id,
        approx_rows_bytes: 0,
        row_ids_by_field_value: AHashMap::new(),
        string_index_by_field: AHashMap::new(),
        string_index_ci_by_field,
        range_row_ids_cache: AHashMap::new(),
    };

    let rows = rows_for_field_string_like_case_insensitive_indexed(&entry, "email", b"%@example.com");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|(row_id, _)| *row_id == 1));
    assert!(rows.iter().any(|(row_id, _)| *row_id == 2));
}

#[test]
fn collect_indexable_equality_filters_rejects_or() {

    let condition = SelectCondition::Or(vec![

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "id".to_string(),
            op: SelectComparisonOp::Eq,
            value: b"1".to_vec(),
        }),

        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "email".to_string(),
            op: SelectComparisonOp::Eq,
            value: b"sam@example.com".to_vec(),
        }),

    ]);

    let mut filters = HashMap::new();
    assert!(!collect_indexable_equality_filters(
        &condition,
        &mut filters
    ));

}

#[test]
fn durable_cold_equality_probe_prefers_checkpoint_without_wal_hydration() {

    let data_dir = unique_temp_dir("access-equality-cold");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let stream_id = "ent:places";
    let table_id = "places";
    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());
    row.insert("display_name".to_string(), b"Cologne".to_vec());

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    wal_writer
        .append(
            stream_id,
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                UserId("seed-user".to_string()),
                TransactionKind::Insert,
                encode_row_payload(&schema, &row).expect("row should encode"),
            ),
        )
        .expect("seed record should append");

    let latest_tx_id = wal_writer
        .latest_transaction_id(stream_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let table = crate::DatabaseTable::new(table_id.to_string(), schema.clone(), HashMap::new());

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(&data_dir, stream_id)
        .expect("wal fingerprint should exist");

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        stream_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &[(1, row.clone())],
    )
    .expect("live-row checkpoint should save");

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        &data_dir,
        &table,
        stream_id,
        latest_tx_id,
        Some(wal_fingerprint),
        accessor_snapshot_max_live_rows() + 10_000,
    )
    .expect("live-row count checkpoint should save");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(stream_id).is_none());

    let filters = HashMap::from([("display_name".to_string(), b"Cologne".to_vec())]);
    let rows = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        stream_id,
        table_id,
        &schema,
        &filters,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get("display_name"), Some(&b"Cologne".to_vec()));

    assert!(
        wal_cold.latest_transaction_id_if_loaded(stream_id).is_none(),
        "cold equality probe should not hydrate WAL when checkpoint-backed paths are available",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_scoped_equality_fallback_to_legacy_stream_avoids_wal_hydration() {

    let data_dir = unique_temp_dir("access-scoped-fallback");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let latest_tx_id = wal_writer
        .latest_transaction_id(&table.table_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        &table.table_id,
    )
    .expect("wal fingerprint should exist");

    let live_rows = load_live_rows(&wal_writer, &table.table_id, &table.table_id, &schema);

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &live_rows,
    )
    .expect("live-row checkpoint should save");

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        accessor_snapshot_max_live_rows() + 10_000,
    )
    .expect("live-row count checkpoint should save");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.entity_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &table,
        &schema,
        &RuntimeIndexStore::new(),
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::TemporaryIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "legacy stream should remain cold during scoped fallback when checkpoints are available",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_scoped_equality_probe_prefers_scoped_checkpoint_over_loaded_legacy_stream() {

    let data_dir = unique_temp_dir("access-scoped-checkpoint-preferred");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");

    let table = catalog.table("users").expect("users table should exist");
    let scoped_stream_id = "scope:users";

    let actor = UserId("test-user".to_string());

    let legacy_row = HashMap::from([
        ("id".to_string(), b"1".to_vec()),
        ("email".to_string(), b"legacy@example.com".to_vec()),
    ]);
    wal_writer
        .append(
            &table.table_id,
            TransactionRecord::with_payload(
                TransactionId(1),
                None,
                None,
                1,
                actor.clone(),
                TransactionKind::Insert,
                encode_row_payload(&schema, &legacy_row).expect("legacy row should encode"),
            ),
        )
        .expect("legacy row should append");

    let scoped_row = HashMap::from([
        ("id".to_string(), b"2".to_vec()),
        ("email".to_string(), b"sam@example.com".to_vec()),
    ]);
    wal_writer
        .append(
            scoped_stream_id,
            TransactionRecord::with_payload(
                TransactionId(2),
                None,
                None,
                2,
                actor,
                TransactionKind::Insert,
                encode_row_payload(&schema, &scoped_row).expect("scoped row should encode"),
            ),
        )
        .expect("scoped row should append");

    let latest_tx_id = wal_writer
        .latest_transaction_id(scoped_stream_id)
        .map(|tx| tx.0)
        .expect("scoped latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        scoped_stream_id,
    )
    .expect("scoped wal fingerprint should exist");

    let scoped_live_rows = load_live_rows(&wal_writer, scoped_stream_id, &table.table_id, &schema);

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        scoped_stream_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &scoped_live_rows,
    )
    .expect("scoped live-row checkpoint should save");

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        &data_dir,
        &table,
        scoped_stream_id,
        latest_tx_id,
        Some(wal_fingerprint),
        scoped_live_rows.len(),
    )
    .expect("scoped live-row count checkpoint should save");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(scoped_stream_id).is_none());

    let _ = wal_cold.latest_transaction_id(&table.table_id);
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_some());

    let mut scoped_table = table.clone();
    scoped_table.entity_id = scoped_stream_id.to_string();

    let rows = materialize_relation_rows(
        &wal_cold,
        &scoped_table,
        &schema,
        &RuntimeIndexStore::new(),
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::TemporaryIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get("email"), Some(&b"sam@example.com".to_vec()));

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_scoped_numeric_equality_probe_uses_scoped_checkpoint_after_restart() {

    let data_dir = unique_temp_dir("access-scoped-numeric-checkpoint");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let schema = table_schema(vec![
        ("id", 1, FieldType::Int(32), FieldIndex::PrimaryKey, false),
        ("id_parent", 2, FieldType::Int(32), FieldIndex::Indexed, false),
    ]);

    let table = crate::DatabaseTable::new("regions".to_string(), schema.clone(), HashMap::new());
    let scoped_stream_id = "scope:regions";
    let actor = UserId("test-user".to_string());

    let target_rows = 64usize;

    for id in 1..=target_rows {
        let row = HashMap::from([
            ("id".to_string(), id.to_string().into_bytes()),
            ("id_parent".to_string(), b"254".to_vec()),
        ]);

        wal_writer
            .append(
                scoped_stream_id,
                TransactionRecord::with_payload(
                    TransactionId(id as u64),
                    None,
                    None,
                    id as u64,
                    actor.clone(),
                    TransactionKind::Insert,
                    encode_row_payload(&schema, &row).expect("scoped row should encode"),
                ),
            )
            .expect("scoped row should append");
    }

    let legacy_row = HashMap::from([
        ("id".to_string(), b"9999".to_vec()),
        ("id_parent".to_string(), b"999".to_vec()),
    ]);

    wal_writer
        .append(
            &table.table_id,
            TransactionRecord::with_payload(
                TransactionId((target_rows + 1) as u64),
                None,
                None,
                (target_rows + 1) as u64,
                actor,
                TransactionKind::Insert,
                encode_row_payload(&schema, &legacy_row).expect("legacy row should encode"),
            ),
        )
        .expect("legacy row should append");

    let latest_tx_id = wal_writer
        .latest_transaction_id(scoped_stream_id)
        .map(|tx| tx.0)
        .expect("scoped latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        scoped_stream_id,
    )
    .expect("scoped wal fingerprint should exist");

    let scoped_live_rows = load_live_rows(&wal_writer, scoped_stream_id, &table.table_id, &schema);

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        scoped_stream_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &scoped_live_rows,
    )
    .expect("scoped live-row checkpoint should save");

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        &data_dir,
        &table,
        scoped_stream_id,
        latest_tx_id,
        Some(wal_fingerprint),
        scoped_live_rows.len(),
    )
    .expect("scoped live-row count checkpoint should save");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(scoped_stream_id).is_none());

    let _ = wal_cold.latest_transaction_id(&table.table_id);
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_some());

    let mut scoped_table = table.clone();
    scoped_table.entity_id = scoped_stream_id.to_string();

    let lookup_value = convert_value_to_field_type(
        b"254",
        &FieldType::Int(32),
        crate::TypeConversionPolicy::Safe,
    )
    .expect("lookup value should normalize");

    let rows = materialize_relation_rows(
        &wal_cold,
        &scoped_table,
        &schema,
        &RuntimeIndexStore::new(),
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "id_parent".to_string(),
                lookup_value: lookup_value.clone(),
                source: EqualityProbeSource::TemporaryIndex,
                equality_filters: HashMap::from([(
                    "id_parent".to_string(),
                    lookup_value,
                )]),
            },
        },
    );

    assert_eq!(rows.len(), target_rows);
    assert!(rows.iter().all(|(_, row)| row.get("id_parent").is_some()));

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_existing_index_equality_without_runtime_state_falls_through_to_cold_scan() {

    let data_dir = unique_temp_dir("access-existing-index-no-runtime-state");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &table,
        &schema,
        &RuntimeIndexStore::new(),
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    // When no checkpoint exists and the state is missing, the path falls through
    // to a cold scan so the first request populates the checkpoint for future use.
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_cold_unique_row_ref_probe_uses_checkpoint_without_wal_hydration() {

    let data_dir = unique_temp_dir("access-unique-row-ref");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let latest_tx_id = wal_writer
        .latest_transaction_id(&table.table_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        &table.table_id,
    )
    .expect("wal fingerprint should exist");

    let live_rows = load_live_rows(&wal_writer, &table.table_id, &table.table_id, &schema);
    let stored_id_value = live_rows
        .iter()
        .find(|(row_id, _)| *row_id == 1)
        .and_then(|(_, row_map)| row_map.get("id").cloned())
        .expect("stored id value should exist");

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &live_rows,
    )
    .expect("live-row checkpoint should save");

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        accessor_snapshot_max_live_rows() + 10_000,
    )
    .expect("live-row count checkpoint should save");

    let id_index = table
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 && index.field_names[0] == "id"
        })
        .cloned()
        .expect("id index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &id_index.index_id.0);
    state.index = Some(id_index.clone());
    state.insert_with_row_ref(vec![stored_id_value.clone()], Some(1));

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "id".to_string(),
                lookup_value: stored_id_value.clone(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([(
                    "id".to_string(),
                    stored_id_value,
                )]),
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "unique row-ref equality probe should avoid cold WAL hydration",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_cold_non_unique_row_refs_without_checkpoint_hydrate_instead_of_scanning() {

    let data_dir = unique_temp_dir("access-non-unique-row-refs-no-checkpoint");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let email_index = table
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 && index.field_names[0] == "email"
        })
        .cloned()
        .expect("email index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &email_index.index_id.0);
    state.index = Some(email_index.clone());
    state.insert_with_row_ref(vec![b"sam@example.com".to_vec()], Some(1));

    // No live-row checkpoint is saved: the probe must hydrate the stream to resolve
    // its row refs rather than abandoning them for a full WAL scan.
    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get("email"), Some(&b"sam@example.com".to_vec()));

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_some(),
        "row refs without a checkpoint should resolve through a hydrated stream read",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_cold_non_unique_key_present_without_row_refs_uses_checkpoint() {

    let data_dir = unique_temp_dir("access-non-unique-key-present-no-row-refs");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let latest_tx_id = wal_writer
        .latest_transaction_id(&table.table_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        &table.table_id,
    )
    .expect("wal fingerprint should exist");

    let live_rows = load_live_rows(&wal_writer, &table.table_id, &table.table_id, &schema);

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &live_rows,
    )
    .expect("live-row checkpoint should save");

    let email_index = table
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 && index.field_names[0] == "email"
        })
        .cloned()
        .expect("email index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &email_index.index_id.0);
    state.index = Some(email_index.clone());

    // Reproduce stale/non-hydrated postings: index key exists, row-ref postings missing.
    state.insert(vec![b"sam@example.com".to_vec()]);

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.get("email"), Some(&b"sam@example.com".to_vec()));

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "checkpoint recovery should avoid cold WAL hydration",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn scoped_equality_probe_key_present_without_row_refs_returns_empty() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let email_index = table
        .indexes
        .values()
        .find(|index| index.field_names.len() == 1 && index.field_names[0] == "email")
        .cloned()
        .expect("email index should exist");

    let scoped_stream_id = "scope:users";
    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(scoped_stream_id, &email_index.index_id.0);
    state.index = Some(email_index.clone());

    // Reproduce scoped clone mismatch: key exists in scoped runtime state but
    // row-ref postings are missing; data exists in legacy stream.
    state.insert(vec![b"sam@example.com".to_vec()]);

    let mut relation_with_scoped_stream = table.clone();
    relation_with_scoped_stream.entity_id = scoped_stream_id.to_string();

    let rows = materialize_relation_rows(
        &wal,
        &relation_with_scoped_stream,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert!(rows.is_empty());

}

#[test]
fn durable_scoped_equality_probe_key_present_without_row_refs_returns_empty() {

    let data_dir = unique_temp_dir("access-durable-scoped-key-present-legacy-checkpoint");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let latest_tx_id = wal_writer
        .latest_transaction_id(&table.table_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        &table.table_id,
    )
    .expect("wal fingerprint should exist");

    let live_rows = load_live_rows(&wal_writer, &table.table_id, &table.table_id, &schema);

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &live_rows,
    )
    .expect("legacy live-row checkpoint should save");

    let email_index = table
        .indexes
        .values()
        .find(|index| index.field_names.len() == 1 && index.field_names[0] == "email")
        .cloned()
        .expect("email index should exist");

    let scoped_stream_id = "scope:durable:users";
    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(scoped_stream_id, &email_index.index_id.0);
    state.index = Some(email_index.clone());
    state.insert(vec![b"sam@example.com".to_vec()]);

    let mut relation_with_scoped_stream = table.clone();
    relation_with_scoped_stream.entity_id = scoped_stream_id.to_string();

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &relation_with_scoped_stream,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert!(rows.is_empty());

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "legacy checkpoint recovery should avoid cold WAL hydration",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_scoped_equality_probe_without_scoped_checkpoint_prefers_legacy_checkpoint_stream() {

    let data_dir = unique_temp_dir("access-durable-scoped-no-checkpoint-prefers-legacy");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let latest_tx_id = wal_writer
        .latest_transaction_id(&table.table_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        &table.table_id,
    )
    .expect("wal fingerprint should exist");

    let live_rows = load_live_rows(&wal_writer, &table.table_id, &table.table_id, &schema);

    RuntimeIndexSnapshotService::save_live_row_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        &live_rows,
    )
    .expect("legacy live-row checkpoint should save");

    let mut relation_with_scoped_stream = table.clone();
    relation_with_scoped_stream.entity_id = "scope:checkpoint:users".to_string();

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());
    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let rows = materialize_relation_rows(
        &wal_cold,
        &relation_with_scoped_stream,
        &schema,
        &RuntimeIndexStore::new(),
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::TemporaryIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert!(
        wal_cold
            .latest_transaction_id_if_loaded(relation_with_scoped_stream.entity_id.as_str())
            .is_none(),
        "scoped stream should not hydrate when legacy checkpoint fallback is used",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_large_without_live_row_checkpoint_prefers_filtered_scan_without_hydration() {

    let data_dir = unique_temp_dir("access-large-no-live-row-checkpoint");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let latest_tx_id = wal_writer
        .latest_transaction_id(&table.table_id)
        .map(|tx| tx.0)
        .expect("latest tx id should exist");

    let wal_fingerprint = RuntimeIndexSnapshotService::wal_stream_fingerprint(
        &data_dir,
        &table.table_id,
    )
    .expect("wal fingerprint should exist");

    RuntimeIndexSnapshotService::save_live_row_count_checkpoint(
        &data_dir,
        &table,
        &table.table_id,
        latest_tx_id,
        Some(wal_fingerprint),
        accessor_snapshot_max_live_rows() + 10_000,
    )
    .expect("live-row count checkpoint should save");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let filters = HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]);

    let rows = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        &table.table_id,
        &table.table_id,
        &schema,
        &filters,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "large cold equality path should use filtered scan and avoid WAL hydration",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_cold_without_checkpoints_prefers_filtered_scan_without_hydration() {

    let data_dir = unique_temp_dir("access-cold-no-checkpoints");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let filters = HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]);
    let rows = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        &table.table_id,
        &table.table_id,
        &schema,
        &filters,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "cold durable equality probe without checkpoints should avoid full WAL hydration",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn planner_cache_uses_equality_probe_hits_before_runtime_index_materialization() {

    let data_dir = unique_temp_dir("access-planner-cache");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");
    let email_index = table.indexes.values().find(|index| index.field_names == vec!["email".to_string()]).cloned().expect("email index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &email_index.index_id.0);
    state.index = Some(email_index.clone());
    state.insert_with_row_ref(vec![b"sam@example.com".to_vec()], Some(1));

    let wal = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let hydrated_latest_tx = wal.latest_transaction_id(&table.table_id);
    assert!(hydrated_latest_tx.is_some());
    assert!(wal.latest_transaction_id_if_loaded(&table.table_id).is_some());

    let filters = HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]);
    let latest_tx_id = wal.latest_transaction_id_if_loaded(&table.table_id).map(|tx| tx.0).unwrap_or(1);

    maybe_cache_equality_probe_rows_with_latest_tx_id(
        &wal,
        &table.table_id,
        &filters,
        &[(1, HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]))],
        Some(latest_tx_id),
    );

    let rows = planner_cached_rows_for_access_plan(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::RuntimeIndexLookup {
                index_id: email_index.index_id.0.clone(),
                lookup_key: vec![b"sam@example.com".to_vec()],
            },
        },
        Some(10),
    );

    assert_eq!(rows.as_ref().map(Vec::len), Some(1));
    assert_eq!(rows.unwrap()[0].0, 1);

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn runtime_index_lookup_populates_equality_probe_result_cache() {

    let data_dir = unique_temp_dir("access-runtime-index-cache");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");
    let email_index = table.indexes.values().find(|index| index.field_names == vec!["email".to_string()]).cloned().expect("email index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &email_index.index_id.0);
    state.index = Some(email_index.clone());
    state.insert_with_row_ref(vec![b"sam@example.com".to_vec()], Some(1));

    let wal = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let filters = HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]);

    let rows = materialize_relation_rows_with_limit(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::RuntimeIndexLookup {
                index_id: email_index.index_id.0.clone(),
                lookup_key: vec![b"sam@example.com".to_vec()],
            },
        },
        Some(10),
    );

    assert_eq!(rows.len(), 1);
    let cached = cached_equality_probe_rows(&wal, &table.table_id, &filters);
    assert!(cached.is_some(), "runtime index lookup should populate equality probe result cache");
    assert_eq!(cached.unwrap().len(), 1);

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_cold_repeated_equality_probe_reuses_scoped_cache_without_second_scan() {

    let data_dir = unique_temp_dir("access-cold-repeat-equality-cache");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal_writer);
    let table = catalog.table("users").expect("users table should exist");

    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    assert!(wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none());

    let filters = HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]);
    let initial_wal_scan_loads = accessor_load_source_stats_for_test(
        wal_cold.cache_scope_id(),
        &table.table_id,
    )
        .map(|stats| stats.2)
        .unwrap_or(0);

    let rows_first = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        &table.table_id,
        &table.table_id,
        &schema,
        &filters,
        None,
    );

    assert_eq!(rows_first.len(), 1);
    assert_eq!(rows_first[0].0, 1);

    let rows_second = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        &table.table_id,
        &table.table_id,
        &schema,
        &filters,
        None,
    );

    assert_eq!(rows_second.len(), 1);
    assert_eq!(rows_second[0].0, 1);

    assert!(
        wal_cold.latest_transaction_id_if_loaded(&table.table_id).is_none(),
        "repeated cold equality probes should not hydrate WAL",
    );

    let stats = accessor_load_source_stats_for_test(
        wal_cold.cache_scope_id(),
        &table.table_id,
    )
        .expect("accessor load source stats should be recorded for stream");
    assert_eq!(
        stats.2.saturating_sub(initial_wal_scan_loads),
        1,
        "only one additional WAL filtered scan should occur for repeated identical probe",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn durable_cold_equality_cache_does_not_poison_other_values() {

    let data_dir = unique_temp_dir("access-cold-equality-poison");
    fs::create_dir_all(&data_dir).expect("temp data dir should be created");

    let wal_writer = ConcurrentWalManager::with_data_dir(data_dir.clone());
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");

    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", schema.clone())
        .expect("places table should register");

    let actor = UserId("test-user".to_string());

    for (tx_id, id, name) in [
        (1u64, b"1".as_slice(), b"alpha".as_slice()),
        (2u64, b"2".as_slice(), b"beta".as_slice()),
    ] {
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.to_vec());
        row.insert("name".to_string(), name.to_vec());

        wal_writer
            .append(
                "places",
                TransactionRecord::with_payload(
                    TransactionId(tx_id),
                    None,
                    None,
                    tx_id,
                    actor.clone(),
                    TransactionKind::Insert,
                    encode_row_payload(&schema, &row).expect("row should encode"),
                ),
            )
            .expect("row should append");
    }

    let table = catalog.table("places").expect("places table should exist");
    let wal_cold = ConcurrentWalManager::with_data_dir(data_dir.clone());

    let initial_wal_scan_loads = accessor_load_source_stats_for_test(
        wal_cold.cache_scope_id(),
        &table.table_id,
    )
        .map(|stats| stats.2)
        .unwrap_or(0);

    let rows_alpha = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        &table.table_id,
        &table.table_id,
        &schema,
        &HashMap::from([("name".to_string(), b"alpha".to_vec())]),
        None,
    );

    assert_eq!(rows_alpha.len(), 1);
    assert_eq!(rows_alpha[0].1.get("name"), Some(&b"alpha".to_vec()));

    let rows_beta = load_live_rows_by_equality_filters_with_limit(
        &wal_cold,
        &table.table_id,
        &table.table_id,
        &schema,
        &HashMap::from([("name".to_string(), b"beta".to_vec())]),
        None,
    );

    assert_eq!(rows_beta.len(), 1);
    assert_eq!(rows_beta[0].1.get("name"), Some(&b"beta".to_vec()));

    let stats = accessor_load_source_stats_for_test(
        wal_cold.cache_scope_id(),
        &table.table_id,
    )
        .expect("accessor load source stats should be recorded for stream");
    assert_eq!(
        stats.2.saturating_sub(initial_wal_scan_loads),
        2,
        "different equality values should not reuse a partial row cache",
    );

    let _ = fs::remove_dir_all(&data_dir);

}

#[test]
fn build_relation_probe_index_groups_duplicate_keys() {

    let rows = vec![

        MaterializedRelationRow {
            row_id: 1,
            row_map: Arc::new(HashMap::from([("id".to_string(), b"1".to_vec())])),
        },

        MaterializedRelationRow {
            row_id: 2,
            row_map: Arc::new(HashMap::from([("id".to_string(), b"1".to_vec())])),
        },

    ];

    let index = build_relation_probe_index(&rows, "id");
    assert_eq!(index.get(b"1".as_slice()).map(Vec::len), Some(2));
}

#[test]
fn field_has_single_column_index_detects_indexed_columns() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    assert!(field_has_single_column_index(&table, "id"));
    assert!(field_has_single_column_index(&table, "email"));
    assert!(!field_has_single_column_index(&table, "nickname"));
    assert_eq!(schema.fields.len(), 3);
}

#[test]
fn field_has_single_column_index_ignores_stale_index_for_removed_column() {
    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
    ]);

    let mut indexes = HashMap::new();
    let stale = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    );
    indexes.insert(stale.index_id.0.clone(), stale);

    let table = crate::DatabaseTable::new("users".to_string(), schema, indexes);

    assert!(!field_has_single_column_index(&table, "email"));
}

#[test]
fn choose_index_lookup_ignores_stale_index_for_removed_column() {
    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
    ]);

    let mut indexes = HashMap::new();
    let stale = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["email".to_string()],
    );
    indexes.insert(stale.index_id.0.clone(), stale);

    let table = crate::DatabaseTable::new("users".to_string(), schema, indexes);

    let filters = HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]);

    assert!(choose_index_lookup(&table, &filters).is_none());
}

#[test]
fn count_condition_predicates_counts_nested_boolean_tree() {
    let condition = users_filter_condition();
    assert_eq!(count_condition_predicates(&condition), 3);
}

#[test]
fn choose_index_lookup_returns_lookup_for_matching_index() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let filters = HashMap::from([
        ("id".to_string(), b"1".to_vec()),
        ("email".to_string(), b"sam@example.com".to_vec()),
    ]);

    let (index, lookup_key) =
        choose_index_lookup(&table, &filters).expect("an index lookup should be selected");

    assert_eq!(lookup_key.len(), 1);
    assert_eq!(lookup_key[0], b"1".to_vec());
    assert!(index.is_primary_key());

}

#[test]
fn choose_index_lookup_prioritizes_pk_then_uk_then_relationship() {

    let mut indexes = HashMap::new();

    let pk = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::PrimaryKey,
        vec!["id".to_string()],
    );
    indexes.insert(pk.index_id.0.clone(), pk);

    let uk = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Unique,
        vec!["email".to_string()],
    );
    indexes.insert(uk.index_id.0.clone(), uk);

    let rel = DatabaseIndex::from_table_fields_with_origin(
        "users",
        DatabaseIndexKind::Indexed,
        DatabaseIndexOrigin::Relationship,
        None,
        vec!["account_id".to_string()],
    );
    indexes.insert(rel.index_id.0.clone(), rel);

    let sec = DatabaseIndex::from_table_fields(
        "users",
        DatabaseIndexKind::Indexed,
        vec!["status".to_string()],
    );
    indexes.insert(sec.index_id.0.clone(), sec);

    let table = crate::DatabaseTable::new(
        "users".to_string(),
        TableSchema::new(Vec::new()),
        indexes,
    );

    let filters = HashMap::from([
        ("id".to_string(), b"1".to_vec()),
        ("email".to_string(), b"a@example.com".to_vec()),
        ("account_id".to_string(), b"acc-1".to_vec()),
        ("status".to_string(), b"active".to_vec()),
    ]);

    let (chosen_pk, _) =
        choose_index_lookup(&table, &filters).expect("pk candidate should be selected");
    assert!(chosen_pk.is_primary_key());

    let filters_without_pk = HashMap::from([
        ("email".to_string(), b"a@example.com".to_vec()),
        ("account_id".to_string(), b"acc-1".to_vec()),
        ("status".to_string(), b"active".to_vec()),
    ]);

    let (chosen_uk, _) = choose_index_lookup(&table, &filters_without_pk)
        .expect("uk candidate should be selected");
    assert!(chosen_uk.is_unique_key() && !chosen_uk.is_primary_key());

    let filters_relationship_only = HashMap::from([
        ("account_id".to_string(), b"acc-1".to_vec()),
        ("status".to_string(), b"active".to_vec()),
    ]);

    let (chosen_rel, _) = choose_index_lookup(&table, &filters_relationship_only)
        .expect("relationship candidate should be selected");
    assert!(chosen_rel.is_relationship_driven());

}

#[test]
fn plan_relation_access_selects_equality_probe_and_full_scan() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let mut filters = HashMap::new();
    filters.insert("email".to_string(), b"sam@example.com".to_vec());

    let equality_plan = plan_relation_access(&table, false, filters.clone(), None, Vec::new(), None);
    assert!(matches!(
        equality_plan.strategy,
        RelationAccessStrategy::EqualityProbe { .. }
    ));

    let full_scan_plan = plan_relation_access(&table, false, HashMap::new(), None, Vec::new(), None);
    assert!(matches!(
        full_scan_plan.strategy,
        RelationAccessStrategy::FullScan
    ));

    let short_circuit_plan = plan_relation_access(&table, true, filters, None, Vec::new(), None);
    assert!(matches!(
        short_circuit_plan.strategy,
        RelationAccessStrategy::EqualityProbe {
            source: EqualityProbeSource::ExistingIndex,
            ..
        }
    ));

    let pk_short_circuit_plan = plan_relation_access(
        &table,
        true,
        HashMap::from([("id".to_string(), b"1".to_vec())]),
        None,
        Vec::new(),
        None,
    );
    assert!(matches!(
        pk_short_circuit_plan.strategy,
        RelationAccessStrategy::RuntimeIndexLookup { .. }
    ));
}

#[test]
fn plan_relation_access_prefers_equality_probe_for_multi_filter_non_unique_indexes() {
    let schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
        ("country_code", 3, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("places", schema)
        .expect("places table should register");
    let table = catalog.table("places").expect("places table should exist");

    let plan = plan_relation_access(
        &table,
        true,
        HashMap::from([
            ("display_name".to_string(), b"Cologne".to_vec()),
            ("country_code".to_string(), b"GM".to_vec()),
        ]),
        None,
        Vec::new(),
        None,
    );

    assert!(matches!(
        plan.strategy,
        RelationAccessStrategy::EqualityProbe {
            source: EqualityProbeSource::ExistingIndex,
            ..
        }
    ));

    let RelationAccessStrategy::EqualityProbe {
        equality_filters,
        ..
    } = plan.strategy else {
        unreachable!("plan should be equality probe");
    };

    assert_eq!(equality_filters.len(), 2);
    assert_eq!(equality_filters.get("display_name"), Some(&b"Cologne".to_vec()));
    assert_eq!(equality_filters.get("country_code"), Some(&b"GM".to_vec()));
}

#[test]
fn plan_relation_access_with_runtime_hint_prefers_more_selective_multi_filter_field() {
    let schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
        ("country_code", 3, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("places", schema)
        .expect("places table should register");
    let table = catalog.table("places").expect("places table should exist");

    let display_name_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["display_name".to_string()])
        .cloned()
        .expect("display_name index should exist");
    let country_code_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["country_code".to_string()])
        .cloned()
        .expect("country_code index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();

    // country_code='US' matches many rows (low selectivity).
    let country_code_state =
        runtime_indexes.index_mut_for_table(&table.table_id, &country_code_index.index_id.0);
    country_code_state.index = Some(country_code_index.clone());
    for row_ref in 1..=50u64 {
        country_code_state.insert_with_row_ref(vec![b"US".to_vec()], Some(row_ref));
    }

    // display_name='Cologne' matches a single row (high selectivity).
    let display_name_state =
        runtime_indexes.index_mut_for_table(&table.table_id, &display_name_index.index_id.0);
    display_name_state.index = Some(display_name_index.clone());
    display_name_state.insert_with_row_ref(vec![b"Cologne".to_vec()], Some(4976506));

    let plan = plan_relation_access_with_runtime_hint(
        &table,
        true,
        HashMap::from([
            ("display_name".to_string(), b"Cologne".to_vec()),
            ("country_code".to_string(), b"US".to_vec()),
        ]),
        None,
        Vec::new(),
        None,
        Some((&runtime_indexes, table.table_id.as_str())),
    );

    let RelationAccessStrategy::EqualityProbe { field_name, .. } = plan.strategy else {
        panic!("expected equality probe plan, got {:?}", plan.strategy);
    };

    assert_eq!(
        field_name, "display_name",
        "planner should prefer the more selective field over the alphabetically-first one",
    );
}

#[test]
fn planner_uses_runtime_lookup_for_non_unique_indexed_field() {
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");
    let wal = ConcurrentWalManager::in_memory();
    seed_users_table(&mut catalog, &wal);

    let table = catalog.table("users").expect("users table should exist");
    let email_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["email".to_string()])
        .expect("email index should exist");

    assert!(!email_index.is_unique_key());

    let runtime_indexes = RuntimeIndexStore::new();
    let plan = plan_relation_access_with_runtime_hint(
        table,
        true,
        HashMap::from([("email".to_string(), b"sam@example.com".to_vec())]),
        None,
        Vec::new(),
        None,
        Some((&runtime_indexes, "users")),
    );

    assert!(matches!(
        plan.strategy,
        RelationAccessStrategy::RuntimeIndexLookup { .. }
    ));
}

#[test]
fn runtime_index_equality_count_uses_posting_cardinality() {
    let schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("country_code", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("places", schema)
        .expect("places table should register");
    let table = catalog.table("places").expect("places table should exist");
    let country_code_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["country_code".to_string()])
        .cloned()
        .expect("country_code index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &country_code_index.index_id.0);
    state.index = Some(country_code_index);
    state.insert_with_row_ref(vec![b"US".to_vec()], Some(101));
    state.insert_with_row_ref(vec![b"US".to_vec()], Some(202));

    assert_eq!(
        count_runtime_index_equality_probe_rows(
            &runtime_indexes,
            &table,
            &table.table_id,
            "country_code",
            b"US",
        ),
        Some(2),
    );
}

#[test]
fn collect_indexable_prefix_like_filter_for_schema_extracts_simple_prefix() {
    let schema = table_schema(vec![
        ("email", 1, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    let condition = SelectCondition::Predicate(SelectPredicate::Like {
        field_name: "email".to_string(),
        pattern: b"sam%".to_vec(),
        negated: false,
        case_insensitive: false,
        escape_char: None,
    });

    let probe = collect_indexable_prefix_like_filter_for_schema(&schema, &condition)
        .expect("prefix-like predicate should be extracted");

    assert_eq!(probe.0, "email");
    assert_eq!(probe.1, b"sam".to_vec());
    assert!(!probe.2);
}

#[test]
fn plan_relation_access_selects_prefix_like_probe_when_available() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let prefix_plan = plan_relation_access(
        &table,
        false,
        HashMap::new(),
        None,
        Vec::new(),
        Some(("email".to_string(), b"sam".to_vec(), false)),
    );

    assert!(matches!(
        prefix_plan.strategy,
        RelationAccessStrategy::PrefixLikeProbe { .. }
    ));
}

#[test]
fn collect_indexable_in_list_filter_for_schema_extracts_values() {
    let schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
    ]);

    let condition = SelectCondition::Predicate(SelectPredicate::InList {
        field_name: "uid".to_string(),
        values: vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
        negated: false,
    });

    let in_list = collect_indexable_in_list_filter_for_schema(&schema, &condition)
        .expect("in-list predicate should be extracted");

    assert_eq!(in_list.0, "uid");
    assert_eq!(in_list.1.len(), 3);
    assert!(in_list.1.iter().all(|value| !value.is_empty()));
}

#[test]
fn plan_relation_access_selects_in_list_probe_when_available() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let plan = plan_relation_access(
        &table,
        false,
        HashMap::new(),
        Some((
            "id".to_string(),
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
        )),
        Vec::new(),
        None,
    );

    assert!(matches!(
        plan.strategy,
        RelationAccessStrategy::InListProbe {
            source: EqualityProbeSource::ExistingIndex,
            ..
        }
    ));
}

#[test]
fn plan_relation_access_prefers_indexed_equality_over_in_list() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let mut equality_filters = HashMap::new();
    equality_filters.insert("email".to_string(), b"sam@example.com".to_vec());

    let plan = plan_relation_access(
        &table,
        false,
        equality_filters,
        Some((
            "id".to_string(),
            vec![b"1".to_vec(), b"2".to_vec(), b"3".to_vec()],
        )),
        Vec::new(),
        None,
    );

    assert!(matches!(
        plan.strategy,
        RelationAccessStrategy::EqualityProbe {
            source: EqualityProbeSource::ExistingIndex,
            ..
        }
    ));
}

#[test]
fn collect_indexable_range_filter_for_schema_extracts_bounds() {
    let schema = table_schema(vec![
        ("latitude", 1, FieldType::Float(64), FieldIndex::Indexed, false),
    ]);

    let condition = SelectCondition::And(vec![
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "latitude".to_string(),
            op: SelectComparisonOp::GtEq,
            value: b"50.0".to_vec(),
        }),
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "latitude".to_string(),
            op: SelectComparisonOp::LtEq,
            value: b"51.0".to_vec(),
        }),
    ]);

    let probe = collect_indexable_range_filter_for_schema(&schema, &condition)
        .expect("range predicate should be extracted");

    assert_eq!(probe.field_name, "latitude");
    assert!(probe
        .lower_bound
        .as_ref()
        .map(|bound| bound.inclusive)
        .unwrap_or(false));
    assert!(probe
        .upper_bound
        .as_ref()
        .map(|bound| bound.inclusive)
        .unwrap_or(false));
}

#[test]
fn collect_indexable_range_filter_for_schema_keeps_single_probe_for_multi_field_ranges() {
    let schema = table_schema(vec![
        ("latitude", 1, FieldType::Float(64), FieldIndex::Indexed, false),
        ("longitude", 2, FieldType::Float(64), FieldIndex::Indexed, false),
    ]);

    let condition = SelectCondition::And(vec![
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "latitude".to_string(),
            op: SelectComparisonOp::GtEq,
            value: b"50.0".to_vec(),
        }),
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "latitude".to_string(),
            op: SelectComparisonOp::LtEq,
            value: b"51.0".to_vec(),
        }),
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "longitude".to_string(),
            op: SelectComparisonOp::GtEq,
            value: b"6.8".to_vec(),
        }),
        SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "longitude".to_string(),
            op: SelectComparisonOp::LtEq,
            value: b"7.0".to_vec(),
        }),
    ]);

    let probes = collect_indexable_range_filters_for_schema(&schema, &condition);

    assert_eq!(probes.len(), 2);
    assert!(probes.iter().any(|probe| {
        probe.field_name == "latitude" && probe.lower_bound.is_some() && probe.upper_bound.is_some()
    }));
    assert!(probes.iter().any(|probe| {
        probe.field_name == "longitude" && probe.lower_bound.is_some() && probe.upper_bound.is_some()
    }));
}

#[test]
fn plan_relation_access_selects_range_probe_when_available() {
    let schema = table_schema(vec![
        ("latitude", 1, FieldType::Float(64), FieldIndex::Indexed, false),
    ]);

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("places", schema)
        .expect("places table should register");
    let table = catalog.table("places").expect("places table should exist");

    let range_plan = plan_relation_access(
        &table,
        false,
        HashMap::new(),
        None,
        vec![RangeFilterBounds {
            field_name: "latitude".to_string(),
            lower_bound: Some(RangeBound {
                value: b"50.0".to_vec(),
                inclusive: true,
            }),
            upper_bound: Some(RangeBound {
                value: b"51.0".to_vec(),
                inclusive: true,
            }),
        }],
        None,
    );

    assert!(matches!(
        range_plan.strategy,
        RelationAccessStrategy::RangeProbe { .. }
    ));
}

#[test]
fn plan_relation_access_selects_range_intersection_probe_when_multiple_ranges_available() {
    let schema = table_schema(vec![
        ("latitude", 1, FieldType::Float(64), FieldIndex::Indexed, false),
        ("longitude", 2, FieldType::Float(64), FieldIndex::Indexed, false),
    ]);

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("places", schema)
        .expect("places table should register");
    let table = catalog.table("places").expect("places table should exist");

    let range_plan = plan_relation_access(
        &table,
        false,
        HashMap::new(),
        None,
        vec![
            RangeFilterBounds {
                field_name: "latitude".to_string(),
                lower_bound: Some(RangeBound {
                    value: b"50.0".to_vec(),
                    inclusive: true,
                }),
                upper_bound: Some(RangeBound {
                    value: b"51.0".to_vec(),
                    inclusive: true,
                }),
            },
            RangeFilterBounds {
                field_name: "longitude".to_string(),
                lower_bound: Some(RangeBound {
                    value: b"6.8".to_vec(),
                    inclusive: true,
                }),
                upper_bound: Some(RangeBound {
                    value: b"7.0".to_vec(),
                    inclusive: true,
                }),
            },
        ],
        None,
    );

    assert!(matches!(
        range_plan.strategy,
        RelationAccessStrategy::RangeIntersectionProbe { .. }
    ));
}

#[test]
fn load_live_rows_filters_deleted_records() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);

    let rows = load_live_rows(&wal, "users", "users", &schema);
    
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1.get("email"), Some(&b"sam@example.com".to_vec()));

}

#[test]
fn load_live_rows_tracks_latest_version_chain_and_delete() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let actor = UserId::from_username("test-user");

    let mut updated_row = HashMap::new();
    updated_row.insert("id".to_string(), b"1".to_vec());
    updated_row.insert("email".to_string(), b"sam+updated@example.com".to_vec());
    updated_row.insert("nickname".to_string(), b"sam".to_vec());

    wal.append(
        "users",
        TransactionRecord::without_payload(
            TransactionId(4),
            None,
            Some(TransactionId(1)),
            4,
            actor.clone(),
            TransactionKind::Delete,
        ),
    )
    .expect("delete old version should append");

    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(5),
            None,
            Some(TransactionId(1)),
            5,
            actor.clone(),
            TransactionKind::Update,
            encode_row_payload(&schema, &updated_row).expect("updated row should encode"),
        ),
    )
    .expect("updated version should append");

    let rows = load_live_rows(&wal, "users", "users", &schema);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 5);
    assert_eq!(rows[0].1.get("email"), Some(&b"sam+updated@example.com".to_vec()));

    wal.append(
        "users",
        TransactionRecord::without_payload(
            TransactionId(6),
            None,
            Some(TransactionId(5)),
            6,
            actor,
            TransactionKind::Delete,
        ),
    )
    .expect("delete latest version should append");

    assert!(load_live_rows(&wal, "users", "users", &schema).is_empty());

}

#[test]
fn load_live_row_count_tracks_latest_version_chain_and_delete() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let actor = UserId::from_username("test-user");

    let mut updated_row = HashMap::new();
    updated_row.insert("id".to_string(), b"1".to_vec());
    updated_row.insert("email".to_string(), b"sam+updated@example.com".to_vec());
    updated_row.insert("nickname".to_string(), b"sam".to_vec());

    wal.append(
        "users",
        TransactionRecord::without_payload(
            TransactionId(4),
            None,
            Some(TransactionId(1)),
            4,
            actor.clone(),
            TransactionKind::Delete,
        ),
    )
    .expect("delete old version should append");

    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(5),
            None,
            Some(TransactionId(1)),
            5,
            actor.clone(),
            TransactionKind::Update,
            encode_row_payload(&schema, &updated_row).expect("updated row should encode"),
        ),
    )
    .expect("updated version should append");

    assert_eq!(load_live_row_count(&wal, "users"), 1);

    wal.append(
        "users",
        TransactionRecord::without_payload(
            TransactionId(6),
            None,
            Some(TransactionId(5)),
            6,
            actor,
            TransactionKind::Delete,
        ),
    )
    .expect("delete latest version should append");

    assert_eq!(load_live_row_count(&wal, "users"), 0);

}

#[test]
fn runtime_index_bootstrap_uses_latest_live_row_keys() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");

    let actor = UserId::from_username("test-user");
    let original_row = HashMap::from([
        ("id".to_string(), b"1".to_vec()),
        ("email".to_string(), b"sam@example.com".to_vec()),
    ]);
    let updated_row = HashMap::from([
        ("id".to_string(), b"1".to_vec()),
        ("email".to_string(), b"sam+updated@example.com".to_vec()),
    ]);

    for record in [
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&schema, &original_row).expect("original row should encode"),
        ),
        TransactionRecord::without_payload(
            TransactionId(2),
            None,
            Some(TransactionId(1)),
            2,
            actor.clone(),
            TransactionKind::Delete,
        ),
        TransactionRecord::with_payload(
            TransactionId(3),
            None,
            Some(TransactionId(1)),
            3,
            actor,
            TransactionKind::Update,
            encode_row_payload(&schema, &updated_row).expect("updated row should encode"),
        ),
    ] {
        wal.append("users", record)
            .expect("wal append should succeed");
    }

    let mut catalogs = HashMap::new();
    catalogs.insert(catalog.database_id.0.clone(), catalog.clone());

    let mut runtime_indexes = RuntimeIndexStore::new();
    runtime_indexes.bootstrap_from_catalogs(&catalogs, &wal);

    let table = catalog.table("users").expect("users table should exist");
    let pk_index = table
        .indexes
        .values()
        .find(|index| index.is_primary_key())
        .expect("primary key index should exist");
    let email_index = table
        .indexes
        .values()
        .find(|index| !index.is_primary_key())
        .expect("secondary index should exist");

    let stored_pk = convert_value_to_field_type(
        b"1",
        &FieldType::UInt(64),
        TypeConversionPolicy::Safe,
    )
    .expect("pk value should encode");

    let table_stream_id = if wal.latest_transaction_id_if_loaded("users").is_some() {
        "users".to_string()
    } else {
        catalog
            .entity_wal_stream_id("users")
            .unwrap_or_else(|| "users".to_string())
    };

    assert!(runtime_indexes
        .index_for_table(&table_stream_id, &pk_index.index_id.0)
        .expect("pk runtime index should exist")
        .contains(&[stored_pk]));

    if let Some(email_runtime_index) = runtime_indexes
        .index_for_table(&table_stream_id, &email_index.index_id.0)
    {
        assert!(email_runtime_index.contains(&[b"sam+updated@example.com".to_vec()]));
        assert!(!email_runtime_index.contains(&[b"sam@example.com".to_vec()]));
    }

}

#[test]
fn load_live_rows_ignores_uncommitted_write_group() {

    let wal = ConcurrentWalManager::in_memory();
    let schema = table_schema(vec![("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false)]);
    let actor = UserId::from_username("test-user");
    let group_id = TransactionId(1);

    wal.append(
        "users",
        TransactionRecord::without_payload(
            group_id,
            Some(group_id),
            None,
            1,
            actor.clone(),
            TransactionKind::WriteBegin,
        ),
    )
    .expect("write begin should append");

    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(2),
            Some(group_id),
            None,
            2,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&schema, &HashMap::from([("id".to_string(), b"1".to_vec())]))
                .expect("row should encode"),
        ),
    )
    .expect("grouped insert should append");

    assert!(load_live_rows(&wal, "users", "users", &schema).is_empty());

}

#[test]
fn load_live_rows_applies_committed_write_group() {

    let wal = ConcurrentWalManager::in_memory();
    let schema = table_schema(vec![("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false)]);
    let actor = UserId::from_username("test-user");
    let group_id = TransactionId(1);

    for record in [
        TransactionRecord::without_payload(
            group_id,
            Some(group_id),
            None,
            1,
            actor.clone(),
            TransactionKind::WriteBegin,
        ),
        TransactionRecord::with_payload(
            TransactionId(2),
            Some(group_id),
            None,
            2,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&schema, &HashMap::from([("id".to_string(), b"1".to_vec())]))
                .expect("row should encode"),
        ),
        TransactionRecord::without_payload(
            TransactionId(3),
            Some(group_id),
            Some(TransactionId(2)),
            3,
            actor,
            TransactionKind::WriteCommit,
        ),
    ] {
        wal.append("users", record).expect("record should append");
    }

    let rows = load_live_rows(&wal, "users", "users", &schema);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 2);

}

#[test]
fn materialize_relation_rows_supports_full_scan_and_equality_probe() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog = DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");
    let runtime_indexes = RuntimeIndexStore::new();

    let full_scan = materialize_relation_rows(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::FullScan,
        },
    );
    assert_eq!(full_scan.len(), 1);

    let equality_probe = materialize_relation_rows(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::TemporaryIndex,
                equality_filters: HashMap::from([(
                    "email".to_string(),
                    b"sam@example.com".to_vec(),
                )]),
            },
        },
    );
    assert_eq!(equality_probe.len(), 1);
    assert_eq!(equality_probe[0].0, 1);

}

#[test]
fn materialize_relation_rows_returns_empty_when_runtime_lookup_key_misses() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let filters = HashMap::from([("id".to_string(), b"1".to_vec())]);
    let (index, _) =
        choose_index_lookup(&table, &filters).expect("an index lookup should be selected");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let table_stream_id = if wal.latest_transaction_id_if_loaded(&table.entity_id).is_some() {
        table.entity_id.clone()
    } else {
        table.table_id.clone()
    };
    runtime_indexes
        .index_mut_for_table(&table_stream_id, &index.index_id.0)
        .insert(vec![b"999".to_vec()]);

    let rows = materialize_relation_rows(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::RuntimeIndexLookup {
                index_id: index.index_id.0.clone(),
                lookup_key: vec![b"1".to_vec()],
            },
        },
    );

    assert!(rows.is_empty());
}

#[test]
fn non_unique_numeric_equality_miss_returns_empty_from_index() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = table_schema(vec![
        ("id", 1, FieldType::Int(32), FieldIndex::PrimaryKey, false),
        ("id_parent", 2, FieldType::Int(32), FieldIndex::Indexed, true),
    ]);
    catalog
        .register_table("regions", schema.clone())
        .expect("regions table should register");

    let actor = UserId("test-user".to_string());
    for (transaction_id, id, parent_id) in [(1, 233, 254), (2, 147, 254)] {
        let row = HashMap::from([
            ("id".to_string(), id.to_string().into_bytes()),
            ("id_parent".to_string(), parent_id.to_string().into_bytes()),
        ]);
        wal.append(
            "regions",
            TransactionRecord::with_payload(
                TransactionId(transaction_id),
                None,
                None,
                transaction_id,
                actor.clone(),
                TransactionKind::Insert,
                encode_row_payload(&schema, &row).expect("row should encode"),
            ),
        )
        .expect("row should append");
    }

    let table = catalog.table("regions").expect("regions table should exist");
    let id_parent_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["id_parent".to_string()])
        .cloned()
        .expect("id_parent index should exist");
    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &id_parent_index.index_id.0);
    state.index = Some(id_parent_index.clone());
    state.insert_with_row_ref(vec![b"252".to_vec()], Some(99));

    let rows = materialize_relation_rows(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "id_parent".to_string(),
                lookup_value: b"254".to_vec(),
                source: EqualityProbeSource::ExistingIndex,
                equality_filters: HashMap::from([("id_parent".to_string(), b"254".to_vec())]),
            },
        },
    );

    assert!(rows.is_empty());
}

#[test]
fn range_intersection_probe_uses_ordered_runtime_index_scan() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = table_schema(vec![
        ("id", 1, FieldType::Int(32), FieldIndex::PrimaryKey, false),
        ("longitude", 2, FieldType::Float(64), FieldIndex::Indexed, true),
        ("latitude", 3, FieldType::Float(64), FieldIndex::Indexed, true),
    ]);

    catalog
        .register_table("places", schema.clone())
        .expect("places table should register");

    // Text ordering would place "50.9375" and "-6.9" inside a "6.93".."6.97" window.
    let seeded = [
        (1, "6.9603", "50.9375"),
        (2, "50.9375", "6.9603"),
        (3, "-6.9500", "50.9375"),
        (4, "6.9500", "50.9375"),
        (5, "6.9900", "50.9375"),
    ];

    let actor = UserId("test-user".to_string());

    for (transaction_id, id, longitude, latitude) in seeded
        .iter()
        .enumerate()
        .map(|(index, (id, longitude, latitude))| (index as u64 + 1, id, longitude, latitude))
    {
        let row = HashMap::from([
            ("id".to_string(), id.to_string().into_bytes()),
            ("longitude".to_string(), longitude.as_bytes().to_vec()),
            ("latitude".to_string(), latitude.as_bytes().to_vec()),
        ]);

        wal.append(
            "places",
            TransactionRecord::with_payload(
                TransactionId(transaction_id),
                None,
                None,
                transaction_id,
                actor.clone(),
                TransactionKind::Insert,
                encode_row_payload(&schema, &row).expect("row should encode"),
            ),
        )
        .expect("row should append");
    }

    let table = catalog.table("places").expect("places table should exist");

    let longitude_index = table
        .indexes
        .values()
        .find(|index| index.field_names == vec!["longitude".to_string()])
        .cloned()
        .expect("longitude index should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table.table_id, &longitude_index.index_id.0);
    state.index = Some(longitude_index.clone());
    state.set_numeric_kind(Some(
        crate::engine::database::indexing::runtime_index_key_codec::RuntimeIndexNumericKind::Float,
    ));

    assert!(
        state.supports_ordered_range_scan(),
        "a float index should keep an ordered key set",
    );

    for (index, (_, longitude, _)) in seeded.iter().enumerate() {
        state.insert_with_row_ref(vec![longitude.as_bytes().to_vec()], Some(index as u64));
    }

    let filters = vec![
        RangeFilterBounds {
            field_name: "longitude".to_string(),
            lower_bound: Some(RangeBound { value: b"6.93".to_vec(), inclusive: false }),
            upper_bound: Some(RangeBound { value: b"6.97".to_vec(), inclusive: false }),
        },
        RangeFilterBounds {
            field_name: "latitude".to_string(),
            lower_bound: Some(RangeBound { value: b"50.91".to_vec(), inclusive: false }),
            upper_bound: Some(RangeBound { value: b"50.95".to_vec(), inclusive: false }),
        },
    ];

    let rows = materialize_relation_rows(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::RangeIntersectionProbe { filters },
        },
    );

    let mut matched_ids = rows
        .iter()
        .filter_map(|(_, row_map)| row_map.get("id").cloned())
        .map(|id| String::from_utf8(id).expect("id should be utf8"))
        .collect::<Vec<_>>();

    matched_ids.sort();

    assert_eq!(matched_ids, vec!["1".to_string(), "4".to_string()]);

}

#[test]
fn materialize_relation_rows_falls_back_to_scan_when_runtime_lookup_state_missing() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let filters = HashMap::from([("id".to_string(), b"1".to_vec())]);
    let (index, _) =
        choose_index_lookup(&table, &filters).expect("an index lookup should be selected");

    let runtime_indexes = RuntimeIndexStore::new();

    let rows = materialize_relation_rows(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::RuntimeIndexLookup {
                index_id: index.index_id.0.clone(),
                lookup_key: vec![b"1".to_vec()],
            },
        },
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
    
}

#[test]
fn load_live_rows_via_primary_key_limit_uses_runtime_row_refs() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");
    let table_stream_id = if wal.latest_transaction_id_if_loaded(&table.entity_id).is_some() {
        table.entity_id.clone()
    } else {
        table.table_id.clone()
    };

    let pk_index = crate::primary_key_index(&table).expect("primary key should exist").clone();

    let mut runtime_indexes = RuntimeIndexStore::new();
    let state = runtime_indexes.index_mut_for_table(&table_stream_id, &pk_index.index_id.0);
    state.index = Some(pk_index);
    state.insert_with_row_ref(vec![b"1".to_vec()], Some(0));
    state.insert_with_row_ref(vec![b"2".to_vec()], Some(1));

    let rows = load_live_rows_via_primary_key_limit(
        &wal,
        &table,
        &table_stream_id,
        &schema,
        &runtime_indexes,
        1,
    )
    .expect("primary key limited load should use runtime row refs");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, 1);
}

#[test]
fn materialize_runtime_lookup_fallback_honors_row_limit_with_pk_cap() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let mut runtime_indexes = RuntimeIndexStore::new();
    let table_stream_id = if table.entity_id.is_empty() {
        table.table_id.clone()
    } else {
        table.entity_id.clone()
    };

    let id_index = table
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 && index.field_names[0] == "id"
        })
        .cloned()
        .expect("id index should exist");

    let state = runtime_indexes.index_mut_for_table(&table_stream_id, &id_index.index_id.0);
    state.index = Some(id_index.clone());
    state.insert_with_row_ref(vec![b"1".to_vec()], Some(1));
    state.insert_with_row_ref(vec![b"2".to_vec()], Some(2));

    // Lookup key shape intentionally does not match index key shape so the
    // runtime lookup path falls back to capped hydration.
    let rows = materialize_relation_rows_with_limit(
        &wal,
        &table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::RuntimeIndexLookup {
                index_id: id_index.index_id.0,
                lookup_key: vec![b"missing-a".to_vec(), b"missing-b".to_vec()],
            },
        },
        Some(1),
    );

    assert_eq!(rows.len(), 1);
}

#[test]
fn equality_probe_result_cache_ttl_treats_negative_one_as_permanent() {
    let entry = EqualityProbeCacheEntry {
        latest_tx_id: 1,
        cached_at: Instant::now() - Duration::from_secs(3600),
        rows: Vec::new(),
    };

    assert_eq!(equality_probe_result_cache_ttl_ms_from_config(-1), None);
    assert!(!equality_probe_cache_entry_is_expired(&entry, None));
}

#[test]
fn equality_probe_result_cache_ttl_expires_entries_after_deadline() {
    let entry = EqualityProbeCacheEntry {
        latest_tx_id: 1,
        cached_at: Instant::now() - Duration::from_millis(10),
        rows: Vec::new(),
    };

    assert!(equality_probe_cache_entry_is_expired(
        &entry,
        equality_probe_result_cache_ttl_ms_from_config(1),
    ));
}
