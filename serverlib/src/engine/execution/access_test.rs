use std::sync::Arc;
use std::path::PathBuf;
use std::fs;
use std::time::{Duration, Instant};


use super::*;
use crate::engine::database::runtime_index_snapshot::RuntimeIndexSnapshotService;
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
    state.insert_with_row_ref(vec![b"1".to_vec()], Some(1));
    state.insert_with_row_ref(vec![b"2".to_vec()], Some(2));

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
