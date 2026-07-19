use std::sync::Arc;


use super::*;
use crate::engine::database::transaction::TransactionLog;
use crate::{
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

    assert!(field_has_single_column_index(table, "id"));
    assert!(field_has_single_column_index(table, "email"));
    assert!(!field_has_single_column_index(table, "nickname"));
    assert_eq!(schema.fields.len(), 3);
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
        choose_index_lookup(table, &filters).expect("an index lookup should be selected");

    assert_eq!(lookup_key.len(), 1);
    assert!(lookup_key[0] == b"1".to_vec() || lookup_key[0] == b"sam@example.com".to_vec());
    assert!(!index.index_id.0.is_empty());

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

    let equality_plan = plan_relation_access(table, false, filters.clone(), None);
    assert!(matches!(
        equality_plan.strategy,
        RelationAccessStrategy::EqualityProbe { .. }
    ));

    let full_scan_plan = plan_relation_access(table, false, HashMap::new(), None);
    assert!(matches!(
        full_scan_plan.strategy,
        RelationAccessStrategy::FullScan
    ));

    let short_circuit_plan = plan_relation_access(table, true, filters, None);
    assert!(matches!(
        short_circuit_plan.strategy,
        RelationAccessStrategy::RuntimeIndexLookup { .. }
    ));
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
        table,
        false,
        HashMap::new(),
        Some(("email".to_string(), b"sam".to_vec(), false)),
    );

    assert!(matches!(
        prefix_plan.strategy,
        RelationAccessStrategy::PrefixLikeProbe { .. }
    ));
}

#[test]
fn load_live_rows_filters_deleted_records() {

    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);

    let rows = load_live_rows(&wal, "users", &schema);
    
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

    let rows = load_live_rows(&wal, "users", &schema);
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

    assert!(load_live_rows(&wal, "users", &schema).is_empty());

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
    
    assert!(runtime_indexes
        .index_for_table(&table_stream_id, &email_index.index_id.0)
        .expect("email runtime index should exist")
        .contains(&[b"sam+updated@example.com".to_vec()]));
    
    assert!(!runtime_indexes
        .index_for_table(&table_stream_id, &email_index.index_id.0)
        .expect("email runtime index should exist")
        .contains(&[b"sam@example.com".to_vec()]));

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

    assert!(load_live_rows(&wal, "users", &schema).is_empty());

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

    let rows = load_live_rows(&wal, "users", &schema);
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
        table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::FullScan,
        },
    );
    assert_eq!(full_scan.len(), 1);

    let equality_probe = materialize_relation_rows(
        &wal,
        table,
        &schema,
        &runtime_indexes,
        &RelationAccessPlan {
            strategy: RelationAccessStrategy::EqualityProbe {
                field_name: "email".to_string(),
                lookup_value: b"sam@example.com".to_vec(),
                source: EqualityProbeSource::TemporaryIndex,
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
        choose_index_lookup(table, &filters).expect("an index lookup should be selected");

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
        table,
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
        choose_index_lookup(table, &filters).expect("an index lookup should be selected");

    let runtime_indexes = RuntimeIndexStore::new();

    let rows = materialize_relation_rows(
        &wal,
        table,
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
