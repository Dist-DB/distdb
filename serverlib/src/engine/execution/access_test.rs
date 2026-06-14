
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
            TransactionRecord {
                id: TransactionId(tx_id),
                refid: None,
                timestamp_epoch_ms: tx_id,
                actor: actor.clone(),
                kind: TransactionKind::Insert,
                payload: encode_row_payload(&schema, &row).expect("row should encode"),
            },
        )
        .expect("row should append");
    }

    let delete_record = TransactionRecord {
        id: TransactionId(3),
        refid: Some(TransactionId(2)),
        timestamp_epoch_ms: 3,
        actor,
        kind: TransactionKind::Delete,
        payload: Vec::new(),
    };

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
            row_map: HashMap::from([("id".to_string(), b"1".to_vec())]),
        },
        MaterializedRelationRow {
            row_id: 2,
            row_map: HashMap::from([("id".to_string(), b"1".to_vec())]),
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

    let equality_plan = plan_relation_access(table, false, filters.clone());
    assert!(matches!(
        equality_plan.strategy,
        RelationAccessStrategy::EqualityProbe { .. }
    ));

    let full_scan_plan = plan_relation_access(table, false, HashMap::new());
    assert!(matches!(
        full_scan_plan.strategy,
        RelationAccessStrategy::FullScan
    ));

    let short_circuit_plan = plan_relation_access(table, true, filters);
    assert!(matches!(
        short_circuit_plan.strategy,
        RelationAccessStrategy::RuntimeIndexLookup { .. }
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
fn materialize_relation_rows_supports_full_scan_and_equality_probe() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
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
fn materialize_relation_rows_short_circuits_when_runtime_lookup_misses() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let schema = seed_users_table(&mut catalog, &wal);
    let table = catalog.table("users").expect("users table should exist");

    let filters = HashMap::from([("id".to_string(), b"1".to_vec())]);
    let (index, _) =
        choose_index_lookup(table, &filters).expect("an index lookup should be selected");

    let mut runtime_indexes = RuntimeIndexStore::new();
    runtime_indexes
        .index_mut(&index.index_id.0)
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
