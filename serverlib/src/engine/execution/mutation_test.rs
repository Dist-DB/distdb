use super::mutation::select_mutation_target_rows;

use crate::{
    encode_row_payload, ConcurrentWalManager, DatabaseCatalog, FieldDef, FieldIndex, FieldType,
    RuntimeIndexStore, SelectComparisonOp, SelectCondition, SelectJoin, SelectJoinKind,
    SelectPredicate, SelectRelation, TableSchema, TransactionId, TransactionKind,
    TransactionRecord, UserId,
};

use crate::engine::database::transaction::TransactionLog;

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

fn seed_rows(catalog: &mut DatabaseCatalog, wal: &ConcurrentWalManager) {
    let users_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::None, false),
    ]);

    catalog
        .register_table("users", users_schema.clone())
        .expect("users table should register");

    let profiles_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        (
            "user_id",
            2,
            FieldType::UInt(64),
            FieldIndex::Indexed,
            false,
        ),
        ("name", 3, FieldType::Text, FieldIndex::None, false),
    ]);

    catalog
        .register_table("profiles", profiles_schema.clone())
        .expect("profiles table should register");

    let actor = UserId("test-user".to_string());

    let mut user_row = std::collections::HashMap::new();
    user_row.insert("id".to_string(), b"1".to_vec());
    user_row.insert("email".to_string(), b"sam@example.com".to_vec());

    wal.append(
        "users",
        TransactionRecord {
            id: TransactionId(1),
            refid: None,
            timestamp_epoch_ms: 1,
            actor: actor.clone(),
            kind: TransactionKind::Insert,
            payload: encode_row_payload(&users_schema, &user_row).expect("user row should encode"),
        },
    )
    .expect("user row should append");

    let mut profile_row = std::collections::HashMap::new();
    profile_row.insert("id".to_string(), b"10".to_vec());
    profile_row.insert("user_id".to_string(), b"1".to_vec());
    profile_row.insert("name".to_string(), b"Sam".to_vec());

    wal.append(
        "profiles",
        TransactionRecord {
            id: TransactionId(10),
            refid: None,
            timestamp_epoch_ms: 10,
            actor: actor.clone(),
            kind: TransactionKind::Insert,
            payload: encode_row_payload(&profiles_schema, &profile_row)
                .expect("profile row should encode"),
        },
    )
    .expect("profile row should append");

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"1".to_vec());
    second_profile_row.insert("name".to_string(), b"Samuel".to_vec());

    wal.append(
        "profiles",
        TransactionRecord {
            id: TransactionId(11),
            refid: None,
            timestamp_epoch_ms: 11,
            actor,
            kind: TransactionKind::Insert,
            payload: encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("profile row should encode"),
        },
    )
    .expect("profile row should append");

    let mut unmatched_profile_row = std::collections::HashMap::new();
    unmatched_profile_row.insert("id".to_string(), b"12".to_vec());
    unmatched_profile_row.insert("user_id".to_string(), b"3".to_vec());
    unmatched_profile_row.insert("name".to_string(), b"Ghost".to_vec());

    wal.append(
        "profiles",
        TransactionRecord {
            id: TransactionId(12),
            refid: None,
            timestamp_epoch_ms: 12,
            actor: UserId("test-user".to_string()),
            kind: TransactionKind::Insert,
            payload: encode_row_payload(&profiles_schema, &unmatched_profile_row)
                .expect("profile row should encode"),
        },
    )
    .expect("profile row should append");
}

#[test]
fn select_mutation_target_rows_deduplicates_join_matches_by_base_row_id() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let relations = vec![SelectRelation {
        table_id: "users".to_string(),
        alias: Some("u".to_string()),
    }];
    let joins = vec![SelectJoin {
        kind: SelectJoinKind::Inner,
        relation: SelectRelation {
            table_id: "profiles".to_string(),
            alias: Some("p".to_string()),
        },
        on_condition: SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name: "u.id".to_string(),
            op: SelectComparisonOp::Eq,
            right_field_name: "p.user_id".to_string(),
        }),
    }];

    let rows = select_mutation_target_rows(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &joins,
        Some(&SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "u.id".to_string(),
            op: SelectComparisonOp::Eq,
            value: b"1".to_vec(),
        })),
        &mut |_, _| true,
    )
    .expect("mutation target selection should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id, 1);
    assert_eq!(
        rows[0].row_map.get("email"),
        Some(&b"sam@example.com".to_vec())
    );
}

#[test]
fn select_mutation_target_rows_skips_right_join_rows_without_base_relation() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let relations = vec![SelectRelation {
        table_id: "users".to_string(),
        alias: Some("u".to_string()),
    }];
    let joins = vec![SelectJoin {
        kind: SelectJoinKind::Right,
        relation: SelectRelation {
            table_id: "profiles".to_string(),
            alias: Some("p".to_string()),
        },
        on_condition: SelectCondition::Predicate(SelectPredicate::FieldComparison {
            left_field_name: "u.id".to_string(),
            op: SelectComparisonOp::Eq,
            right_field_name: "p.user_id".to_string(),
        }),
    }];

    let rows = select_mutation_target_rows(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &joins,
        Some(&SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "p.name".to_string(),
            op: SelectComparisonOp::Eq,
            value: b"Ghost".to_vec(),
        })),
        &mut |_, _| true,
    )
    .expect("mutation target selection should succeed");

    assert!(rows.is_empty());
}
