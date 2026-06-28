
use super::*;
use crate::render_stored_field_value;

use crate::engine::database::transaction::TransactionLog;
use crate::{
    encode_row_payload, ConcurrentWalManager, DatabaseCatalog, FieldDef, FieldIndex, FieldType,
    RuntimeIndexStore, SelectComparisonOp, SelectCondition, SelectJoin, SelectJoinKind,
    SelectPredicate, SelectRelation, TableSchema, TransactionId, TransactionKind,
    TransactionRecord, UserId,
};

fn relation(table_id: &str, alias: &str) -> SelectRelation {
    SelectRelation {
        table_id: table_id.to_string(),
        alias: Some(alias.to_string()),
    }
}

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
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&users_schema, &user_row).expect("user row should encode"),
        ),
    )
    .expect("user row should append");

    let mut other_user_row = std::collections::HashMap::new();
    other_user_row.insert("id".to_string(), b"2".to_vec());
    other_user_row.insert("email".to_string(), b"alex@example.com".to_vec());
    wal.append(
        "users",
        TransactionRecord::with_payload(
            TransactionId(2),
            None,
            None,
            2,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&users_schema, &other_user_row)
                .expect("user row should encode"),
        ),
    )
    .expect("user row should append");

    for (transaction_id, profile_id, user_id, name) in [
        (10, b"10".as_slice(), b"1".as_slice(), b"Sam".as_slice()),
        (11, b"11".as_slice(), b"1".as_slice(), b"Samuel".as_slice()),
        (12, b"12".as_slice(), b"3".as_slice(), b"Ghost".as_slice()),
    ] {
        let mut profile_row = std::collections::HashMap::new();
        profile_row.insert("id".to_string(), profile_id.to_vec());
        profile_row.insert("user_id".to_string(), user_id.to_vec());
        profile_row.insert("name".to_string(), name.to_vec());

        wal.append(
            "profiles",
            TransactionRecord::with_payload(
                TransactionId(transaction_id),
                None,
                None,
                transaction_id,
                actor.clone(),
                TransactionKind::Insert,
                encode_row_payload(&profiles_schema, &profile_row)
                    .expect("profile row should encode"),
            ),
        )
        .expect("profile row should append");
    }
}

fn join_condition() -> SelectCondition {
    SelectCondition::Predicate(SelectPredicate::FieldComparison {
        left_field_name: "u.id".to_string(),
        op: SelectComparisonOp::Eq,
        right_field_name: "p.user_id".to_string(),
    })
}

#[test]
fn build_joined_row_tuples_supports_all_join_kinds() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);
    let runtime_indexes = RuntimeIndexStore::new();

    let relations = vec![relation("users", "u")];
    let join = SelectJoin {
        kind: SelectJoinKind::Inner,
        relation: relation("profiles", "p"),
        on_condition: join_condition(),
    };

    let inner_rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        std::slice::from_ref(&join),
        &mut |_, _| Ok(true),
    )
    .expect("inner join should succeed");

    assert_eq!(inner_rows.len(), 2);
    assert!(inner_rows
        .iter()
        .all(|row| row.value("u.id").is_some() && row.value("p.user_id").is_some()));

    let left_rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &[SelectJoin {
            kind: SelectJoinKind::Left,
            ..join.clone()
        }],
        &mut |_, _| Ok(true),
    )
    .expect("left join should succeed");

    assert_eq!(left_rows.len(), 3);
    assert!(left_rows.iter().any(|row| {
        row.value("u.id")
            .map(|value| render_stored_field_value(value))
            == Some(b"2".to_vec())
            && row.value("p.name").is_none()
    }));

    let right_rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &[SelectJoin {
            kind: SelectJoinKind::Right,
            ..join.clone()
        }],
        &mut |_, _| Ok(true),
    )
    .expect("right join should succeed");

    assert_eq!(right_rows.len(), 3);
    assert!(right_rows.iter().any(|row| {
        row.value("u.id").is_none() && row.value("p.name") == Some(&b"Ghost".to_vec())
    }));

    let full_rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &[SelectJoin {
            kind: SelectJoinKind::Full,
            ..join
        }],
        &mut |_, _| Ok(true),
    )
    .expect("full join should succeed");

    assert_eq!(full_rows.len(), 4);
    assert!(full_rows
        .iter()
        .any(|row| { row.value("u.id").is_some() && row.value("p.name").is_none() }));
    assert!(full_rows.iter().any(|row| {
        row.value("u.id").is_none() && row.value("p.name") == Some(&b"Ghost".to_vec())
    }));
}

#[test]
fn build_joined_row_tuples_supports_non_field_comparison_join_conditions() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);
    let runtime_indexes = RuntimeIndexStore::new();

    let relations = vec![relation("users", "u")];
    let join = SelectJoin {
        kind: SelectJoinKind::Inner,
        relation: relation("profiles", "p"),
        on_condition: SelectCondition::Predicate(SelectPredicate::Comparison {
            field_name: "u.id".to_string(),
            op: SelectComparisonOp::Eq,
            value: b"1".to_vec(),
        }),
    };

    let rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &[join],
        &mut |_, _| Ok(true),
    )
    .expect("non-field-comparison join ON condition should be supported");

    assert_eq!(rows.len(), 3);
    assert!(rows.iter().all(|row| {
        row.value("u.id")
            .map(|value| render_stored_field_value(value))
            == Some(b"1".to_vec())
    }));
}

#[test]
fn build_joined_row_tuples_returns_empty_when_no_relations_exist() {
    let wal = ConcurrentWalManager::in_memory();
    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let runtime_indexes = RuntimeIndexStore::new();

    let rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &[],
        &[],
        &[],
        &mut |_, _| Ok(true),
    )
    .expect("empty relation list should succeed");

    assert!(rows.is_empty());
}

#[test]
fn build_joined_row_tuples_supports_cross_join() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);
    let runtime_indexes = RuntimeIndexStore::new();

    let relations = vec![relation("users", "u")];
    let join = SelectJoin {
        kind: SelectJoinKind::Cross,
        relation: relation("profiles", "p"),
        on_condition: SelectCondition::And(Vec::new()),
    };

    let rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &[join],
        &mut |_, _| Ok(true),
    )
    .expect("cross join should succeed");

    assert_eq!(rows.len(), 6);
    assert!(rows
        .iter()
        .all(|row| row.value("u.id").is_some() && row.value("p.id").is_some()));
}

#[test]
fn build_joined_row_tuples_supports_and_field_comparison_join_conditions() {
    let wal = ConcurrentWalManager::in_memory();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);
    let runtime_indexes = RuntimeIndexStore::new();

    let relations = vec![relation("users", "u")];
    let join = SelectJoin {
        kind: SelectJoinKind::Inner,
        relation: relation("profiles", "p"),
        on_condition: SelectCondition::And(vec![
            SelectCondition::Predicate(SelectPredicate::FieldComparison {
                left_field_name: "u.id".to_string(),
                op: SelectComparisonOp::Eq,
                right_field_name: "p.user_id".to_string(),
            }),
            SelectCondition::Predicate(SelectPredicate::FieldComparison {
                left_field_name: "p.user_id".to_string(),
                op: SelectComparisonOp::Eq,
                right_field_name: "p.user_id".to_string(),
            }),
        ]),
    };

    let rows = build_joined_row_tuples(
        &catalog,
        &wal,
        &runtime_indexes,
        &relations,
        &[None, None],
        &[join],
        &mut |_, _| Ok(true),
    )
    .expect("and field comparison join condition should succeed");

    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.value("u.id")
            .map(|value| render_stored_field_value(value))
            == Some(b"1".to_vec())
    }));
}
