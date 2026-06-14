use super::*;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::engine::database::transaction::TransactionLog;
use crate::{
    encode_row_payload, parse_select_read_plan_from_statement, ConcurrentWalManager,
    DatabaseCatalog, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, SelectCondition,
    SelectPredicate, SelectRelation, TableSchema, TransactionId, TransactionKind,
    TransactionRecord, UserId,
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

    let mut other_user_row = std::collections::HashMap::new();
    other_user_row.insert("id".to_string(), b"2".to_vec());
    other_user_row.insert("email".to_string(), b"alex@example.com".to_vec());
    wal.append(
        "users",
        TransactionRecord {
            id: TransactionId(2),
            refid: None,
            timestamp_epoch_ms: 2,
            actor: actor.clone(),
            kind: TransactionKind::Insert,
            payload: encode_row_payload(&users_schema, &other_user_row)
                .expect("user row should encode"),
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
            actor,
            kind: TransactionKind::Insert,
            payload: encode_row_payload(&profiles_schema, &profile_row)
                .expect("profile row should encode"),
        },
    )
    .expect("profile row should append");
}

#[test]
fn execute_joined_select_plan_projects_null_extended_rows() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
            "select u.email, p.name, concat('join', '!') from users u left join profiles p on u.id = p.user_id",
        )
        .expect("join plan should parse");

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut |function| evaluate_inbuilt_sql_function(function),
        &mut |row_map, nested_condition| {
            row_matches_select_condition(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
        &mut |row_tuple, nested_condition| {
            row_matches_select_condition(
                row_tuple,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("joined select should succeed");

    assert_eq!(result.columns.len(), 3);
    assert!(!result.columns[0].nullable);
    assert!(result.columns[1].nullable);
    assert!(result.columns[2].nullable);

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email should be utf8"),
                String::from_utf8(row[1].clone()).expect("name should be utf8"),
                String::from_utf8(row[2].clone()).expect("function output should be utf8"),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            (
                "alex@example.com".to_string(),
                "NULL".to_string(),
                "join!".to_string()
            ),
            (
                "sam@example.com".to_string(),
                "Sam".to_string(),
                "join!".to_string()
            ),
        ]
    );
}

#[test]
fn execute_projection_only_select_plan_returns_inbuilt_row() {
    let read_plan = parse_select_read_plan_from_statement("select concat('sa', 'm')")
        .expect("projection-only plan should parse");

    let result =
        execute_projection_only_select_plan(&read_plan, &mut |_function| Ok(Some(b"sam".to_vec())))
            .expect("projection-only select should succeed");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows, vec![vec![b"sam".to_vec()]]);
}

#[test]
fn execute_joined_select_plan_supports_inbuilt_function_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
            "select u.email, concat('join', '!') from users u inner join profiles p on u.id = p.user_id",
        )
        .expect("join plan should parse");

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut |function| evaluate_inbuilt_sql_function(function),
        &mut |row_map, nested_condition| {
            row_matches_select_condition(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
        &mut |row_tuple, nested_condition| {
            row_matches_select_condition(
                row_tuple,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("joined select should succeed");

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.rows.len(), 1);

    let mut outputs = result
        .rows
        .iter()
        .map(|row| String::from_utf8(row[1].clone()).expect("function output should be utf8"))
        .collect::<Vec<_>>();

    outputs.sort();

    assert_eq!(outputs, vec!["join!".to_string()]);
}

#[test]
fn row_matches_select_condition_supports_simple_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let row = std::collections::HashMap::from([
        ("id".to_string(), b"1".to_vec()),
        ("email".to_string(), b"sam@example.com".to_vec()),
    ]);

    let condition = SelectCondition::Predicate(SelectPredicate::Comparison {
        field_name: "email".to_string(),
        op: crate::SelectComparisonOp::Eq,
        value: b"sam@example.com".to_vec(),
    });

    assert!(row_matches_select_condition(
        &row,
        Some(&condition),
        &catalog,
        &wal,
        &runtime_indexes,
    ));
}

#[test]
fn execute_joined_select_plan_rejects_wildcard_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let read_plan = SelectReadPlan {
        table_id: "users".to_string(),
        relations: vec![SelectRelation {
            table_id: "users".to_string(),
            alias: Some("u".to_string()),
        }],
        joins: vec![crate::SelectJoin {
            kind: crate::SelectJoinKind::Inner,
            relation: SelectRelation {
                table_id: "profiles".to_string(),
                alias: Some("p".to_string()),
            },
            on_condition: SelectCondition::Predicate(SelectPredicate::FieldComparison {
                left_field_name: "u.id".to_string(),
                op: crate::SelectComparisonOp::Eq,
                right_field_name: "p.user_id".to_string(),
            }),
        }],
        pushdown_conditions: vec![None, None],
        projection: None,
        projection_items: Vec::new(),
        projection_is_wildcard: true,
        where_condition: None,
        is_explain: false,
    };

    let err = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut |_function| Ok(None),
        &mut |_, _| true,
        &mut |_, _| true,
    )
    .expect_err("wildcard join projection should be rejected");

    assert!(err.contains("wildcard projection"));
}

#[test]
fn explain_joined_select_plan_result_lists_multiple_join_steps() {
    let read_plan = parse_select_read_plan_from_statement(
            "explain select u.email, p.name, t.label from users u inner join profiles p on u.id = p.user_id left join teams t on p.id = t.profile_id where u.id = 1",
        )
        .expect("joined explain plan should parse");

    let result = explain_joined_select_plan_result(&read_plan);

    assert_eq!(result.columns.len(), 5);
    assert_eq!(result.rows.len(), 3);

    let row_text = result
        .rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|column| String::from_utf8_lossy(column).to_string())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(row_text[0][0], "0");
    assert_eq!(row_text[0][1], "base");
    assert!(row_text[0][2].contains("users"));

    assert_eq!(row_text[1][0], "1");
    assert_eq!(row_text[1][1], "inner");
    assert!(row_text[1][2].contains("profiles"));
    assert!(row_text[1][3].contains("u.id = p.user_id"));

    assert_eq!(row_text[2][0], "2");
    assert_eq!(row_text[2][1], "left");
    assert!(row_text[2][2].contains("teams"));
    assert!(row_text[2][3].contains("p.id = t.profile_id"));
}
