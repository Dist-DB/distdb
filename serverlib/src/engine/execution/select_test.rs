use super::*;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::engine::database::transaction::TransactionLog;
use crate::{
    encode_row_payload, parse_select_read_plan_from_statement, ConcurrentWalManager,
    DatabaseCatalog, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, SelectComparisonOp,
    SelectCondition, SelectPredicate, SelectProjectionItem, SelectRelation, TableSchema,
    TransactionId, TransactionKind, TransactionRecord, UserId,
};

fn table_schema(fields: Vec<(&str, u32, FieldType, FieldIndex, bool)>) -> TableSchema {
    TableSchema::new(
        fields
            .into_iter()
            .map(|(field_name, seqno, field_type, indexed, nullable)| FieldDef {
                seqno,
                field_name: field_name.to_string(),
                field_type,
                nullable,
                indexed,
                default_value: None,
                metadata: None,
            })
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
            groupid: None,
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
            groupid: None,
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
            groupid: None,
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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
        &mut |row_tuple, nested_condition| {
            row_matches_select_condition_result(
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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
        &mut |row_tuple, nested_condition| {
            row_matches_select_condition_result(
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
        op: SelectComparisonOp::Eq,
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
fn execute_relation_select_plan_applies_limit_and_offset() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement("select u.email from users u limit 1 offset 1")
        .expect("limited relation select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("limited relation select should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "alex@example.com"
    );
}

#[test]
fn execute_relation_select_plan_supports_exists_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where exists (select id from users where id = 1)",
    )
    .expect("exists select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("exists select should succeed");

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn execute_relation_select_plan_supports_not_exists_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where not exists (select uid from users where id = 999)",
    )
    .expect("not exists select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("not exists select should succeed");

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn execute_relation_select_plan_supports_correlated_exists_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where exists (select id from profiles p where p.user_id = u.id)",
    )
    .expect("correlated exists select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("correlated exists select should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
}

#[test]
fn execute_relation_select_plan_supports_correlated_in_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id in (select user_id from profiles p where p.user_id = u.id)",
    )
    .expect("correlated in select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("correlated in select should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
}

#[test]
fn execute_relation_select_plan_supports_scalar_subquery_comparisons() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id = (select id from users where email = 'sam@example.com')",
    )
    .expect("scalar subquery select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("scalar subquery select should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
}

#[test]
fn execute_relation_select_plan_rejects_multi_row_scalar_subqueries() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id = (select id from users)",
    )
    .expect("scalar subquery select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let err = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect_err("multi-row scalar subquery should fail");

    assert!(err.contains("scalar subquery returned more than one row"));
}

#[test]
fn execute_relation_select_plan_supports_any_subquery_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id = any ((select user_id from profiles p where p.user_id = u.id))",
    )
    .expect("any-subquery select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("any-subquery select should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
}

#[test]
fn execute_relation_select_plan_supports_all_subquery_predicates() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id > all ((select user_id from profiles where user_id = 99))",
    )
    .expect("all-subquery select should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("all-subquery select should succeed");

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn execute_joined_select_plan_expands_qualified_wildcard_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = SelectReadPlan {
        table_id: "users".to_string(),
        relations: vec![
            SelectRelation {
                table_id: "users".to_string(),
                alias: Some("u".to_string()),
            },
            SelectRelation {
                table_id: "profiles".to_string(),
                alias: Some("p".to_string()),
            },
        ],
        joins: vec![crate::SelectJoin {
            kind: crate::SelectJoinKind::Inner,
            relation: SelectRelation {
                table_id: "profiles".to_string(),
                alias: Some("p".to_string()),
            },
            on_condition: SelectCondition::Predicate(SelectPredicate::FieldComparison {
                left_field_name: "u.id".to_string(),
                op: SelectComparisonOp::Eq,
                right_field_name: "p.user_id".to_string(),
            }),
        }],
        pushdown_conditions: vec![None, None],
        projection: None,
        projection_items: vec![SelectProjectionItem::Wildcard {
            relation: Some("u".to_string()),
        }],
        projection_is_wildcard: false,
        limit: None,
        offset: None,
        where_condition: None,
        is_explain: false,
    };

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut |_function| Ok(None),
        &mut |_, _| Ok(true),
        &mut |_, _| Ok(true),
    )
    .expect("wildcard join projection should expand");

    assert_eq!(
        result.columns.iter().map(|column| column.field_name.clone()).collect::<Vec<_>>(),
        vec!["id".to_string(), "email".to_string()]
    );
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn execute_joined_select_plan_expands_unqualified_wildcard_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = SelectReadPlan {
        table_id: "users".to_string(),
        relations: vec![
            SelectRelation {
                table_id: "users".to_string(),
                alias: Some("u".to_string()),
            },
            SelectRelation {
                table_id: "profiles".to_string(),
                alias: Some("p".to_string()),
            },
        ],
        joins: vec![crate::SelectJoin {
            kind: crate::SelectJoinKind::Inner,
            relation: SelectRelation {
                table_id: "profiles".to_string(),
                alias: Some("p".to_string()),
            },
            on_condition: SelectCondition::Predicate(SelectPredicate::FieldComparison {
                left_field_name: "u.id".to_string(),
                op: SelectComparisonOp::Eq,
                right_field_name: "p.user_id".to_string(),
            }),
        }],
        pushdown_conditions: vec![None, None],
        projection: None,
        projection_items: vec![SelectProjectionItem::Wildcard { relation: None }],
        projection_is_wildcard: true,
        limit: None,
        offset: None,
        where_condition: None,
        is_explain: false,
    };

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut |_function| Ok(None),
        &mut |_, _| Ok(true),
        &mut |_, _| Ok(true),
    )
    .expect("unqualified wildcard join projection should expand");

    assert_eq!(
        result.columns.iter().map(|column| column.field_name.clone()).collect::<Vec<_>>(),
        vec![
            "id".to_string(),
            "email".to_string(),
            "id".to_string(),
            "user_id".to_string(),
            "name".to_string(),
        ]
    );
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0]
            .iter()
            .map(|value| String::from_utf8(value.clone()).expect("utf8"))
            .collect::<Vec<_>>(),
        vec!["1", "sam@example.com", "10", "1", "Sam"]
    );
}

#[test]
fn execute_relation_select_plan_supports_passthrough_derived_wrapper() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select * from (select email from users where id = 1) d",
    )
    .expect("passthrough derived wrapper select should parse");

    let relation = catalog
        .table(&read_plan.table_id)
        .expect("relation table should exist");
    let schema = catalog
        .table_schema(&read_plan.table_id)
        .expect("relation schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("passthrough derived wrapper select should execute");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
}

#[test]
fn execute_relation_select_plan_supports_passthrough_derived_wrapper_with_outer_where_and_window() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select * from (select id, email from users) d where d.id > 0 limit 1 offset 1",
    )
    .expect("passthrough derived wrapper with outer where/window should parse");

    let relation = catalog
        .table(&read_plan.table_id)
        .expect("relation table should exist");
    let schema = catalog
        .table_schema(&read_plan.table_id)
        .expect("relation schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut |_function| Ok(None),
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("passthrough derived wrapper with outer where/window should execute");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][1].clone()).expect("utf8"),
        "alex@example.com"
    );
}
