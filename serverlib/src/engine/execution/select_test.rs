use super::*;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::engine::sql::{
    evaluate_inbuilt_sql_function_with_lookup, with_lookup_sql_function_evaluator,
};
use crate::engine::database::transaction::TransactionLog;
use crate::{
    encode_row_payload, parse_select_read_plan_from_statement, ConcurrentWalManager,
    DatabaseCatalog, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, SelectComparisonOp,
    SelectCondition, SelectPredicate, SelectProjectionItem, SelectRelation, TableSchema,
    TransactionId, TransactionKind, TransactionRecord, UserId,
};

fn evaluate_inbuilt_for_test(function: &sqlparser::ast::Function) -> Result<Option<Vec<u8>>, String> {
    evaluate_inbuilt_sql_function(function)
}

fn evaluate_none_for_test(_: &sqlparser::ast::Function) -> Result<Option<Vec<u8>>, String> {
    Ok(None)
}

fn evaluate_sam_for_test(_: &sqlparser::ast::Function) -> Result<Option<Vec<u8>>, String> {
    Ok(Some(b"sam".to_vec()))
}

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

    let mut profile_row = std::collections::HashMap::new();
    profile_row.insert("id".to_string(), b"10".to_vec());
    profile_row.insert("user_id".to_string(), b"1".to_vec());
    profile_row.insert("name".to_string(), b"Sam".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(10),
            None,
            None,
            10,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &profile_row)
                .expect("profile row should encode"),
        ),
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
        &mut evaluate_inbuilt_for_test,
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
           execute_projection_only_select_plan(&read_plan, &mut evaluate_sam_for_test)
            .expect("projection-only select should succeed");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows, vec![vec![b"sam".to_vec()]]);
}

#[test]
fn execute_projection_only_select_plan_accepts_order_by_ordinal() {
    let read_plan = parse_select_read_plan_from_statement("select concat('sa', 'm') as value order by 1 desc")
        .expect("projection-only order by plan should parse");

    let result = execute_projection_only_select_plan(
        &read_plan,
        &mut evaluate_sam_for_test,
    )
    .expect("projection-only order by select should succeed");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "value");
    assert_eq!(result.rows, vec![vec![b"sam".to_vec()]]);
}

#[test]
fn execute_projection_only_select_plan_supports_row_independent_case_projection() {
    let read_plan = parse_select_read_plan_from_statement(
        "select case 1 when abs(-1) then upper('yes') else lower('NO') end as state",
    )
    .expect("projection-only CASE plan should parse");

    let result = execute_projection_only_select_plan(
           &read_plan,
           &mut evaluate_inbuilt_for_test,
    )
    .expect("projection-only CASE select should succeed");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "state");
    assert_eq!(result.rows, vec![vec![b"YES".to_vec()]]);
}

#[test]
fn execute_relation_select_plan_supports_count_star_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement("select count(*) from users")
        .expect("count select should parse");

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
            &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("count select should execute");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "count");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![b"2".to_vec()]);
}

#[test]
fn execute_relation_select_plan_count_star_materializes_rows_when_full_table() {

    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let users_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::None, false),
    ]);
    catalog
        .register_table("users", users_schema.clone())
        .expect("users table should register");

    // Build runtime indexes from seeded rows and execute count against the
    // same seeded WAL to validate row materialization semantics.
    let wal_seed = ConcurrentWalManager::in_memory();
    let actor = UserId("test-user".to_string());

    for i in 1..=3u64 {
        let mut row_map = std::collections::HashMap::new();
        row_map.insert("id".to_string(), i.to_string().into_bytes());
        row_map.insert("email".to_string(), format!("u{}@example.com", i).into_bytes());
        wal_seed
            .append(
                "users",
                TransactionRecord::with_payload(
                    TransactionId(i),
                    None,
                    None,
                    i,
                    actor.clone(),
                    TransactionKind::Insert,
                    encode_row_payload(&users_schema, &row_map)
                        .expect("row should encode"),
                ),
            )
            .expect("row should append");
    }

    let mut catalogs = std::collections::HashMap::new();
    catalogs.insert(catalog.database_id.0.clone(), catalog.clone());
    runtime_indexes.bootstrap_from_catalogs(&catalogs, &wal_seed);

    let read_plan = parse_select_read_plan_from_statement("select count(*) from users")
        .expect("count select should parse");

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
           &wal_seed,
           relation,
           schema,
           &runtime_indexes,
           &read_plan,
           &access_plan,
           &mut evaluate_none_for_test,
        &mut |_row_map, _nested_condition| Ok(true),
    )
    .expect("count select should execute from materialized relation rows");

    assert_eq!(result.rows, vec![vec![b"3".to_vec()]]);
    
}

#[test]
fn execute_relation_select_plan_count_star_falls_back_when_pk_cardinality_is_zero() {

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement("select count(*) from users")
        .expect("count select should parse");

    let relation = catalog
        .table(&read_plan.table_id)
        .expect("relation table should exist");
    let schema = catalog
        .table_schema(&read_plan.table_id)
        .expect("relation schema should exist");

    let pk_index = relation
        .indexes
        .values()
        .find(|index| index.is_primary_key())
        .cloned()
        .expect("primary key index should exist");

    // Simulate a stale bootstrap state: index metadata is present but contains no rows.
    runtime_indexes.register_index(pk_index);

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
            &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("count select should fall back to scanning rows");

    assert_eq!(result.rows, vec![vec![b"2".to_vec()]]);

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
            &mut evaluate_inbuilt_for_test,
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
fn execute_joined_select_plan_supports_row_dependent_inbuilt_function_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email, concat(u.email, '!') as tagged from users u",
    )
    .expect("relation function projection plan should parse");

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog.table_schema("users").expect("users schema should exist");
    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::FullScan,
    };

    let mut evaluator = with_lookup_sql_function_evaluator(|function, lookup| {
        evaluate_inbuilt_sql_function_with_lookup(function, lookup)
    });

    let result = execute_relation_select_plan(
            &wal,
            relation,
            schema,
            &runtime_indexes,
            &read_plan,
            &access_plan,
            &mut evaluator,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("row-dependent function projection should succeed");

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email utf8"),
                String::from_utf8(row[1].clone()).expect("tag utf8"),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            (
                "alex@example.com".to_string(),
                "alex@example.com!".to_string(),
            ),
            (
                "sam@example.com".to_string(),
                "sam@example.com!".to_string(),
            ),
        ]
    );
}

#[test]
fn execute_joined_select_plan_supports_complex_join_on_conditions() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email, p.name from users u inner join profiles p on u.id = p.user_id and p.name = 'Sam'",
    )
    .expect("complex join ON plan should parse");

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut evaluate_inbuilt_for_test,
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
    .expect("joined select with complex ON should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
    assert_eq!(
        String::from_utf8(result.rows[0][1].clone()).expect("utf8"),
        "Sam"
    );
}

#[test]
fn execute_joined_select_plan_supports_case_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email, case when p.name = 'Sam' then 'known' else 'unknown' end as bucket from users u left join profiles p on u.id = p.user_id",
    )
    .expect("join CASE projection plan should parse");

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut evaluate_inbuilt_for_test,
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
    .expect("joined select with CASE projection should succeed");

    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[1].field_name, "bucket");

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email utf8"),
                String::from_utf8(row[1].clone()).expect("bucket utf8"),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("alex@example.com".to_string(), "unknown".to_string()),
            ("sam@example.com".to_string(), "known".to_string()),
        ]
    );
}

#[test]
fn execute_joined_select_plan_supports_case_projection_function_values() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email, case when p.name = 'Sam' then upper('known') else lower('UNKNOWN') end as bucket from users u left join profiles p on u.id = p.user_id",
    )
    .expect("join CASE projection with function values should parse");

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut evaluate_inbuilt_for_test,
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
    .expect("joined select with function-valued CASE projection should succeed");

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email utf8"),
                String::from_utf8(row[1].clone()).expect("bucket utf8"),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("alex@example.com".to_string(), "unknown".to_string()),
            ("sam@example.com".to_string(), "KNOWN".to_string()),
        ]
    );
}

#[test]
fn execute_joined_select_plan_supports_case_projection_function_values_with_columns() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email, case when p.name = 'Sam' then concat(p.name, '!') else lower('UNKNOWN') end as bucket from users u left join profiles p on u.id = p.user_id",
    )
    .expect("join CASE projection with column-arg function values should parse");

    let mut evaluator = with_lookup_sql_function_evaluator(|function, lookup| {
        evaluate_inbuilt_sql_function_with_lookup(function, lookup)
    });

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut evaluator,
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
    .expect("joined select with column-arg function-valued CASE projection should succeed");

    let mut rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email utf8"),
                String::from_utf8(row[1].clone()).expect("bucket utf8"),
            )
        })
        .collect::<Vec<_>>();
    rows.sort();

    assert_eq!(
        rows,
        vec![
            ("alex@example.com".to_string(), "unknown".to_string()),
            ("sam@example.com".to_string(), "Sam!".to_string()),
        ]
    );
}

#[test]
fn execute_joined_select_plan_returns_explain_rows_when_requested() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "explain select u.email from users u inner join profiles p on u.id = p.user_id",
    )
    .expect("explain join plan should parse");

    let result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &read_plan,
        &mut evaluate_inbuilt_for_test,
        &mut |_, _| Ok(true),
        &mut |_, _| Ok(true),
    )
    .expect("explain join should succeed");

    assert_eq!(result.columns.len(), 8);
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.columns[5].field_name, "complexity_score");
    assert_eq!(result.columns[6].field_name, "execution_mode");
    assert_eq!(result.columns[7].field_name, "complexity_reasons");
    assert_eq!(result.rows[0][6], b"adaptive_materialize".to_vec());
    assert_eq!(result.rows[0][7], b"joins".to_vec());
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        "select u.email from users u where not exists (select id from users where id = 999)",
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
        &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("not exists select should succeed");

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn execute_relation_select_plan_supports_exists_predicates_with_inbuilt_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where exists (select concat('x', 'y') from users where id = 1)",
    )
    .expect("exists select with inbuilt projection should parse");

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
        &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("exists select with inbuilt projection should succeed");

    assert_eq!(result.rows.len(), 2);
}

#[test]
fn execute_relation_select_plan_supports_in_subquery_with_inbuilt_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id in (select abs(-1))",
    )
    .expect("in-subquery with inbuilt projection should parse");

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
        &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("in-subquery with inbuilt projection should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
}

#[test]
fn execute_relation_select_plan_supports_scalar_subquery_with_inbuilt_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where u.id = (select abs(-1))",
    )
    .expect("scalar subquery with inbuilt projection should parse");

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
        &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(row_map, nested_condition, &catalog, &wal, &runtime_indexes)
        },
    )
    .expect("scalar subquery with inbuilt projection should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        ctes: Vec::new(),
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
        distinct: false,
        order_by: Vec::new(),
        group_by: Vec::new(),
        having_condition: None,
        has_window_clause: false,
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
        &mut evaluate_none_for_test,
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
        ctes: Vec::new(),
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
        distinct: false,
        order_by: Vec::new(),
        group_by: Vec::new(),
        having_condition: None,
        has_window_clause: false,
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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
        &mut evaluate_none_for_test,
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

#[test]
fn execute_relation_select_plan_supports_passthrough_derived_wrapper_with_outer_projection_aliases() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select d.email as contact from (select id, email from users) d where d.id = 1",
    )
    .expect("passthrough derived wrapper with outer projection aliases should parse");

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
        &mut evaluate_none_for_test,
        &mut |row_map, nested_condition| {
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("passthrough derived wrapper with outer projection aliases should execute");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "contact");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "sam@example.com"
    );
}
