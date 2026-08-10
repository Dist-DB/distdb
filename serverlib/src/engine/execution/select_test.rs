use super::*;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::engine::sql::{
    evaluate_inbuilt_sql_function_with_lookup, with_lookup_sql_function_evaluator,
};
use crate::engine::database::transaction::TransactionLog;
use crate::{
    encode_row_payload, parse_select_read_plan_from_statement, ConcurrentWalManager,
    DatabaseCatalog, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, SelectComparisonOp,
    SelectCondition, SelectLockMode, SelectPredicate, SelectProjectionItem, SelectRelation, TableSchema,
    TransactionId, TransactionKind, TransactionRecord, UserId, render_stored_field_value,
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
fn execute_joined_select_plan_supports_count_star_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select count(*) from users u left join profiles p on u.id = p.user_id",
    )
    .expect("join count plan should parse");

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
    .expect("joined count select should succeed");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "count");
    assert_eq!(result.columns[0].field_type, FieldType::UInt(64));
    assert_eq!(result.rows, vec![vec![b"2".to_vec()]]);
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
fn execute_projection_only_select_plan_returns_newuuid_value() {
    let read_plan = parse_select_read_plan_from_statement("select newuuid()")
        .expect("projection-only plan should parse");

    let result =
        execute_projection_only_select_plan(&read_plan, &mut evaluate_inbuilt_for_test)
            .expect("projection-only select should succeed");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0].len(), 1);

    let value = String::from_utf8(result.rows[0][0].clone())
        .expect("newuuid output should be utf8");
    let parsed = common::Uuid::parse_str(&value)
        .expect("newuuid output should be a valid UUID");
    assert_eq!(parsed.to_string(), value);
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
    assert_eq!(result.columns[0].field_type, FieldType::UInt(64));
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![b"2".to_vec()]);
}

#[test]
fn execute_relation_select_plan_count_star_uses_live_row_count_when_full_table() {

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
        &mut |_row_map, _nested_condition| {
            panic!("strict full-table count(*) should not materialize rows")
        },
    )
    .expect("count select should execute from live row count");

    assert_eq!(result.columns[0].field_type, FieldType::UInt(64));
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

    assert_eq!(result.columns[0].field_type, FieldType::UInt(64));
}

#[test]
fn execute_relation_select_plan_count_star_equality_probe_uses_fast_path() {

    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select count(*) from users where id=1",
    )
    .expect("count equality select should parse");

    let relation = catalog
        .table(&read_plan.table_id)
        .expect("relation table should exist");
    let schema = catalog
        .table_schema(&read_plan.table_id)
        .expect("relation schema should exist");

    let mut equality_filters = std::collections::HashMap::new();
    let where_condition = read_plan
        .where_condition
        .as_ref()
        .expect("where condition should exist");

    assert!(crate::collect_indexable_equality_filters_for_schema(
        &schema,
        where_condition,
        &mut equality_filters,
    ));

    let lookup_value = equality_filters
        .get("id")
        .cloned()
        .expect("id equality filter should exist");

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "id".to_string(),
            lookup_value,
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters,
        },
    };

    let result = execute_relation_select_plan(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &read_plan,
        &access_plan,
        &mut evaluate_none_for_test,
        &mut |_row_map, _nested_condition| {
            panic!("equality count fast path should avoid row materialization")
        },
    )
    .expect("count equality select should execute");

    assert_eq!(result.rows, vec![vec![b"1".to_vec()]]);

}

#[test]
fn materialize_relation_rows_with_limit_bounds_equality_probe_results() {

    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let users_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("region", 2, FieldType::UInt(64), FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("users", users_schema.clone())
        .expect("users table should register");

    let actor = UserId("test-user".to_string());

    for i in 1..=10u64 {
        let mut row_map = std::collections::HashMap::new();
        row_map.insert("id".to_string(), i.to_string().into_bytes());
        row_map.insert("region".to_string(), b"5412".to_vec());

        wal.append(
            "users",
            TransactionRecord::with_payload(
                TransactionId(i),
                None,
                None,
                i,
                actor.clone(),
                TransactionKind::Insert,
                encode_row_payload(&users_schema, &row_map).expect("row should encode"),
            ),
        )
        .expect("row should append");
    }

    let relation = catalog.table("users").expect("users table should exist");
    let schema = catalog
        .table_schema("users")
        .expect("users schema should exist");

    let read_plan = parse_select_read_plan_from_statement(
        "select * from users where region=5412",
    )
    .expect("relation select should parse");

    let mut equality_filters = std::collections::HashMap::new();
    let where_condition = read_plan
        .where_condition
        .as_ref()
        .expect("where condition should exist");

    assert!(crate::collect_indexable_equality_filters_for_schema(
        &schema,
        where_condition,
        &mut equality_filters,
    ));

    let lookup_value = equality_filters
        .get("region")
        .cloned()
        .expect("region equality filter should exist");

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "region".to_string(),
            lookup_value,
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters,
        },
    };

    let rows = crate::materialize_relation_rows_with_limit(
        &wal,
        relation,
        schema,
        &runtime_indexes,
        &access_plan,
        Some(3),
    );

    assert_eq!(rows.len(), 3);

}

#[test]
fn equality_probe_uses_runtime_index_scope_when_relation_stream_has_no_rows() {

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-equality-scope-fallback-{}-{}",
        std::process::id(),
        common::epoch_nanos!(),
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema.clone())
        .expect("places table should register");

    let actor = UserId("test-user".to_string());
    let mut row_map = std::collections::HashMap::new();
    row_map.insert("uid".to_string(), b"1".to_vec());
    row_map.insert("display_name".to_string(), b"Cologne".to_vec());

    wal.append(
        "places",
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &row_map).expect("row should encode"),
        ),
    )
    .expect("row should append to legacy stream");

    let relation = catalog.table("places").expect("places table should exist");
    let schema = catalog
        .table_schema("places")
        .expect("places schema should exist");

    let display_name_index_id = relation
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 &&
            index.field_names[0] == "display_name"
        })
        .map(|index| index.index_id.0.clone())
        .expect("display_name index should exist");

    runtime_indexes
        .index_mut_for_table("places", &display_name_index_id)
        .insert(vec![b"Cologne".to_vec()]);

    let mut relation_with_scoped_stream = relation;
    relation_with_scoped_stream.entity_id = "main:scoped:places".to_string();

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "display_name".to_string(),
            lookup_value: b"Cologne".to_vec(),
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters: std::collections::HashMap::from([(
                "display_name".to_string(),
                b"Cologne".to_vec(),
            )]),
        },
    };

    let rows = crate::materialize_relation_rows_with_limit(
        &wal,
        &relation_with_scoped_stream,
        &schema,
        &runtime_indexes,
        &access_plan,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .1
            .get("display_name")
            .map(|value| render_stored_field_value(value)),
        Some(b"Cologne".to_vec())
    );

    let _ = std::fs::remove_dir_all(temp_root);

}

#[test]
fn equality_probe_falls_back_to_legacy_table_stream_when_scoped_stream_is_empty() {

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-equality-legacy-fallback-{}-{}",
        std::process::id(),
        common::epoch_nanos!(),
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema.clone())
        .expect("places table should register");

    let actor = UserId("test-user".to_string());
    let mut row_map = std::collections::HashMap::new();
    row_map.insert("uid".to_string(), b"1".to_vec());
    row_map.insert("display_name".to_string(), b"Cologne".to_vec());

    wal.append(
        "places",
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &row_map).expect("row should encode"),
        ),
    )
    .expect("row should append to legacy stream");

    let relation = catalog.table("places").expect("places table should exist");
    let schema = catalog
        .table_schema("places")
        .expect("places schema should exist");

    let display_name_index_id = relation
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 &&
            index.field_names[0] == "display_name"
        })
        .map(|index| index.index_id.0.clone())
        .expect("display_name index should exist");

    let mut relation_with_scoped_stream = relation;
    relation_with_scoped_stream.entity_id = "main:scoped:places".to_string();

    runtime_indexes
        .index_mut_for_table(
            relation_with_scoped_stream.entity_id.as_str(),
            &display_name_index_id,
        )
        .insert(vec![b"Cologne".to_vec()]);

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "display_name".to_string(),
            lookup_value: b"Cologne".to_vec(),
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters: std::collections::HashMap::from([(
                "display_name".to_string(),
                b"Cologne".to_vec(),
            )]),
        },
    };

    let rows = crate::materialize_relation_rows_with_limit(
        &wal,
        &relation_with_scoped_stream,
        &schema,
        &runtime_indexes,
        &access_plan,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .1
            .get("display_name")
            .map(|value| render_stored_field_value(value)),
        Some(b"Cologne".to_vec())
    );

    let _ = std::fs::remove_dir_all(temp_root);

}

#[test]
fn equality_probe_falls_back_to_legacy_table_stream_when_scoped_stream_has_only_schema_records() {

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-equality-legacy-fallback-schema-only-{}-{}",
        std::process::id(),
        common::epoch_nanos!(),
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema.clone())
        .expect("places table should register");

    let actor = UserId("test-user".to_string());
    let mut row_map = std::collections::HashMap::new();
    row_map.insert("uid".to_string(), b"1".to_vec());
    row_map.insert("display_name".to_string(), b"Cologne".to_vec());

    wal.append(
        "places",
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &row_map).expect("row should encode"),
        ),
    )
    .expect("row should append to legacy stream");

    let relation = catalog.table("places").expect("places table should exist");
    let schema = catalog
        .table_schema("places")
        .expect("places schema should exist");

    let display_name_index_id = relation
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 &&
            index.field_names[0] == "display_name"
        })
        .map(|index| index.index_id.0.clone())
        .expect("display_name index should exist");

    let mut relation_with_scoped_stream = relation;
    relation_with_scoped_stream.entity_id = "main:scoped:places".to_string();

    // Scoped stream contains only schema records, so latest tx id is non-zero
    // but there are still no row writes there.
    wal.append(
        relation_with_scoped_stream.entity_id.as_str(),
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor,
            TransactionKind::SchemaChange,
            b"schema-change".to_vec(),
        ),
    )
    .expect("schema record should append to scoped stream");

    runtime_indexes
        .index_mut_for_table(
            relation_with_scoped_stream.entity_id.as_str(),
            &display_name_index_id,
        )
        .insert(vec![b"Cologne".to_vec()]);

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "display_name".to_string(),
            lookup_value: b"Cologne".to_vec(),
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters: std::collections::HashMap::from([(
                "display_name".to_string(),
                b"Cologne".to_vec(),
            )]),
        },
    };

    let rows = crate::materialize_relation_rows_with_limit(
        &wal,
        &relation_with_scoped_stream,
        &schema,
        &runtime_indexes,
        &access_plan,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .1
            .get("display_name")
            .map(|value| render_stored_field_value(value)),
        Some(b"Cologne".to_vec())
    );

    let _ = std::fs::remove_dir_all(temp_root);

}

#[test]
fn equality_probe_retries_legacy_stream_when_scoped_probe_returns_empty() {

    let temp_root = std::env::temp_dir().join(format!(
        "distdb-equality-legacy-retry-empty-{}-{}",
        std::process::id(),
        common::epoch_nanos!(),
    ));
    std::fs::create_dir_all(&temp_root).expect("temp dir should be created");

    let wal = ConcurrentWalManager::with_data_dir(temp_root.clone());
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema.clone())
        .expect("places table should register");

    let relation = catalog.table("places").expect("places table should exist");
    let schema = catalog
        .table_schema("places")
        .expect("places schema should exist");

    let display_name_index_id = relation
        .indexes
        .values()
        .find(|index| {
            index.field_names.len() == 1 &&
            index.field_names[0] == "display_name"
        })
        .map(|index| index.index_id.0.clone())
        .expect("display_name index should exist");

    let mut relation_with_scoped_stream = relation;
    relation_with_scoped_stream.entity_id = "main:scoped:places".to_string();

    let actor = UserId("test-user".to_string());

    // Legacy stream has the matching Cologne row.
    let mut cologne_row = std::collections::HashMap::new();
    cologne_row.insert("uid".to_string(), b"1".to_vec());
    cologne_row.insert("display_name".to_string(), b"Cologne".to_vec());

    wal.append(
        "places",
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &cologne_row).expect("row should encode"),
        ),
    )
    .expect("row should append to legacy stream");

    // Scoped stream has data writes, but no Cologne match.
    let mut other_row = std::collections::HashMap::new();
    other_row.insert("uid".to_string(), b"2".to_vec());
    other_row.insert("display_name".to_string(), b"Berlin".to_vec());

    wal.append(
        relation_with_scoped_stream.entity_id.as_str(),
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &other_row).expect("row should encode"),
        ),
    )
    .expect("row should append to scoped stream");

    runtime_indexes
        .index_mut_for_table(
            relation_with_scoped_stream.entity_id.as_str(),
            &display_name_index_id,
        )
        .insert(vec![b"Cologne".to_vec()]);

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "display_name".to_string(),
            lookup_value: b"Cologne".to_vec(),
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters: std::collections::HashMap::from([(
                "display_name".to_string(),
                b"Cologne".to_vec(),
            )]),
        },
    };

    let rows = crate::materialize_relation_rows_with_limit(
        &wal,
        &relation_with_scoped_stream,
        &schema,
        &runtime_indexes,
        &access_plan,
        None,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .1
            .get("display_name")
            .map(|value| render_stored_field_value(value)),
        Some(b"Cologne".to_vec())
    );

    let _ = std::fs::remove_dir_all(temp_root);

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
fn execute_joined_select_plan_supports_qualify_filtering() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select email from users qualify id = 2",
    )
    .expect("qualify plan should parse");

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
    )
    .expect("qualify filtering should succeed");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        String::from_utf8(result.rows[0][0].clone()).expect("utf8"),
        "alex@example.com"
    );
}

#[test]
fn execute_relation_select_plan_supports_row_number_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select email, row_number() over (order by email desc) as rn from users order by email",
    )
    .expect("row_number select should parse");

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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("row_number window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("email utf8"),
                String::from_utf8(row[1].clone()).expect("rn utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("alex@example.com".to_string(), "2".to_string()),
            ("sam@example.com".to_string(), "1".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_partitioned_row_number_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());
    let mut extra_profile_row = std::collections::HashMap::new();
    extra_profile_row.insert("id".to_string(), b"11".to_vec());
    extra_profile_row.insert("user_id".to_string(), b"1".to_vec());
    extra_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &extra_profile_row)
                .expect("extra profile row should encode"),
        ),
    )
    .expect("extra profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, user_id, row_number() over (partition by user_id order by id desc) as rn from profiles order by id",
    )
    .expect("partitioned row_number select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("partitioned row_number window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("user_id utf8"),
                String::from_utf8(row[2].clone()).expect("rn utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "1".to_string(), "2".to_string()),
            ("11".to_string(), "1".to_string(), "1".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_named_window_reuse_with_frame_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles").expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());
    let mut extra_profile_row = std::collections::HashMap::new();
    extra_profile_row.insert("id".to_string(), b"11".to_vec());
    extra_profile_row.insert("user_id".to_string(), b"1".to_vec());
    extra_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &extra_profile_row)
                .expect("extra profile row should encode"),
        ),
    )
    .expect("extra profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, user_id, row_number() over (w rows between unbounded preceding and current row) as rn from profiles window w as (partition by user_id order by id desc) order by id",
    )
    .expect("named window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("named window row_number window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("user_id utf8"),
                String::from_utf8(row[2].clone()).expect("rn utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "1".to_string(), "2".to_string()),
            ("11".to_string(), "1".to_string(), "1".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_sum_over_named_window_with_frame_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());
    let mut extra_profile_row = std::collections::HashMap::new();
    extra_profile_row.insert("id".to_string(), b"11".to_vec());
    extra_profile_row.insert("user_id".to_string(), b"1".to_vec());
    extra_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &extra_profile_row)
                .expect("extra profile row should encode"),
        ),
    )
    .expect("extra profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, user_id, sum(id) over (w rows between unbounded preceding and current row) as running_sum from profiles window w as (partition by user_id order by id) order by id",
    )
    .expect("sum window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("sum window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("user_id utf8"),
                String::from_utf8(row[2].clone()).expect("running_sum utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "1".to_string(), "10".to_string()),
            ("11".to_string(), "1".to_string(), "21".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_named_window_partition_order_and_frame_overrides() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());
    let mut extra_profile_row = std::collections::HashMap::new();
    extra_profile_row.insert("id".to_string(), b"11".to_vec());
    extra_profile_row.insert("user_id".to_string(), b"1".to_vec());
    extra_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &extra_profile_row)
                .expect("extra profile row should encode"),
        ),
    )
    .expect("extra profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, user_id, row_number() over (w order by id) as rn_order_override, row_number() over (w partition by id order by id) as rn_partition_override, sum(id) over (w rows between unbounded preceding and current row) as running_sum_frame_override from profiles window w as (partition by user_id order by id desc rows between current row and current row) order by id",
    )
    .expect("named window override select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
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
    .expect("named window override projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("user_id utf8"),
                String::from_utf8(row[2].clone()).expect("order override utf8"),
                String::from_utf8(row[3].clone()).expect("partition override utf8"),
                String::from_utf8(row[4].clone()).expect("frame override utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            (
                "10".to_string(),
                "1".to_string(),
                "1".to_string(),
                "1".to_string(),
                "21".to_string(),
            ),
            (
                "11".to_string(),
                "1".to_string(),
                "2".to_string(),
                "1".to_string(),
                "11".to_string(),
            ),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_rank_and_dense_rank_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"2".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let mut third_profile_row = std::collections::HashMap::new();
    third_profile_row.insert("id".to_string(), b"12".to_vec());
    third_profile_row.insert("user_id".to_string(), b"2".to_vec());
    third_profile_row.insert("name".to_string(), b"Zed".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(12),
            None,
            None,
            12,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &third_profile_row)
                .expect("third profile row should encode"),
        ),
    )
    .expect("third profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, name, rank() over (order by name), dense_rank() over (order by name) from profiles order by id",
    )
    .expect("rank window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("rank window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("name utf8"),
                String::from_utf8(row[2].clone()).expect("rank utf8"),
                String::from_utf8(row[3].clone()).expect("dense_rank utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            (
                "10".to_string(),
                "Sam".to_string(),
                "1".to_string(),
                "1".to_string(),
            ),
            (
                "11".to_string(),
                "Sam".to_string(),
                "1".to_string(),
                "1".to_string(),
            ),
            (
                "12".to_string(),
                "Zed".to_string(),
                "3".to_string(),
                "2".to_string(),
            ),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_lag_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select id, lag(id, 1, 0) over (order by id) as prev_id from users order by id",
    )
    .expect("lag window select should parse");

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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("lag window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("lag utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "0".to_string()),
            ("2".to_string(), "1".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_lead_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select id, lead(id, 1, 99) over (order by id) as next_id from users order by id",
    )
    .expect("lead window select should parse");

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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("lead window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("lead utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("1".to_string(), "2".to_string()),
            ("2".to_string(), "99".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_avg_min_max_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"1".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, count(id) over (order by id rows between unbounded preceding and current row) as running_count, avg(id) over (order by id rows between unbounded preceding and current row) as running_avg, min(id) over (order by id rows between unbounded preceding and current row) as running_min, max(id) over (order by id rows between unbounded preceding and current row) as running_max from profiles order by id",
    )
    .expect("count/avg/min/max window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("count/avg/min/max window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("count utf8"),
                String::from_utf8(row[2].clone()).expect("avg utf8"),
                String::from_utf8(row[3].clone()).expect("min utf8"),
                String::from_utf8(row[4].clone()).expect("max utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            (
                "10".to_string(),
                "1".to_string(),
                "10".to_string(),
                "10".to_string(),
                "10".to_string(),
            ),
            (
                "11".to_string(),
                "2".to_string(),
                "10.5".to_string(),
                "10".to_string(),
                "11".to_string(),
            ),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_first_last_value_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"1".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, first_value(id) over (order by id rows between unbounded preceding and current row) as first_id, last_value(id) over (order by id rows between unbounded preceding and current row) as last_id from profiles order by id",
    )
    .expect("first/last value window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("first/last value window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("first value utf8"),
                String::from_utf8(row[2].clone()).expect("last value utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "10".to_string(), "10".to_string()),
            ("11".to_string(), "10".to_string(), "11".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_nth_value_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"1".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam Two".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, nth_value(id, 2) over (order by id rows between unbounded preceding and current row) as second_id from profiles order by id",
    )
    .expect("nth_value window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("nth_value window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("nth value utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "NULL".to_string()),
            ("11".to_string(), "11".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_percent_rank_and_cume_dist_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"2".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let mut third_profile_row = std::collections::HashMap::new();
    third_profile_row.insert("id".to_string(), b"12".to_vec());
    third_profile_row.insert("user_id".to_string(), b"2".to_vec());
    third_profile_row.insert("name".to_string(), b"Zed".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(12),
            None,
            None,
            12,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &third_profile_row)
                .expect("third profile row should encode"),
        ),
    )
    .expect("third profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, name, percent_rank() over (order by name), cume_dist() over (order by name) from profiles order by id",
    )
    .expect("percent_rank/cume_dist window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("percent_rank/cume_dist window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("name utf8"),
                String::from_utf8(row[2].clone()).expect("percent_rank utf8"),
                String::from_utf8(row[3].clone()).expect("cume_dist utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            (
                "10".to_string(),
                "Sam".to_string(),
                "0".to_string(),
                "0.6666666666666666".to_string(),
            ),
            (
                "11".to_string(),
                "Sam".to_string(),
                "0".to_string(),
                "0.6666666666666666".to_string(),
            ),
            (
                "12".to_string(),
                "Zed".to_string(),
                "1".to_string(),
                "1".to_string(),
            ),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_ntile_window_projection() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"2".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let mut third_profile_row = std::collections::HashMap::new();
    third_profile_row.insert("id".to_string(), b"12".to_vec());
    third_profile_row.insert("user_id".to_string(), b"2".to_vec());
    third_profile_row.insert("name".to_string(), b"Zed".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(12),
            None,
            None,
            12,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &third_profile_row)
                .expect("third profile row should encode"),
        ),
    )
    .expect("third profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, name, ntile(2) over (order by name) as tile from profiles order by id",
    )
    .expect("ntile window select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("ntile window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("name utf8"),
                String::from_utf8(row[2].clone()).expect("tile utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "Sam".to_string(), "1".to_string()),
            ("11".to_string(), "Sam".to_string(), "1".to_string()),
            ("12".to_string(), "Zed".to_string(), "2".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_range_frame_unit() {

    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"1".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam Two".to_vec());

    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, sum(id) over (order by id range between 1 preceding and current row) as running_sum from profiles order by id",
    )
    .expect("range frame select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("range frame window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("sum utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "10".to_string()),
            ("11".to_string(), "21".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_groups_frame_unit() {
    
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let profiles_schema = catalog
        .table_schema("profiles")
        .expect("profiles schema should exist");
    let actor = UserId("test-user".to_string());

    let mut second_profile_row = std::collections::HashMap::new();
    second_profile_row.insert("id".to_string(), b"11".to_vec());
    second_profile_row.insert("user_id".to_string(), b"2".to_vec());
    second_profile_row.insert("name".to_string(), b"Sam".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(11),
            None,
            None,
            11,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &second_profile_row)
                .expect("second profile row should encode"),
        ),
    )
    .expect("second profile row should append");

    let mut third_profile_row = std::collections::HashMap::new();
    third_profile_row.insert("id".to_string(), b"12".to_vec());
    third_profile_row.insert("user_id".to_string(), b"2".to_vec());
    third_profile_row.insert("name".to_string(), b"Zed".to_vec());
    wal.append(
        "profiles",
        TransactionRecord::with_payload(
            TransactionId(12),
            None,
            None,
            12,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&profiles_schema, &third_profile_row)
                .expect("third profile row should encode"),
        ),
    )
    .expect("third profile row should append");

    let read_plan = parse_select_read_plan_from_statement(
        "select id, name, max(id) over (order by name groups between current row and current row) as peer_max from profiles order by id",
    )
    .expect("groups frame select should parse");

    let relation = catalog.table("profiles").expect("profiles table should exist");
    let schema = catalog.table_schema("profiles").expect("profiles schema should exist");
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
    .expect("groups frame window projection should succeed");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("name utf8"),
                String::from_utf8(row[2].clone()).expect("peer max utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            ("10".to_string(), "Sam".to_string(), "11".to_string()),
            ("11".to_string(), "Sam".to_string(), "11".to_string()),
            ("12".to_string(), "Zed".to_string(), "12".to_string()),
        ]
    );
}

#[test]
fn execute_relation_select_plan_supports_window_aware_qualify_filtering() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select id, row_number() over (order by id desc) as rn from users qualify rn = 1 order by id",
    )
    .expect("qualify window select should parse");

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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("qualify window select should execute");

    let rows = result
        .rows
        .iter()
        .map(|row| {
            (
                String::from_utf8(row[0].clone()).expect("id utf8"),
                String::from_utf8(row[1].clone()).expect("rn utf8"),
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(rows, vec![("2".to_string(), "1".to_string())]);
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
fn explain_select_plan_lists_indexed_equality_filters_for_equality_probe() {
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
        ("country_code", 3, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema)
        .expect("places table should register");

    let table = catalog.table("places").expect("places table should exist");

    let read_plan = parse_select_read_plan_from_statement(
        "explain select * from places where display_name='Cologne' and country_code='GM'",
    )
    .expect("explain relation plan should parse");

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::EqualityProbe {
            field_name: "display_name".to_string(),
            lookup_value: b"Cologne".to_vec(),
            source: crate::EqualityProbeSource::ExistingIndex,
            equality_filters: std::collections::HashMap::from([
                ("display_name".to_string(), b"Cologne".to_vec()),
                ("country_code".to_string(), b"GM".to_vec()),
            ]),
        },
    };

    let result = explain_select_plan_result(
        "places",
        2,
        Some(&access_plan),
        None,
        &runtime_indexes,
        &read_plan,
        Some(&table),
    );

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], b"equality_probe".to_vec());
    assert_eq!(result.columns[10].field_name, "planner_score");
    assert_eq!(result.columns[11].field_name, "index_prioritization");
    assert_eq!(result.columns[12].field_name, "row_ref_hydration");

    let index_ids = String::from_utf8(result.rows[0][2].clone())
        .expect("index ids should be UTF-8 text");

    assert!(index_ids.contains("ind:places:display_name"));
    assert!(index_ids.contains("ind:places:country_code"));

    let prioritization = String::from_utf8(result.rows[0][11].clone())
        .expect("index prioritization should be UTF-8 text");
    assert!(prioritization.contains("equality_probe"));
    assert!(prioritization.contains("full_scan"));

    let row_ref_hydration = String::from_utf8(result.rows[0][12].clone())
        .expect("row_ref_hydration should be UTF-8 text");
    assert!(!row_ref_hydration.is_empty());
}

#[test]
fn explain_select_plan_reports_row_ref_hydration_for_uid_runtime_lookup() {
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("uid", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("display_name", 2, FieldType::Text, FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema)
        .expect("places table should register");

    let table = catalog.table("places").expect("places table should exist");

    let primary_index = table
        .indexes
        .values()
        .find(|index| index.is_primary_key())
        .cloned()
        .expect("primary index should exist");

    let uid_key = vec![4980768_u64.to_le_bytes().to_vec()];

    let table_scope_id = if table.entity_id.is_empty() {
        "places".to_string()
    } else {
        table.entity_id.clone()
    };

    let state = runtime_indexes.index_mut_for_table(&table_scope_id, &primary_index.index_id.0);
    state.index = Some(primary_index.clone());
    state.insert_with_row_ref(uid_key.clone(), Some(4980768));

    let read_plan = parse_select_read_plan_from_statement(
        "explain select * from places where uid=4980768",
    )
    .expect("explain relation plan should parse");

    let access_plan = crate::RelationAccessPlan {
        strategy: crate::RelationAccessStrategy::RuntimeIndexLookup {
            index_id: primary_index.index_id.0.clone(),
            lookup_key: uid_key,
        },
    };

    let result = explain_select_plan_result(
        "places",
        1,
        Some(&access_plan),
        None,
        &runtime_indexes,
        &read_plan,
        Some(&table),
    );

    assert_eq!(result.rows.len(), 1);

    let row_ref_hydration = String::from_utf8(result.rows[0][12].clone())
        .expect("row_ref_hydration should be UTF-8 text");
    assert_eq!(row_ref_hydration, "eligible_direct_row_ref");
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
fn execute_relation_select_plan_uses_unordered_limit_fast_path() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement("select u.email from users u limit 1")
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
    let email = String::from_utf8(result.rows[0][0].clone()).expect("utf8");
    assert!(matches!(email.as_str(), "sam@example.com" | "alex@example.com"));
}

#[test]
fn execute_relation_select_plan_caps_unbounded_simple_selects() {
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("email", 2, FieldType::Text, FieldIndex::None, false),
    ]);
    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");

    let table = catalog.table("users").expect("users table should exist");
    let pk_index = table
        .indexes
        .values()
        .find(|index| index.is_primary_key())
        .cloned()
        .expect("primary key should exist");

    let actor = UserId("test-user".to_string());
    for tx_id in 1..=1_100_u64 {
        let mut row = std::collections::HashMap::new();
        row.insert("id".to_string(), tx_id.to_string().into_bytes());
        row.insert(
            "email".to_string(),
            format!("user-{tx_id}@example.com").into_bytes(),
        );

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

    let table_scope_id = if table.entity_id.is_empty() {
        table.table_id.clone()
    } else {
        table.entity_id.clone()
    };
    let state = runtime_indexes.index_mut_for_table(&table_scope_id, &pk_index.index_id.0);
    state.index = Some(pk_index.clone());
    for tx_id in 1..=1_100_u64 {
        state.insert_with_row_ref(vec![tx_id.to_string().into_bytes()], Some(tx_id));
    }

    let read_plan = parse_select_read_plan_from_statement("select u.email from users u")
        .expect("unbounded relation select should parse");

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
    .expect("unbounded relation select should succeed");

    assert_eq!(result.rows.len(), 1_000);
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
fn execute_relation_select_plan_supports_computed_where_comparison() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select email from users where concat(email, '') = 'sam@example.com'",
    )
    .expect("computed where select should parse");

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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("computed where select should execute");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], b"sam@example.com".to_vec());
}

#[test]
fn execute_relation_select_plan_expression_numeric_comparison_uses_numeric_ordering() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select u.email from users u where 12.5 <= 100",
    )
    .expect("constant numeric comparison should parse");

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
            row_matches_select_condition_result(
                row_map,
                nested_condition,
                &catalog,
                &wal,
                &runtime_indexes,
            )
        },
    )
    .expect("constant numeric comparison should execute");

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
        named_windows: Vec::new(),
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
        limit_by: None,
        top_percent: None,
        top_percent_with_ties: None,
        top_with_ties_limit: None,
        fetch_percent: None,
        fetch_percent_with_ties: None,
        fetch_with_ties_limit: None,
        limit: None,
        offset: None,
        where_condition: None,
        qualify_condition: None,
        lock_mode: SelectLockMode::None,
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
        named_windows: Vec::new(),
        projection: None,
        projection_items: vec![SelectProjectionItem::Wildcard { relation: None }],
        projection_is_wildcard: true,
        distinct: false,
        order_by: Vec::new(),
        group_by: Vec::new(),
        having_condition: None,
        has_window_clause: false,
        limit_by: None,
        top_percent: None,
        top_percent_with_ties: None,
        top_with_ties_limit: None,
        fetch_percent: None,
        fetch_percent_with_ties: None,
        fetch_with_ties_limit: None,
        limit: None,
        offset: None,
        where_condition: None,
        qualify_condition: None,
        lock_mode: SelectLockMode::None,
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

#[test]
fn execute_relation_select_plan_top_with_ties_accepts_qualified_order_by_projection_alias() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    seed_rows(&mut catalog, &wal);

    let read_plan = parse_select_read_plan_from_statement(
        "select top 1 with ties e.email from (select email from users) e order by e.email",
    )
    .expect("qualified top-with-ties select should parse");

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
    .expect("qualified top-with-ties select should execute");

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "email");
    assert!(!result.rows.is_empty());
}

#[test]
fn execute_sql_function_with_lookup_supports_begin_set_return_body() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "fndistance",
            "create FUNCTION `fndistance`(lon1 DECIMAL(10,7), lat1 DECIMAL(10,7), lon2 DECIMAL(10,7), lat2 DECIMAL(10,7)) RETURNS decimal(15,7)\n            DETERMINISTIC\n        BEGIN\n        SET @dlat = (lat2-lat1) * 0.0174532925;\n        SET @dlon = (lon2-lon1) * 0.0174532925;\n        SET @lat1 = lat1 * 0.0174532925;\n        SET @lat2 = lat2 * 0.0174532925;\n        SET @a = SIN(@dlat/2) * SIN(@dlat/2) + SIN(@dlon/2) * SIN(@dlon/2) * COS(@lat1) * COS(@lat2);\n        SET @c = 2 * ATAN2(SQRT(@a), SQRT(1-@a));\n        SET @d = 6371 * @c;\n        RETURN @d;\n        END",
            vec![],
        )
        .expect("function should register");

    let statements = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "select fndistance(6.95, 50.93, 6.96, 50.94)",
    )
    .expect("select should parse");

    let Some(sqlparser::ast::Statement::Query(query)) = statements.into_iter().next() else {
        panic!("query statement should be present");
    };

    let sqlparser::ast::SetExpr::Select(select) = *query.body else {
        panic!("query should contain select body");
    };

    let Some(sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(function))) =
        select.projection.into_iter().next()
    else {
        panic!("projection should contain function call");
    };

    let result = execute_sql_function_with_lookup(
        &catalog,
        &wal,
        &runtime_indexes,
        &function,
        &mut |_| None,
    )
    .expect("function execution should succeed")
    .expect("function should return a value");

    let text = String::from_utf8(result).expect("function result should be utf8");
    let distance = text
        .parse::<f64>()
        .expect("function result should parse as f64");

    assert!(distance.is_finite());
    assert!(distance > 0.0);
}

#[test]
fn udf_predicate_alignment_across_relation_and_join_where() {
    let wal = ConcurrentWalManager::in_memory();
    let runtime_indexes = RuntimeIndexStore::new();
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let places_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("lon", 2, FieldType::Text, FieldIndex::None, false),
        ("lat", 3, FieldType::Text, FieldIndex::None, false),
    ]);

    let visits_schema = table_schema(vec![
        ("id", 1, FieldType::UInt(64), FieldIndex::PrimaryKey, false),
        ("place_id", 2, FieldType::UInt(64), FieldIndex::Indexed, false),
    ]);

    catalog
        .register_table("places", places_schema.clone())
        .expect("places table should register");
    catalog
        .register_table("visits", visits_schema.clone())
        .expect("visits table should register");

    catalog
        .register_stored_procedure(
            "fndistance",
            "create FUNCTION `fndistance`(lon1 DECIMAL(10,7), lat1 DECIMAL(10,7), lon2 DECIMAL(10,7), lat2 DECIMAL(10,7)) RETURNS decimal(15,7)\n            DETERMINISTIC\n        BEGIN\n        SET @dlat = (lat2-lat1) * 0.0174532925;\n        SET @dlon = (lon2-lon1) * 0.0174532925;\n        SET @lat1 = lat1 * 0.0174532925;\n        SET @lat2 = lat2 * 0.0174532925;\n        SET @a = SIN(@dlat/2) * SIN(@dlat/2) + SIN(@dlon/2) * SIN(@dlon/2) * COS(@lat1) * COS(@lat2);\n        SET @c = 2 * ATAN2(SQRT(@a), SQRT(1-@a));\n        SET @d = 6371 * @c;\n        RETURN @d;\n        END",
            vec![],
        )
        .expect("function should register");

    let actor = UserId("test-user".to_string());

    let mut near_place = std::collections::HashMap::new();
    near_place.insert("id".to_string(), b"1".to_vec());
    near_place.insert("lon".to_string(), b"6.95".to_vec());
    near_place.insert("lat".to_string(), b"50.93".to_vec());

    let mut far_place = std::collections::HashMap::new();
    far_place.insert("id".to_string(), b"2".to_vec());
    far_place.insert("lon".to_string(), b"7.8".to_vec());
    far_place.insert("lat".to_string(), b"51.3".to_vec());

    wal.append(
        "places",
        TransactionRecord::with_payload(
            TransactionId(1),
            None,
            None,
            1,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &near_place).expect("near place row should encode"),
        ),
    )
    .expect("near place row should append");

    wal.append(
        "places",
        TransactionRecord::with_payload(
            TransactionId(2),
            None,
            None,
            2,
            actor.clone(),
            TransactionKind::Insert,
            encode_row_payload(&places_schema, &far_place).expect("far place row should encode"),
        ),
    )
    .expect("far place row should append");

    let mut visit_row = std::collections::HashMap::new();
    visit_row.insert("id".to_string(), b"10".to_vec());
    visit_row.insert("place_id".to_string(), b"1".to_vec());

    wal.append(
        "visits",
        TransactionRecord::with_payload(
            TransactionId(10),
            None,
            None,
            10,
            actor,
            TransactionKind::Insert,
            encode_row_payload(&visits_schema, &visit_row).expect("visit row should encode"),
        ),
    )
    .expect("visit row should append");

    let relation_plan = parse_select_read_plan_from_statement(
        "select id from places where fndistance(lon, lat, 6.95, 50.93) < 5",
    )
    .expect("relation UDF plan should parse");

    let relation = catalog
        .table(&relation_plan.table_id)
        .expect("places table should exist");
    let relation_schema = catalog
        .table_schema(&relation_plan.table_id)
        .expect("places schema should exist");

    let relation_result = execute_relation_select_plan(
        &wal,
        relation,
        relation_schema,
        &runtime_indexes,
        &relation_plan,
        &crate::RelationAccessPlan {
            strategy: crate::RelationAccessStrategy::FullScan,
        },
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
    .expect("relation UDF select should execute");

    assert_eq!(relation_result.rows.len(), 1);
    assert_eq!(relation_result.rows[0][0], b"1".to_vec());

    let join_plan = parse_select_read_plan_from_statement(
        "select p.id from places p inner join visits v on p.id = v.place_id where fndistance(p.lon, p.lat, 6.95, 50.93) < 5",
    )
    .expect("join UDF plan should parse");

    let join_result = execute_joined_select_plan(
        &catalog,
        &wal,
        &runtime_indexes,
        &join_plan,
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
    .expect("join UDF select should execute");

    assert_eq!(join_result.rows.len(), 1);
    assert_eq!(join_result.rows[0][0], b"1".to_vec());
}
