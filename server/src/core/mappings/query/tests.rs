use std::collections::HashMap;

use connector::{ConnectorResult, DataQuery};
use serverlib::DatabaseCatalog;
use serverlib::{
    ConcurrentWalManager, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, TableSchema,
    TransactionId, TransactionKind,
};

use super::*;

fn query_result_rows(response: connector::ConnectorResponse) -> Vec<Vec<String>> {
    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query response")
    };

    result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

#[test]
fn explain_inner_statement_detects_prefix_case_insensitive() {
    let (inner, is_explain) = explain_inner_statement("  ExPlAiN   select 1  ");
    assert!(is_explain);
    assert_eq!(inner, "select 1");

    let (inner, is_explain) = explain_inner_statement("select 1");
    assert!(!is_explain);
    assert_eq!(inner, "select 1");
}

#[test]
fn explain_mutation_plan_returns_attribute_value_rows() {
    let response = explain_mutation_plan(
        "req-1",
        vec![
            vec!["operation".to_string(), "insert".to_string()],
            vec!["table".to_string(), "users".to_string()],
        ],
    );

    let rows = query_result_rows(response);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec!["operation".to_string(), "insert".to_string()]);
    assert_eq!(rows[1], vec!["table".to_string(), "users".to_string()]);
}

#[test]
fn explain_join_mutation_plan_includes_join_surface_details() {
    let plan = serverlib::parse_update_rows_from_statement(
        "update users u inner join profiles p on u.id = p.user_id set u.email = 'a@b.com' where p.user_id = 1",
    )
    .expect("update plan should parse");

    let response = explain_join_mutation_plan(
        "req-join",
        "update",
        &plan.table_id,
        &plan.relations,
        &plan.joins,
        &plan.pushdown_conditions,
        plan.assignments.len(),
        plan.where_condition.is_some(),
    );

    let rows = query_result_rows(response);
    assert!(rows.contains(&vec!["join_count".to_string(), "1".to_string()]));
    assert!(rows.contains(&vec!["assignment_count".to_string(), "1".to_string()]));
    assert!(rows.contains(&vec![
        "join[0].relation".to_string(),
        "profiles".to_string()
    ]));
}

#[test]
fn resolve_catalog_supports_user_database_name_lookup() {
    let catalog =
        DatabaseCatalog::create_empty_from_name("OrdersDb").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id.clone(), catalog);

    assert!(resolve_catalog(&catalogs, &db_id).is_some());
    assert!(resolve_catalog(&catalogs, "OrdersDb").is_some());
    assert!(resolve_catalog_mut(&mut catalogs, "OrdersDb").is_some());
}

#[test]
fn begin_transaction_is_explicitly_recognized() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "begin".to_string(),
    };

    let response = handle_query_command(
        "req-begin",
        &data_query,
        &mut catalogs,
        &wal,
        std::path::Path::new("."),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(message.contains("session transactions are not wired yet"));
}

#[test]
fn commit_is_explicitly_recognized() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "commit".to_string(),
    };

    let response = handle_query_command(
        "req-commit",
        &data_query,
        &mut catalogs,
        &wal,
        std::path::Path::new("."),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(message.contains("session transactions are not wired yet"));
}

#[test]
fn append_row_payload_record_rejects_missing_refid() {
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(32),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("table should be registered");

    let table = catalog
        .table("users")
        .expect("users table should exist")
        .clone();

    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());

    let payload =
        serverlib::encode_row_payload(table.schema(), &row).expect("row payload should encode");

    let err = super::core::append_row_payload_record(
        &wal,
        "users",
        &table,
        &mut runtime_indexes,
        TransactionKind::Delete,
        payload,
        1,
        Some(TransactionId(99)),
        None,
    )
    .expect_err("missing refid should be rejected");

    assert!(err.contains("references stale or missing live transaction id 99"));
}

#[test]
fn append_row_payload_record_rejects_stale_refid() {
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::Int(32),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("table should be registered");

    let table = catalog
        .table("users")
        .expect("users table should exist")
        .clone();

    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());

    let payload =
        serverlib::encode_row_payload(table.schema(), &row).expect("row payload should encode");

    super::core::append_row_payload_record(
        &wal,
        "users",
        &table,
        &mut runtime_indexes,
        TransactionKind::Insert,
        payload.clone(),
        1,
        None,
        None,
    )
    .expect("insert should succeed");

    super::core::append_row_payload_record(
        &wal,
        "users",
        &table,
        &mut runtime_indexes,
        TransactionKind::Delete,
        payload.clone(),
        2,
        Some(TransactionId(1)),
        None,
    )
    .expect("first delete should succeed");

    let err = super::core::append_row_payload_record(
        &wal,
        "users",
        &table,
        &mut runtime_indexes,
        TransactionKind::Delete,
        payload,
        3,
        Some(TransactionId(1)),
        None,
    )
    .expect_err("stale refid should be rejected");

    assert!(err.contains("references stale or missing live transaction id 1"));
}
