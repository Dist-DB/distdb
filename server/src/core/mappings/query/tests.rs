use std::collections::HashMap;
use std::path::PathBuf;

use connector::{ConnectorResult, DataQuery};
use serverlib::DatabaseCatalog;
use serverlib::{
    ConcurrentWalManager, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, TableSchema,
    TransactionId, TransactionKind,
};

use super::*;

fn test_node_data_dir() -> PathBuf {
    
    let dir = std::env::temp_dir().join(format!(
        "distdb-query-tests-{}-{}",
        std::process::id(),
        common::epoch_nanos!()
    ));

    std::fs::create_dir_all(&dir).expect("test data dir should be created");
    dir

}

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

fn query_result_columns(response: connector::ConnectorResponse) -> Vec<connector::FieldDef> {

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query response")
    };

    result.columns
    
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
fn select_from_dotted_table_without_active_database_resolves_catalog_prefix() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");

    let mut catalogs = HashMap::new();
    catalogs.insert(catalog.database_id.0.clone(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: String::new(),
        sql: "select * from main.users".to_string(),
    };

    let response = handle_query_command(
        "req-dotted-select",
        &data_query,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        response
    );
    let rows = query_result_rows(response);
    assert!(rows.is_empty());
}

#[test]
fn select_from_dotted_table_with_active_database_strips_matching_prefix() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("locations").expect("catalog should be created");
    catalog
        .register_table("places", TableSchema::new(Vec::new()))
        .expect("table should be created");

    let mut catalogs = HashMap::new();
    catalogs.insert(catalog.database_id.0.clone(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "locations".to_string(),
        sql: "select * from locations.places".to_string(),
    };

    let response = handle_query_command(
        "req-dotted-select-active-db",
        &data_query,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        response
    );
    let rows = query_result_rows(response);
    assert!(rows.is_empty());
}

#[test]
fn create_view_persists_dependencies_from_view_body() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let database_id = catalog.database_id.0.clone();
    catalog
        .register_table("users", TableSchema::new(Vec::new()))
        .expect("table should be created");

    let mut catalogs = HashMap::new();
    catalogs.insert(database_id.clone(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "create view users_v as select * from users".to_string(),
    };
    let request = serverlib::SqlRequest {
        database_id: data_query.database_id.clone(),
        sql: data_query.sql.clone(),
        parsed_statement: None,
        parsed_insert_plan: None,
        directive: serverlib::SqlDirective::Create,
        operation: serverlib::SqlOperation::CreateView,
        object_name: Some("users_v".to_string()),
        compatibility_target: serverlib::engine::sql::DEFAULT_SQL_COMPATIBILITY_TARGET,
    };

    let response = super::core::execute_create_view_impl(
        "req-create-view",
        &data_query,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &request,
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        response
    );

    let catalog = catalogs
        .get(&database_id)
        .expect("catalog should still exist after view creation");
    let view = catalog.view("users_v").expect("view should exist");

    assert_eq!(view.dependencies, vec!["users".to_string()]);
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
        &test_node_data_dir(),
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
        &test_node_data_dir(),
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
        &catalog,
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
fn union_query_executes_and_deduplicates_rows() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived_users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "2"),
        ("archived_users", "2"),
        ("archived_users", "3"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union select id from archived_users".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows, vec![
        vec!["1".to_string()],
        vec!["2".to_string()],
        vec!["3".to_string()],
    ]);
}

#[test]
fn create_database_with_aes_option_enables_at_rest_encryption() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let response = handle_query_command(
        "req-create-db-aes",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create database analytics --aes".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.result, ConnectorResult::Mutation(_)));

    let catalog = catalogs
        .values()
        .find(|catalog| catalog.database_name() == "analytics")
        .expect("created analytics catalog should exist");

    assert!(catalog.at_rest_encryption_enabled());
    assert_eq!(catalog.at_rest_encryption_key_version(), 1);

    let key_ref = catalog
        .at_rest_encryption_key_ref()
        .expect("encryption key reference should be set");
    assert!(key_ref.starts_with("enc:analytics:"));
}

#[test]
fn create_database_with_explicit_aes_key_ref_sets_metadata() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let response = handle_query_command(
        "req-create-db-aes-explicit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create database billing --aes=enc:node1:billing".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.result, ConnectorResult::Mutation(_)));

    let catalog = catalogs
        .values()
        .find(|catalog| catalog.database_name() == "billing")
        .expect("created billing catalog should exist");

    assert!(catalog.at_rest_encryption_enabled());
    assert_eq!(catalog.at_rest_encryption_key_version(), 1);
    assert_eq!(
        catalog.at_rest_encryption_key_ref(),
        Some("enc:node1:billing")
    );
}

#[test]
fn except_query_executes_with_distinct_semantics() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived_users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "2"),
        ("users", "3"),
        ("archived_users", "2"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-except-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users except select id from archived_users".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["3".to_string()]]);
}

#[test]
fn intersect_query_executes_with_distinct_semantics() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived_users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "2"),
        ("users", "3"),
        ("archived_users", "2"),
        ("archived_users", "4"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-intersect-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users intersect select id from archived_users".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["2".to_string()]]);
}

#[test]
fn mixed_set_operators_execute_with_precedence_aware_result() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived_users table should register");
    catalog
        .register_table(
            "backup_users",
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
        .expect("backup_users table should register");

    for (table_id, id) in [("users", "1"), ("archived_users", "2"), ("backup_users", "2")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-set-mixed-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union select id from archived_users intersect select id from backup_users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["2".to_string()]]);
}

#[test]
fn union_query_supports_mixed_union_and_union_all_semantics() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema.clone())
        .expect("archived_users table should register");
    catalog
        .register_table("backup_users", schema)
        .expect("backup_users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "2"),
        ("archived_users", "2"),
        ("archived_users", "3"),
        ("backup_users", "2"),
        ("backup_users", "4"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-mixed-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union all select id from archived_users union select id from backup_users".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![
        vec!["1".to_string()],
        vec!["2".to_string()],
        vec!["3".to_string()],
        vec!["4".to_string()],
    ]);
}

#[test]
fn union_query_applies_query_level_order_by_limit_and_offset() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived_users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "3"),
        ("archived_users", "2"),
        ("archived_users", "4"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-order-limit-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union all select id from archived_users order by id desc limit 2 offset 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["3".to_string()], vec!["2".to_string()]]);
}

#[test]
fn union_query_applies_order_by_ordinal_position() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived_users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "3"),
        ("archived_users", "2"),
        ("archived_users", "4"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-order-ordinal-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union all select id from archived_users order by 1 desc"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(
        rows,
        vec![
            vec!["4".to_string()],
            vec!["3".to_string()],
            vec!["2".to_string()],
            vec!["1".to_string()]
        ]
    );
}

#[test]
fn union_query_with_top_level_cte_executes_branches() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::PrimaryKey,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for id in ["1", "2", "3"] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            "users",
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-cte-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with staged as (select id from users where id > 1) select id from staged union all select id from staged order by 1 desc"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(
        rows,
        vec![
            vec!["3".to_string()],
            vec!["3".to_string()],
            vec!["2".to_string()],
            vec!["2".to_string()]
        ]
    );
}

#[test]
fn union_query_coerces_numeric_and_text_column_types_to_text() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "users",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::Int(32),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("users table should register");

    catalog
        .register_table(
            "labels",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::Text,
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("labels table should register");

    for (table_id, value) in [("users", "10"), ("labels", "alpha")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("value".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-type-coerce-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select value from users union all select value from labels".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let columns = query_result_columns(response);
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].field_type, FieldType::Text);
}

#[test]
fn union_query_keeps_integer_family_for_signed_and_unsigned_columns() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "signed_values",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::Int(32),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("signed_values table should register");

    catalog
        .register_table(
            "unsigned_values",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::UInt(16),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("unsigned_values table should register");

    for (table_id, value) in [("signed_values", "10"), ("unsigned_values", "20")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("value".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-int-family-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select value from signed_values union all select value from unsigned_values"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let columns = query_result_columns(response);
    assert_eq!(columns[0].field_type, FieldType::Int(64));
}

#[test]
fn union_query_widens_fixed_length_strings() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "short_codes",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "code".to_string(),
                field_type: FieldType::StringFixed(3),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("short_codes table should register");

    catalog
        .register_table(
            "long_codes",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "code".to_string(),
                field_type: FieldType::StringFixed(12),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("long_codes table should register");

    for (table_id, value) in [("short_codes", "abc"), ("long_codes", "abcdefghijkl")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("code".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-string-family-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select code from short_codes union all select code from long_codes".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let columns = query_result_columns(response);
    assert_eq!(columns[0].field_type, FieldType::StringFixed(12));
}

#[test]
fn union_query_reconciles_enum_types_to_wider_string_family() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "draft_status",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "status".to_string(),
                field_type: FieldType::Enum(vec!["draft".to_string(), "pub".to_string()]),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("draft_status table should register");

    catalog
        .register_table(
            "review_status",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "status".to_string(),
                field_type: FieldType::Enum(vec!["draft".to_string(), "published".to_string()]),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("review_status table should register");

    for (table_id, value) in [("draft_status", "draft"), ("review_status", "published")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("status".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-enum-family-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select status from draft_status union all select status from review_status"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let columns = query_result_columns(response);
    assert_eq!(columns[0].field_type, FieldType::StringFixed(9));
}

#[test]
fn union_query_deduplicates_case_insensitively_under_ci_collation() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let metadata = common::schema::FieldMetadata {
        comment: None,
        auto_increment: false,
        original_sql_type: None,
        character_set: Some("utf8mb4".to_string()),
        collation: Some("utf8mb4_general_ci".to_string()),
        system_visibility: common::schema::SystemFieldVisibility::Visible,
    };

    catalog
        .register_table(
            "left_strings",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::StringFixed(8),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: Some(metadata.clone()),
            }]),
        )
        .expect("left_strings table should register");

    catalog
        .register_table(
            "right_strings",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::StringFixed(8),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: Some(metadata),
            }]),
        )
        .expect("right_strings table should register");

    for (table_id, value) in [("left_strings", "Alpha"), ("right_strings", "alpha")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("value".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-ci-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select value from left_strings union select value from right_strings"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["Alpha".to_string()]]);

    let columns = query_result_columns(handle_query_command(
        "req-union-ci-columns-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select value from left_strings union select value from right_strings"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    ));
    assert_eq!(columns[0].metadata.as_ref().and_then(|metadata| metadata.collation.as_deref()), Some("utf8mb4_general_ci"));
}

#[test]
fn union_query_rejects_conflicting_collations() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "ci_strings",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::StringFixed(8),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: Some(common::schema::FieldMetadata {
                    comment: None,
                    auto_increment: false,
                    original_sql_type: None,
                    character_set: Some("utf8mb4".to_string()),
                    collation: Some("utf8mb4_general_ci".to_string()),
                    system_visibility: common::schema::SystemFieldVisibility::Visible,
                }),
            }]),
        )
        .expect("ci_strings table should register");

    catalog
        .register_table(
            "bin_strings",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "value".to_string(),
                field_type: FieldType::StringFixed(8),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: Some(common::schema::FieldMetadata {
                    comment: None,
                    auto_increment: false,
                    original_sql_type: None,
                    character_set: Some("utf8mb4".to_string()),
                    collation: Some("utf8mb4_bin".to_string()),
                    system_visibility: common::schema::SystemFieldVisibility::Visible,
                }),
            }]),
        )
        .expect("bin_strings table should register");

    for (table_id, value) in [("ci_strings", "Alpha"), ("bin_strings", "alpha")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("value".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-ci-conflict-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select value from ci_strings union select value from bin_strings".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error response")
    };

    assert!(message.contains("collation mismatch"));
}

#[test]
fn union_query_rejects_incompatible_blob_and_spatial_types() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    catalog
        .register_table(
            "binary_assets",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "payload".to_string(),
                field_type: FieldType::Blob,
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("binary_assets table should register");

    catalog
        .register_table(
            "geo_assets",
            TableSchema::new(vec![FieldDef {
                seqno: 1,
                field_name: "payload".to_string(),
                field_type: FieldType::Spatial,
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }]),
        )
        .expect("geo_assets table should register");

    for (table_id, value) in [("binary_assets", "blob-data"), ("geo_assets", "point(1 2)")] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("payload".to_string(), value.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            table_id,
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-union-type-incompatible-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select payload from binary_assets union all select payload from geo_assets"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error response")
    };

    assert!(message.contains("type mismatch"));
}

#[test]
fn select_distinct_group_having_order_executes_in_first_pass_model() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for id in ["1", "1", "2", "3"] {
        let table = catalog.table("users").expect("users table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            "users",
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-select-order-group-having-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select distinct id from users group by id having id > 1 order by id desc"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["3".to_string()], vec!["2".to_string()]]);
}

#[test]
fn select_orders_by_non_projected_field_without_returning_hidden_sort_key() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![
        FieldDef {
            seqno: 1,
            field_name: "id".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::PrimaryKey,
            default_value: None,
            metadata: None,
        },
        FieldDef {
            seqno: 2,
            field_name: "name".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for (id, name) in [("1", "alice"), ("2", "charlie"), ("3", "bob")] {
        let table = catalog.table("users").expect("users table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("name".to_string(), name.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            "users",
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-select-hidden-order-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by name desc".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query response")
    };

    let rendered_rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(result.columns.len(), 1);
    assert_eq!(result.columns[0].field_name, "id");
    assert_eq!(rendered_rows, vec![
        vec!["2".to_string()],
        vec!["3".to_string()],
        vec!["1".to_string()],
    ]);
}

#[test]
fn select_with_cte_executes_and_orders_rows() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let schema = TableSchema::new(vec![FieldDef {
        seqno: 1,
        field_name: "id".to_string(),
        field_type: FieldType::Int(32),
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for id in ["1", "2"] {
        let table = catalog.table("users").expect("users table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());

        let payload = serverlib::encode_row_payload(table.schema(), &row)
            .expect("row payload should encode");

        super::core::append_row_payload_record(
            &catalog,
            &wal,
            "users",
            table,
            &mut runtime_indexes,
            TransactionKind::Insert,
            payload,
            common::epoch_nanos!(),
            None,
            None,
        )
        .expect("row append should succeed");
    }

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-select-cte-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with staged as (select id from users) select id from staged order by id desc"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["2".to_string()], vec!["1".to_string()]]);
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
        &catalog,
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
        &catalog,
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
        &catalog,
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
