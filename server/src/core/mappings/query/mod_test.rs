use std::collections::HashMap;
use std::path::PathBuf;

use connector::{ConnectorResult, DataQuery};
use serverlib::DatabaseCatalog;
use serverlib::{
    ConcurrentWalManager, FieldDef, FieldIndex, FieldType, RuntimeIndexStore, TableSchema,
    TransactionId, TransactionKind,
};
use serverlib::engine::security::AccountPrivilege;

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

fn expected_os_user_identity() -> String {
    std::env::var("USER")
        .ok()
        .map(|user| format!("{}@localhost", user))
        .unwrap_or_else(|| "root@localhost".to_string())
}

fn query_as_session(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &PathBuf,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
) -> connector::ConnectorResponse {
    let mut session_overrides = SessionVariableOverrides::new();

    query_as_session_with_overrides(
        request_id,
        query,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        session_id,
        connection_id,
        &mut session_overrides,
    )
}

fn query_as_session_with_overrides(
    request_id: &str,
    query: &DataQuery,
    catalogs: &mut HashMap<String, DatabaseCatalog>,
    wal: &ConcurrentWalManager,
    node_data_dir: &PathBuf,
    runtime_indexes: &mut RuntimeIndexStore,
    session_id: &str,
    connection_id: usize,
    session_overrides: &mut SessionVariableOverrides,
) -> connector::ConnectorResponse {

    handle_query_command_with_session_variables(
        request_id,
        query,
        catalogs,
        wal,
        node_data_dir,
        runtime_indexes,
        session_id,
        connection_id,
        Some(expected_os_user_identity()),
        session_overrides,
    )
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

    let response = query_as_session(
        "req-dotted-select",
        &data_query,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
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

    let response = query_as_session(
        "req-dotted-select-active-db",
        &data_query,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
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
        required_privilege: Some(serverlib::engine::security::AccountPrivilege::Create),
        compatibility_target: serverlib::engine::sql::DEFAULT_SQL_COMPATIBILITY_TARGET,
    };

    let response = super::core::execute_create_view_impl(
        "req-create-view",
        data_query.database_id.as_str(),
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
fn begin_transaction_is_accepted_as_noop_mutation() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "begin".to_string(),
    };

    let response = query_as_session(
        "req-begin",
        &data_query,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let ConnectorResult::Mutation(result) = response.result else {
        panic!("expected mutation result")
    };

    assert_eq!(result.affected_rows, 0);
}

#[test]
fn insert_returning_returns_inserted_rows() {
    let mut catalogs = HashMap::new();
    let mut catalog = DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
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
                    field_name: "email".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("users table should register");
    catalogs.insert("main".to_string(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let response = query_as_session(
        "req-insert-returning",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'sam@example.com') returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string(), "sam@example.com".to_string()]]);
}

#[test]
fn replace_into_replaces_existing_primary_key_row() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-replace",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64), active varchar(8))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-replace-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, active) values (1, 'seed@example.com', 'false')"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let replace = query_as_session(
        "req-replace-users",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "replace into users (id, email, active) values (1, 'incoming@example.com', 'true') returning id, email, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(replace),
        vec![vec![
            "1".to_string(),
            "incoming@example.com".to_string(),
            "true".to_string(),
        ]]
    );
}

#[test]
fn replace_into_replaces_existing_unique_key_row() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-replace-unique",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64) unique, active varchar(8))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-replace-unique-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, active) values (1, 'seed@example.com', 'false')"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let replace = query_as_session(
        "req-replace-users-unique",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "replace into users (id, email, active) values (2, 'seed@example.com', 'true') returning id, email, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(replace),
        vec![vec![
            "2".to_string(),
            "seed@example.com".to_string(),
            "true".to_string(),
        ]]
    );

    let rows = query_result_rows(query_as_session(
        "req-select-users-replace-unique",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, email, active from users".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    ));

    assert_eq!(
        rows,
        vec![vec![
            "2".to_string(),
            "seed@example.com".to_string(),
            "true".to_string(),
        ]]
    );
}

#[test]
fn replace_into_with_duplicate_values_keeps_last_payload() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-replace-duplicate-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let replace = query_as_session(
        "req-replace-duplicate-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "replace into users (id, email) values (1, 'first@example.com'), (1, 'second@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(replace.status, connector::ResponseStatus::Applied));

    let read = query_as_session(
        "req-select-replace-duplicate-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, email from users".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(read),
        vec![vec!["1".to_string(), "second@example.com".to_string()]]
    );
}

#[test]
fn insert_accepts_placeholder_literals_as_raw_values() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-placeholder-insert",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let insert = query_as_session(
        "req-insert-placeholder-literal",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, :incoming_email) returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(insert),
        vec![vec!["1".to_string(), ":incoming_email".to_string()]]
    );
}

#[test]
fn insert_default_values_uses_schema_defaults() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-default-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key default 1, email varchar(64) default 'seed@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let insert = query_as_session(
        "req-insert-default-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users default values returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(insert),
        vec![vec!["1".to_string(), "seed@example.com".to_string()]]
    );
}

#[test]
fn insert_default_values_with_column_list_uses_defaults() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-default-values-column-list",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key default 9, email varchar(64) default 'seed@example.com', nickname varchar(64) default 'seed')".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let insert = query_as_session(
        "req-insert-default-values-column-list",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) default values returning id, email, nickname".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(insert),
        vec![vec![
            "9".to_string(),
            "seed@example.com".to_string(),
            "seed".to_string(),
        ]]
    );
}

#[test]
fn insert_set_syntax_inserts_row() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-insert-set",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64), active bool default false)".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let insert = query_as_session(
        "req-insert-users-set",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users set id = 1, email = 'set@example.com' returning id, email, active"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(insert),
        vec![vec![
            "1".to_string(),
            "set@example.com".to_string(),
            "false".to_string(),
        ]]
    );
}

#[test]
fn insert_set_syntax_with_qualified_targets_inserts_row() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-insert-set-qualified",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let insert = query_as_session(
        "req-insert-users-set-qualified",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users set app.users.id = 1, users.email = 'qualified@example.com' returning id, email"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(insert),
        vec![vec!["1".to_string(), "qualified@example.com".to_string(),]]
    );
}

#[test]
fn insert_default_values_rejects_when_required_column_has_no_default() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-default-values-reject",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key default 1, email varchar(64) not null)".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let insert = query_as_session(
        "req-insert-default-values-reject",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users default values".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(insert.status, connector::ResponseStatus::Rejected));
    let connector::ConnectorResult::Error(message) = insert.result else {
        panic!("expected error result")
    };
    assert!(message.contains("missing required column 'email'"));
}

#[test]
fn insert_ignore_skips_duplicate_primary_keys() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-insert-ignore",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let response = query_as_session(
        "req-insert-ignore-users",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert ignore into users (id, email) values (1, 'a@example.com'), (1, 'dup@example.com'), (2, 'b@example.com') returning id".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["2".to_string()]]);
}

#[test]
fn insert_rejects_duplicate_non_primary_unique_key() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-unique-reject",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64) unique)".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-users-unique-reject-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'dup@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let duplicate = query_as_session(
        "req-insert-users-unique-reject-duplicate",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (2, 'dup@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(duplicate.status, connector::ResponseStatus::Rejected));
    let connector::ConnectorResult::Error(message) = duplicate.result else {
        panic!("expected error result")
    };
    assert!(message.contains("duplicate unique key"));
}

#[test]
fn insert_ignore_skips_duplicate_non_primary_unique_key() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_table = query_as_session(
        "req-create-users-unique-ignore",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64) unique)".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let response = query_as_session(
        "req-insert-ignore-users-unique",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert ignore into users (id, email) values (1, 'a@example.com'), (2, 'a@example.com'), (3, 'c@example.com') returning id".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string()], vec!["3".to_string()]]);
}

#[test]
fn insert_on_duplicate_key_update_updates_existing_rows() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64), active varchar(8))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, active) values (1, 'a@example.com', 'false')"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, active) values (1, 'dup@example.com', 'true') on duplicate key update email = 'updated@example.com', active = 'true' returning id, email, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(upsert);
    assert_eq!(
        rows,
        vec![vec![
            "1".to_string(),
            "updated@example.com".to_string(),
            "true".to_string(),
        ]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_values_column_reference() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64), active varchar(8))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-values-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, active) values (1, 'seed@example.com', 'false')"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-values",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, active) values (1, 'incoming@example.com', 'true') on duplicate key update email = values(email), active = values(active) returning id, email, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(upsert);
    assert_eq!(
        rows,
        vec![vec![
            "1".to_string(),
            "incoming@example.com".to_string(),
            "true".to_string(),
        ]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_unique_key_conflicts_without_primary_key() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-unique-no-pk",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (email varchar(64) unique, active varchar(8))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-unique-no-pk-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (email, active) values ('seed@example.com', 'false')"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-unique-no-pk",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (email, active) values ('seed@example.com', 'true') on duplicate key update active = values(active) returning email, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(upsert),
        vec![vec!["seed@example.com".to_string(), "true".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_existing_column_reference() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-existing-col-ref",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-existing-col-ref-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'seed@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-existing-col-ref",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'incoming@example.com') on duplicate key update email = email returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(upsert);
    assert_eq!(
        rows,
        vec![vec!["1".to_string(), "seed@example.com".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_arithmetic_assignment_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 2) on duplicate key update login_count = login_count + values(login_count) returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(upsert),
        vec![vec!["1".to_string(), "12".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_nested_arithmetic_assignment_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-nested-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-nested-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-nested-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 2) on duplicate key update login_count = (login_count + values(login_count)) * 2 returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(upsert),
        vec![vec!["1".to_string(), "24".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_arithmetic_function_operand_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-fn-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-fn-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-fn-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 2) on duplicate key update login_count = login_count + abs(1) returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(upsert),
        vec![vec!["1".to_string(), "11".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_unary_arithmetic_operand_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-unary-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-unary-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-unary-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 2) on duplicate key update login_count = -login_count + values(login_count) returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(upsert),
        vec![vec!["1".to_string(), "-8".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_supports_top_level_unary_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-top-unary",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-top-unary-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-top-unary",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 2) on duplicate key update login_count = -values(login_count) returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(upsert),
        vec![vec!["1".to_string(), "-2".to_string()]]
    );
}

#[test]
fn update_rows_supports_nested_arithmetic_assignment_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-nested-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-update-nested-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let update = query_as_session(
        "req-update-nested-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set login_count = (login_count + 1) * 2 where id = 1 returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(update),
        vec![vec!["1".to_string(), "22".to_string()]]
    );
}

#[test]
fn update_rows_supports_arithmetic_function_operand_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-fn-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-update-fn-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let update = query_as_session(
        "req-update-fn-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set login_count = login_count + abs(1) where id = 1 returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(update),
        vec![vec!["1".to_string(), "11".to_string()]]
    );
}

#[test]
fn update_rows_supports_top_level_unary_assignment_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-top-unary",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-update-top-unary-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 5)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let update = query_as_session(
        "req-update-top-unary",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set login_count = -login_count where id = 1 returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(update),
        vec![vec!["1".to_string(), "-5".to_string()]]
    );
}

#[test]
fn update_rows_supports_unary_arithmetic_operand_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-unary-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-update-unary-arithmetic-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let update = query_as_session(
        "req-update-unary-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set login_count = -login_count + 5 where id = 1 returning id, login_count".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(update),
        vec![vec!["1".to_string(), "-5".to_string()]]
    );
}

#[test]
fn update_order_by_lower_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-order-lower",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, name varchar(32), active bool)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, name, active) values (1, 'B', false)",
        "insert into users (id, name, active) values (2, 'a', false)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-update-order-lower",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let update = query_as_session(
        "req-update-users-order-lower",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = true order by lower(name) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(update.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-order-lower",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, active from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(selected),
        vec![
            vec!["1".to_string(), "false".to_string()],
            vec!["2".to_string(), "true".to_string()],
        ]
    );
}

#[test]
fn update_order_by_abs_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-order-abs",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, score int, active bool)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, score, active) values (1, -1, false)",
        "insert into users (id, score, active) values (2, -5, false)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-update-order-abs",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let update = query_as_session(
        "req-update-users-order-abs",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = true order by abs(score) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(update.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-order-abs",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, active from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(selected),
        vec![
            vec!["1".to_string(), "true".to_string()],
            vec!["2".to_string(), "false".to_string()],
        ]
    );
}

#[test]
fn update_order_by_trim_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-order-trim",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, name varchar(32), active bool)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, name, active) values (1, '  a', false)",
        "insert into users (id, name, active) values (2, ' b', false)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-update-order-trim",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let update = query_as_session(
        "req-update-users-order-trim",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = true order by trim(name) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(update.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-order-trim",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, active from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(selected),
        vec![
            vec!["1".to_string(), "true".to_string()],
            vec!["2".to_string(), "false".to_string()],
        ]
    );
}

#[test]
fn update_order_by_round_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-order-round",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, score double, active bool)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, score, active) values (1, 1.2, false)",
        "insert into users (id, score, active) values (2, 1.8, false)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-update-order-round",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let update = query_as_session(
        "req-update-users-order-round",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = true order by round(score) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(update.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-order-round",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, active from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(selected),
        vec![
            vec!["1".to_string(), "true".to_string()],
            vec!["2".to_string(), "false".to_string()],
        ]
    );
}

#[test]
fn update_order_by_round_with_scale_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-update-order-round-scale",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, score double, active bool)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, score, active) values (1, 1.24, false)",
        "insert into users (id, score, active) values (2, 1.26, false)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-update-order-round-scale",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let update = query_as_session(
        "req-update-users-order-round-scale",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = true order by round(score,1) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(update.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-order-round-scale",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, active from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(selected),
        vec![
            vec!["1".to_string(), "true".to_string()],
            vec!["2".to_string(), "false".to_string()],
        ]
    );
}

#[test]
fn delete_order_by_lower_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-delete-order-lower",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, name varchar(32))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, name) values (1, 'B')",
        "insert into users (id, name) values (2, 'a')",
    ] {
        let inserted = query_as_session(
            "req-insert-users-delete-order-lower",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let deleted = query_as_session(
        "req-delete-users-order-lower",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users order by lower(name) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(deleted.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-delete-order-lower",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(query_result_rows(selected), vec![vec!["1".to_string()]]);
}

#[test]
fn delete_order_by_length_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-delete-order-length",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, name varchar(32))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, name) values (1, 'aa')",
        "insert into users (id, name) values (2, 'bbbb')",
    ] {
        let inserted = query_as_session(
            "req-insert-users-delete-order-length",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let deleted = query_as_session(
        "req-delete-users-order-length",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users order by length(name) desc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(deleted.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-delete-order-length",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(query_result_rows(selected), vec![vec!["1".to_string()]]);
}

#[test]
fn delete_order_by_ltrim_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-delete-order-ltrim",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, name varchar(32))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, name) values (1, '  b')",
        "insert into users (id, name) values (2, ' a')",
    ] {
        let inserted = query_as_session(
            "req-insert-users-delete-order-ltrim",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let deleted = query_as_session(
        "req-delete-users-order-ltrim",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users order by ltrim(name) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(deleted.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-delete-order-ltrim",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(query_result_rows(selected), vec![vec!["1".to_string()]]);
}

#[test]
fn delete_order_by_floor_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-delete-order-floor",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, score double)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, score) values (1, 1.9)",
        "insert into users (id, score) values (2, 1.1)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-delete-order-floor",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let deleted = query_as_session(
        "req-delete-users-order-floor",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users order by floor(score) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(deleted.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-delete-order-floor",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(query_result_rows(selected), vec![vec!["2".to_string()]]);
}

#[test]
fn delete_order_by_round_with_scale_expression_is_applied_before_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-delete-order-round-scale",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, score double)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for sql in [
        "insert into users (id, score) values (1, 1.24)",
        "insert into users (id, score) values (2, 1.26)",
    ] {
        let inserted = query_as_session(
            "req-insert-users-delete-order-round-scale",
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(inserted.status, connector::ResponseStatus::Applied));
    }

    let deleted = query_as_session(
        "req-delete-users-order-round-scale",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users order by round(score,1) asc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(deleted.status, connector::ResponseStatus::Applied));

    let selected = query_as_session(
        "req-select-users-delete-order-round-scale",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(query_result_rows(selected), vec![vec!["2".to_string()]]);
}

#[test]
fn insert_ignore_with_on_duplicate_key_update_is_supported() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-ignore",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-ignore-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'seed@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-ignore",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert ignore into users (id, email) values (1, 'incoming@example.com') on duplicate key update email = values(email) returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(upsert);
    assert_eq!(
        rows,
        vec![vec!["1".to_string(), "incoming@example.com".to_string()]]
    );
}

#[test]
fn insert_on_duplicate_key_update_rejects_primary_key_assignment() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-pk",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-upsert-pk-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'a@example.com')".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-pk",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'dup@example.com') on duplicate key update id = 2"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(upsert.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = upsert.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("cannot modify primary key fields"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn insert_on_duplicate_key_update_handles_intra_statement_duplicate_keys() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = query_as_session(
        "req-create-users-upsert-staged",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let upsert = query_as_session(
        "req-insert-upsert-staged",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email) values (1, 'first@example.com'), (1, 'dup@example.com'), (2, 'two@example.com') on duplicate key update email = 'staged-updated@example.com'".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(upsert.status, connector::ResponseStatus::Applied));

    let rows = query_result_rows(query_as_session(
        "req-select-upsert-staged",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, email from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    ));

    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "staged-updated@example.com".to_string()],
            vec!["2".to_string(), "two@example.com".to_string()],
        ]
    );
}

#[test]
fn update_returning_returns_updated_rows() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog = DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
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
                    field_name: "active".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("users table should register");

    let users = catalog.table("users").expect("users table should exist");
    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());
    row.insert("active".to_string(), b"false".to_vec());
    let payload = serverlib::encode_row_payload(users.schema(), &row).expect("payload should encode");
    super::core::append_row_payload_record(
        &catalog,
        &wal,
        "users",
        users,
        &mut runtime_indexes,
        TransactionKind::Insert,
        payload,
        common::epoch_nanos!(),
        None,
        None,
    )
    .expect("row append should succeed");

    catalogs.insert("main".to_string(), catalog);

    let response = query_as_session(
        "req-update-returning",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = true where id = 1 returning id, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string(), "true".to_string()]]);
}

#[test]
fn update_supports_existing_column_assignment_reference() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = query_as_session(
        "req-create-users-update-col-ref",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64), nickname varchar(64))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-users-update-col-ref",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, email, nickname) values (1, 'seed@example.com', 'alias@example.com')"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let update = query_as_session(
        "req-update-users-col-ref",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set email = nickname where id = 1 returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(update),
        vec![vec!["1".to_string(), "alias@example.com".to_string()]]
    );
}

#[test]
fn update_supports_arithmetic_assignment_expression() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = query_as_session(
        "req-create-users-update-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, login_count int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let seed = query_as_session(
        "req-insert-users-update-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id, login_count) values (1, 10)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(seed.status, connector::ResponseStatus::Applied));

    let update = query_as_session(
        "req-update-users-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set login_count = login_count + 2 where id = 1 returning id, login_count"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(
        matches!(update.status, connector::ResponseStatus::Applied),
        "unexpected update response: {:?}",
        update
    );

    let verify = query_as_session(
        "req-select-users-arithmetic",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, login_count from users where id = 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(verify),
        vec![vec!["1".to_string(), "12".to_string()]]
    );
}

#[test]
fn update_order_by_limit_updates_only_ranked_subset() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = query_as_session(
        "req-create-users-update-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, active varchar(8))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-users-update-order-limit-1",
            "insert into users (id, active) values (1, 'false')",
        ),
        (
            "req-insert-users-update-order-limit-2",
            "insert into users (id, active) values (2, 'false')",
        ),
        (
            "req-insert-users-update-order-limit-3",
            "insert into users (id, active) values (3, 'false')",
        ),
    ] {
        let response = query_as_session(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(response.status, connector::ResponseStatus::Applied));
    }

    let update_response = query_as_session(
        "req-update-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = 'true' order by id desc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(
        matches!(update_response.status, connector::ResponseStatus::Applied),
        "unexpected update response: {:?}",
        update_response
    );

    let rows = query_result_rows(query_as_session(
        "req-select-users-update-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, active from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    ));

    assert_eq!(
        rows,
        vec![
            vec!["1".to_string(), "false".to_string()],
            vec!["2".to_string(), "false".to_string()],
            vec!["3".to_string(), "true".to_string()],
        ]
    );
}

#[test]
fn delete_returning_returns_deleted_rows() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog = DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalog
        .register_table(
            "users",
            TableSchema::new(vec![
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
                    field_name: "email".to_string(),
                    field_type: FieldType::Text,
                    nullable: false,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                },
            ]),
        )
        .expect("users table should register");

    let users = catalog.table("users").expect("users table should exist");
    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());
    row.insert("email".to_string(), b"sam@example.com".to_vec());
    let payload = serverlib::encode_row_payload(users.schema(), &row).expect("payload should encode");
    super::core::append_row_payload_record(
        &catalog,
        &wal,
        "users",
        users,
        &mut runtime_indexes,
        TransactionKind::Insert,
        payload,
        common::epoch_nanos!(),
        None,
        None,
    )
    .expect("row append should succeed");

    catalogs.insert("main".to_string(), catalog);

    let response = query_as_session(
        "req-delete-returning",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users where id = 1 returning id, email".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["1".to_string(), "sam@example.com".to_string()]]);
}

#[test]
fn delete_order_by_limit_deletes_only_ranked_subset() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = query_as_session(
        "req-create-users-delete-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-users-delete-order-limit-1",
            "insert into users (id, email) values (1, 'a@example.com')",
        ),
        (
            "req-insert-users-delete-order-limit-2",
            "insert into users (id, email) values (2, 'b@example.com')",
        ),
        (
            "req-insert-users-delete-order-limit-3",
            "insert into users (id, email) values (3, 'c@example.com')",
        ),
    ] {
        let response = query_as_session(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
        );
        assert!(matches!(response.status, connector::ResponseStatus::Applied));
    }

    let delete_response = query_as_session(
        "req-delete-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users order by id desc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    let ConnectorResult::Mutation(delete_mutation) = delete_response.result else {
        panic!("expected mutation response");
    };
    assert_eq!(delete_mutation.affected_rows, 1);

    let remaining = query_as_session(
        "req-select-users-delete-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert_eq!(
        query_result_rows(remaining),
        vec![vec!["1".to_string()], vec!["2".to_string()]]
    );
}

#[test]
fn update_from_applies_changes_for_matching_rows() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-update-from",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, active varchar(8))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let create_audit = handle_query_command(
        "req-create-audit-update-from",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table audit (id int primary key, user_id int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_audit.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        ("req-insert-users-update-from-1", "insert into users (id, active) values (1, 'false')"),
        ("req-insert-users-update-from-2", "insert into users (id, active) values (2, 'false')"),
        ("req-insert-audit-update-from-1", "insert into audit (id, user_id) values (10, 1)"),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let update_response = handle_query_command(
        "req-update-from",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "update users set active = 'true' from audit where users.id = audit.user_id returning id, active".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(update_response);
    assert_eq!(rows, vec![vec!["1".to_string(), "true".to_string()]]);
}

#[test]
fn delete_using_removes_matching_rows() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-delete-using",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key, email varchar(64))".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let create_profiles = handle_query_command(
        "req-create-profiles-delete-using",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table profiles (id int primary key, user_id int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_profiles.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-users-delete-using-1",
            "insert into users (id, email) values (1, 'sam@example.com')",
        ),
        (
            "req-insert-users-delete-using-2",
            "insert into users (id, email) values (2, 'jane@example.com')",
        ),
        (
            "req-insert-profiles-delete-using-1",
            "insert into profiles (id, user_id) values (10, 1)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let delete_response = handle_query_command(
        "req-delete-using",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "delete from users using profiles where users.id = profiles.user_id returning id".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    let rows = query_result_rows(delete_response);
    assert_eq!(rows, vec![vec!["1".to_string()]]);
}

#[test]
fn commit_is_accepted_as_noop_mutation() {
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

    let ConnectorResult::Mutation(result) = response.result else {
        panic!("expected mutation result")
    };

    assert_eq!(result.affected_rows, 0);
}

#[test]
fn show_slices_without_view_identifier_is_rejected() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let response = handle_query_command(
        "req-show-slices",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("show slices missing olap view identifier"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn show_slices_returns_dimension_coordinates_row_counts_and_numeric_aggregates() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-orders",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table orders (id bigint not null primary key, region varchar(32), product varchar(32), qty int, revenue int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        ("req-insert-orders-1", "insert into orders (id, region, product, qty, revenue) values (1, 'EU', 'A', 10, 100)"),
        ("req-insert-orders-2", "insert into orders (id, region, product, qty, revenue) values (2, 'EU', 'A', 20, 200)"),
        ("req-insert-orders-3", "insert into orders (id, region, product, qty, revenue) values (3, 'US', 'B', 7, 70)"),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let create_olap = handle_query_command(
        "req-create-olap",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create olapview sales_cube using region, product as select id, region, product, qty from orders".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_olap.status, connector::ResponseStatus::Applied));

    let slices = handle_query_command(
        "req-show-slices-live",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(slices.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        slices
    );

    let ConnectorResult::Query(result) = slices.result else {
        panic!("expected query result")
    };

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            vec![
                "EU".to_string(),
                "A".to_string(),
                "2".to_string(),
                "30".to_string(),
                "10".to_string(),
                "20".to_string(),
                "15".to_string(),
            ],
            vec![
                "US".to_string(),
                "B".to_string(),
                "1".to_string(),
                "7".to_string(),
                "7".to_string(),
                "7".to_string(),
                "7".to_string(),
            ],
        ]
    );

    let columns = result
        .columns
        .into_iter()
        .map(|field| field.field_name)
        .collect::<Vec<_>>();

    assert_eq!(
        columns,
        vec![
            "region".to_string(),
            "product".to_string(),
            "row_count".to_string(),
            "sum_qty".to_string(),
            "min_qty".to_string(),
            "max_qty".to_string(),
            "avg_qty".to_string(),
        ]
    );
}

#[test]
fn show_slices_numeric_aggregates_ignore_null_values() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-orders-null-agg",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table orders (id bigint not null primary key, region varchar(32), product varchar(32), qty int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-orders-null-1",
            "insert into orders (id, region, product, qty) values (1, 'EU', 'A', 10)",
        ),
        (
            "req-insert-orders-null-2",
            "insert into orders (id, region, product, qty) values (2, 'EU', 'A', null)",
        ),
        (
            "req-insert-orders-null-3",
            "insert into orders (id, region, product, qty) values (3, 'US', 'B', null)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let create_olap = handle_query_command(
        "req-create-olap-null-agg",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create olapview sales_cube using region, product as select id, region, product, qty from orders".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_olap.status, connector::ResponseStatus::Applied));

    let slices = handle_query_command(
        "req-show-slices-null-agg",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(slices.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        slices
    );

    let ConnectorResult::Query(result) = slices.result else {
        panic!("expected query result")
    };

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            vec![
                "EU".to_string(),
                "A".to_string(),
                "2".to_string(),
                "10".to_string(),
                "10".to_string(),
                "10".to_string(),
                "10".to_string(),
            ],
            vec![
                "US".to_string(),
                "B".to_string(),
                "1".to_string(),
                "NULL".to_string(),
                "NULL".to_string(),
                "NULL".to_string(),
                "NULL".to_string(),
            ],
        ]
    );
}

#[test]
fn show_slices_orders_rows_deterministically_with_null_coordinates() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-orders-null-order",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table orders (id bigint not null primary key, region varchar(32), product varchar(32), qty int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-orders-null-order-1",
            "insert into orders (id, region, product, qty) values (1, null, 'A', 10)",
        ),
        (
            "req-insert-orders-null-order-2",
            "insert into orders (id, region, product, qty) values (2, 'EU', null, 20)",
        ),
        (
            "req-insert-orders-null-order-3",
            "insert into orders (id, region, product, qty) values (3, 'EU', 'A', 30)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let create_olap = handle_query_command(
        "req-create-olap-null-order",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create olapview sales_cube using region, product as select id, region, product, qty from orders".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_olap.status, connector::ResponseStatus::Applied));

    let slices = handle_query_command(
        "req-show-slices-null-order",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(slices.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        slices
    );

    let ConnectorResult::Query(result) = slices.result else {
        panic!("expected query result")
    };

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            vec![
                "NULL".to_string(),
                "A".to_string(),
                "1".to_string(),
                "10".to_string(),
                "10".to_string(),
                "10".to_string(),
                "10".to_string(),
            ],
            vec![
                "EU".to_string(),
                "NULL".to_string(),
                "1".to_string(),
                "20".to_string(),
                "20".to_string(),
                "20".to_string(),
                "20".to_string(),
            ],
            vec![
                "EU".to_string(),
                "A".to_string(),
                "1".to_string(),
                "30".to_string(),
                "30".to_string(),
                "30".to_string(),
                "30".to_string(),
            ],
        ]
    );
}

#[test]
fn show_slices_supports_order_by_desc_and_limit() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-orders-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table orders (id bigint not null primary key, region varchar(32), product varchar(32), qty int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-orders-order-limit-1",
            "insert into orders (id, region, product, qty) values (1, 'EU', 'A', 10)",
        ),
        (
            "req-insert-orders-order-limit-2",
            "insert into orders (id, region, product, qty) values (2, 'EU', 'A', 20)",
        ),
        (
            "req-insert-orders-order-limit-3",
            "insert into orders (id, region, product, qty) values (3, 'US', 'B', 7)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let create_olap = handle_query_command(
        "req-create-olap-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create olapview sales_cube using region, product as select id, region, product, qty from orders".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_olap.status, connector::ResponseStatus::Applied));

    let slices = handle_query_command(
        "req-show-slices-order-limit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube order by sum_qty desc limit 1".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(slices.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        slices
    );

    let ConnectorResult::Query(result) = slices.result else {
        panic!("expected query result")
    };

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![vec![
            "EU".to_string(),
            "A".to_string(),
            "2".to_string(),
            "30".to_string(),
            "10".to_string(),
            "20".to_string(),
            "15".to_string(),
        ]]
    );
}

#[test]
fn show_slices_rejects_malformed_order_by_clause() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let response = handle_query_command(
        "req-show-slices-bad-order-by",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube order".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("ORDER BY clause is malformed"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn show_slices_supports_where_filtering_on_slice_columns() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-orders-where",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table orders (id bigint not null primary key, region varchar(32), product varchar(32), qty int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-orders-where-1",
            "insert into orders (id, region, product, qty) values (1, 'EU', 'A', 10)",
        ),
        (
            "req-insert-orders-where-2",
            "insert into orders (id, region, product, qty) values (2, 'EU', 'A', 20)",
        ),
        (
            "req-insert-orders-where-3",
            "insert into orders (id, region, product, qty) values (3, 'US', 'B', 7)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );
        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let create_olap = handle_query_command(
        "req-create-olap-where",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create olapview sales_cube using region, product as select id, region, product, qty from orders".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_olap.status, connector::ResponseStatus::Applied));

    let slices = handle_query_command(
        "req-show-slices-where",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube where region = 'eu' and sum_qty >= 30".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(slices.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        slices
    );

    let ConnectorResult::Query(result) = slices.result else {
        panic!("expected query result")
    };

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![vec![
            "EU".to_string(),
            "A".to_string(),
            "2".to_string(),
            "30".to_string(),
            "10".to_string(),
            "20".to_string(),
            "15".to_string(),
        ]]
    );
}

#[test]
fn show_slices_rejects_malformed_where_clause() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let response = handle_query_command(
        "req-show-slices-bad-where",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show slices from sales_cube where region".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("WHERE clause is malformed"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn select_for_update_is_supported_via_query_execution_path() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-select-for-update",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let insert_user = handle_query_command(
        "req-insert-users-select-for-update",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-select-for-update",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users for update".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));
    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn select_for_share_is_supported_via_query_execution_path() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-select-for-share",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let insert_user = handle_query_command(
        "req-insert-users-select-for-share",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-select-for-share",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users for share".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));
    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn select_for_update_join_is_supported_in_first_pass_lock_mode() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-select-for-update-join",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let create_profiles = handle_query_command(
        "req-create-profiles-select-for-update-join",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table profiles (id int primary key, user_id int)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_profiles.status, connector::ResponseStatus::Applied));

    let insert_user = handle_query_command(
        "req-insert-users-select-for-update-join",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let insert_profile = handle_query_command(
        "req-insert-profiles-select-for-update-join",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into profiles (id, user_id) values (1, 1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_profile.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-select-for-update-join",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select u.id from users u join profiles p on u.id = p.user_id for update"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));
    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn select_for_update_cte_is_supported_in_first_pass_lock_mode() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-select-for-update-cte",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let insert_user = handle_query_command(
        "req-insert-users-select-for-update-cte",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-select-for-update-cte",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with active_users as (select id from users) select id from active_users for update"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));
    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn recursive_cte_union_all_repeating_frontier_is_rejected_early() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-recursive-cte-repeating-frontier",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let insert_user = handle_query_command(
        "req-insert-users-recursive-cte-repeating-frontier",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-recursive-cte-repeating-frontier",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with recursive t(id) as (select id from users where id = 1 union all select id from t) select id from t"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };
    assert!(
        message.contains("repeating UNION ALL frontier")
            || message.contains("exceeded max iterations"),
        "unexpected recursive rejection message: {message}"
    );
}

#[test]
fn set_variable_cte_max_iterations_limits_recursive_cte_execution() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let create_table = handle_query_command(
        "req-create-edges-set-max-iterations",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table edges (id bigint not null primary key, parent_id int, child_id int)"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-edges-set-max-iterations-1",
            "insert into edges (id, parent_id, child_id) values (1, 1, 2)",
        ),
        (
            "req-insert-edges-set-max-iterations-2",
            "insert into edges (id, parent_id, child_id) values (2, 2, 3)",
        ),
        (
            "req-insert-edges-set-max-iterations-3",
            "insert into edges (id, parent_id, child_id) values (3, 3, 4)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );

        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let set_response = query_as_session_with_overrides(
        "req-set-max-iterations",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_iterations = 2".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        &mut session_overrides,
    );
    assert!(matches!(set_response.status, connector::ResponseStatus::Applied));

    let recursive_query = query_as_session_with_overrides(
        "req-recursive-cte-set-max-iterations",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with recursive chain as (select parent_id, child_id from edges where parent_id = 1 union select e.parent_id, e.child_id from edges e join chain c on e.parent_id = c.child_id) select child_id from chain order by child_id"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        &mut session_overrides,
    );

    assert!(matches!(
        recursive_query.status,
        connector::ResponseStatus::Rejected
    ));
    let ConnectorResult::Error(message) = recursive_query.result else {
        panic!("expected error result")
    };
    assert!(
        message.contains("exceeded max iterations (2)"),
        "unexpected recursive rejection message: {message}"
    );
}

#[test]
fn set_variable_cte_union_all_repeat_detection_can_be_disabled() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let create_users = query_as_session_with_overrides(
        "req-create-users-recursive-cte-repeat-toggle",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        &mut session_overrides,
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let insert_user = query_as_session_with_overrides(
        "req-insert-users-recursive-cte-repeat-toggle",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        &mut session_overrides,
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let set_response = query_as_session_with_overrides(
        "req-set-recursive-cte-repeat-toggle",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.union_all_repeat_detection = false, cte.max_iterations = 3"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        &mut session_overrides,
    );
    assert!(matches!(set_response.status, connector::ResponseStatus::Applied));

    let response = query_as_session_with_overrides(
        "req-recursive-cte-repeat-toggle",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with recursive t(id) as (select id from users where id = 1 union all select id from t) select id from t"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        &mut session_overrides,
    );

    assert!(matches!(response.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };
    assert!(
        message.contains("exceeded max iterations (3)"),
        "unexpected recursive rejection message: {message}"
    );
    assert!(
        !message.contains("repeating UNION ALL frontier"),
        "repeat-frontier detection should be disabled: {message}"
    );
}

#[test]
fn show_variables_includes_recursive_cte_runtime_controls() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let response = handle_query_command_with_session_variables(
        "req-show-variables-cte",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variables".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));

    let rows = query_result_rows(response);
    assert!(rows.iter().any(|row| {
        row.len() == 2 && row[0] == "cte.max_iterations" && row[1] == "128"
    }));
    assert!(rows.iter().any(|row| {
        row.len() == 2 && row[0] == "cte.max_rows" && row[1] == "50000"
    }));
    assert!(rows.iter().any(|row| {
        row.len() == 2 && row[0] == "cte.timeout_ms" && row[1] == "0"
    }));
    assert!(rows.iter().any(|row| {
        row.len() == 2 && row[0] == "cte.union_all_repeat_detection" && row[1] == "true"
    }));
}

#[test]
fn show_variable_reflects_set_variable_updates() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let set_response = handle_query_command_with_session_variables(
        "req-set-variables-show-variable",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.timeout_ms = 42, cte.max_rows = 321".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );
    assert!(matches!(set_response.status, connector::ResponseStatus::Applied));

    let show_timeout = handle_query_command_with_session_variables(
        "req-show-variable-timeout",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variable cte.timeout_ms".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(show_timeout.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(show_timeout),
        vec![vec!["cte.timeout_ms".to_string(), "42".to_string()]]
    );

    let show_max_rows = handle_query_command_with_session_variables(
        "req-show-variable-max-rows",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variable @@session.cte.max_rows".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(show_max_rows.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(show_max_rows),
        vec![vec!["cte.max_rows".to_string(), "321".to_string()]]
    );
}

#[test]
fn set_variable_rejects_out_of_range_values() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let response = query_as_session(
        "req-set-variable-out-of-range",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.timeout_ms = 3600001".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(response.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("cte.timeout_ms is out of allowed range"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn set_variable_rejects_duplicate_assignments() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let response = query_as_session(
        "req-set-variable-duplicate-assignment",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_rows = 100, cte.max_rows = 200".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(response.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("duplicate variable assignment"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn set_variable_rejects_unsupported_global_scope() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let response = query_as_session(
        "req-set-variable-global-scope-reject",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set @@global.cte.max_rows = 100".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
    );

    assert!(matches!(response.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("does not support 'global' scope"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn set_variable_accepts_explicit_session_scope() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let set_response = handle_query_command_with_session_variables(
        "req-set-variable-session-scope",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set session cte.max_rows = 333".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );
    assert!(matches!(set_response.status, connector::ResponseStatus::Applied));

    let show_response = handle_query_command_with_session_variables(
        "req-show-variable-session-scope",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variable cte.max_rows".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(show_response.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(show_response),
        vec![vec!["cte.max_rows".to_string(), "333".to_string()]]
    );
}

#[test]
fn set_variable_does_not_bleed_between_sessions() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_a_overrides = SessionVariableOverrides::new();
    let mut session_b_overrides = SessionVariableOverrides::new();

    let set_session_a = handle_query_command_with_session_variables(
        "req-set-session-a-max-rows",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_rows = 333".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-a",
        1,
        Some("root@localhost".to_string()),
        &mut session_a_overrides,
    );
    assert!(matches!(set_session_a.status, connector::ResponseStatus::Applied));

    let show_session_a = handle_query_command_with_session_variables(
        "req-show-session-a-max-rows",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variable cte.max_rows".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-a",
        1,
        Some("root@localhost".to_string()),
        &mut session_a_overrides,
    );
    assert!(matches!(show_session_a.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(show_session_a),
        vec![vec!["cte.max_rows".to_string(), "333".to_string()]]
    );

    let show_session_b = handle_query_command_with_session_variables(
        "req-show-session-b-max-rows",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variable cte.max_rows".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-b",
        2,
        Some("root@localhost".to_string()),
        &mut session_b_overrides,
    );
    assert!(matches!(show_session_b.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(show_session_b),
        vec![vec!["cte.max_rows".to_string(), "50000".to_string()]]
    );
}

#[test]
fn udf_style_function_expression_can_read_runtime_variables() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let set_response = handle_query_command_with_session_variables(
        "req-set-runtime-var-for-udf-read",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_rows = 345".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-udf",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );
    assert!(matches!(set_response.status, connector::ResponseStatus::Applied));

    let read_response = handle_query_command_with_session_variables(
        "req-read-runtime-var-via-udf-style-fn",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select coalesce(cte.max_rows, 0) as runtime_value".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-udf",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(read_response.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(read_response),
        vec![vec!["345".to_string()]]
    );
}

#[test]
fn call_procedure_can_read_runtime_variables_in_session_context() {
    let mut catalog = DatabaseCatalog::create_empty_from_name("main")
        .expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "p_read_runtime_var",
            "create procedure p_read_runtime_var(out p_out bigint) begin set p_out = cte.max_rows; end",
            Vec::new(),
        )
        .expect("procedure should be registered");

    let mut catalogs = HashMap::new();
    catalogs.insert("main".to_string(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let set_response = handle_query_command_with_session_variables(
        "req-set-runtime-var-for-procedure-read",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_rows = 456".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-proc",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );
    assert!(matches!(set_response.status, connector::ResponseStatus::Applied));

    let call_response = handle_query_command_with_session_variables(
        "req-call-read-runtime-var-procedure",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "call p_read_runtime_var(out_slot)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-proc",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(call_response.status, connector::ResponseStatus::Applied));

    let columns = query_result_columns(call_response.clone());
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].field_name, "out_slot");

    assert_eq!(
        query_result_rows(call_response),
        vec![vec!["456".to_string()]]
    );
}

#[test]
fn set_variable_rejects_conflicting_statement_and_assignment_scopes() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let response = handle_query_command(
        "req-set-variable-scope-conflict",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set session @@local.cte.max_rows = 50".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("conflicting scope"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn set_variable_rolls_back_session_overrides_on_failure() {

    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();
    let mut session_overrides = SessionVariableOverrides::new();

    let initial_set = handle_query_command_with_session_variables(
        "req-set-variable-rollback-seed",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_rows = 333".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );
    
    assert!(matches!(initial_set.status, connector::ResponseStatus::Applied));

    let failing_set = handle_query_command_with_session_variables(
        "req-set-variable-rollback-failure",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "set cte.max_rows = 444, cte.timeout_ms = 3600001".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(failing_set.status, connector::ResponseStatus::Rejected));
    let ConnectorResult::Error(message) = failing_set.result else {
        panic!("expected error result")
    };
    assert!(
        message.contains("cte.timeout_ms is out of allowed range"),
        "unexpected rejection message: {message}"
    );

    let show_response = handle_query_command_with_session_variables(
        "req-show-variable-rollback-check",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variable cte.max_rows".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
        &mut session_overrides,
    );

    assert!(matches!(show_response.status, connector::ResponseStatus::Applied));
    assert_eq!(
        query_result_rows(show_response),
        vec![vec!["cte.max_rows".to_string(), "333".to_string()]]
    );
}

#[test]
fn show_variables_like_filters_recursive_cte_controls() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let response = handle_query_command(
        "req-show-variables-like-cte-max",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show variables like 'cte.max_%'".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));

    let rows = query_result_rows(response);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows,
        vec![
            vec!["cte.max_iterations".to_string(), "128".to_string()],
            vec!["cte.max_rows".to_string(), "50000".to_string()],
        ]
    );
}

#[test]
fn select_sql_no_cache_modifier_is_accepted_in_compat_mode() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_users = handle_query_command(
        "req-create-users-select-sql-no-cache",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id int primary key)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_users.status, connector::ResponseStatus::Applied));

    let insert_user = handle_query_command(
        "req-insert-users-select-sql-no-cache",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "insert into users (id) values (1)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(insert_user.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-select-sql-no-cache",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select sql_no_cache id from users".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(response.status, connector::ResponseStatus::Applied));
    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn create_unique_index_is_rejected_until_unique_semantics_are_supported() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-users-unique-index",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id bigint not null primary key, email varchar(64))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-create-unique-index",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create unique index idx_users_email on users(email)".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.contains("CREATE UNIQUE INDEX is not supported yet"),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn create_index_with_using_clause_is_rejected_until_supported() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-users-index-using",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table users (id bigint not null primary key, email varchar(64))"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    let response = handle_query_command(
        "req-create-index-using",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create index idx_users_email on users(email) using btree".to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(
        message.to_ascii_lowercase().contains("using")
            && (
                message.contains("CREATE INDEX USING is not supported yet")
                || message.contains("sql parse failed")
            ),
        "unexpected rejection message: {message}"
    );
}

#[test]
fn recursive_cte_executes_hierarchy_expansion() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-edges",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table edges (id bigint not null primary key, parent_id int, child_id int)"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-edges-1",
            "insert into edges (id, parent_id, child_id) values (1, 1, 2)",
        ),
        (
            "req-insert-edges-2",
            "insert into edges (id, parent_id, child_id) values (2, 2, 3)",
        ),
        (
            "req-insert-edges-3",
            "insert into edges (id, parent_id, child_id) values (3, 3, 4)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );

        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let recursive_query = handle_query_command(
        "req-recursive-cte",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with recursive chain as (select parent_id, child_id from edges where parent_id = 1 union select e.parent_id, e.child_id from edges e join chain c on e.parent_id = c.child_id) select child_id from chain order by child_id"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(recursive_query.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        recursive_query
    );

    let rows = query_result_rows(recursive_query);

    assert_eq!(
        rows,
        vec![
            vec!["2".to_string()],
            vec!["3".to_string()],
            vec!["4".to_string()],
        ]
    );
}

#[test]
fn recursive_cte_union_all_executes_hierarchy_expansion() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let node_data_dir = test_node_data_dir();

    let create_table = handle_query_command(
        "req-create-edges-union-all",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create table edges (id bigint not null primary key, parent_id int, child_id int)"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );
    assert!(matches!(create_table.status, connector::ResponseStatus::Applied));

    for (request_id, sql) in [
        (
            "req-insert-edges-union-all-1",
            "insert into edges (id, parent_id, child_id) values (1, 1, 2)",
        ),
        (
            "req-insert-edges-union-all-2",
            "insert into edges (id, parent_id, child_id) values (2, 2, 3)",
        ),
        (
            "req-insert-edges-union-all-3",
            "insert into edges (id, parent_id, child_id) values (3, 3, 4)",
        ),
    ] {
        let response = handle_query_command(
            request_id,
            &DataQuery {
                database_id: "main".to_string(),
                sql: sql.to_string(),
            },
            &mut catalogs,
            &wal,
            &node_data_dir,
            &mut runtime_indexes,
            "session-test",
            1,
            Some("root@localhost".to_string()),
        );

        assert!(
            matches!(response.status, connector::ResponseStatus::Applied),
            "unexpected response for {request_id}: {:?}",
            response
        );
    }

    let recursive_query = handle_query_command(
        "req-recursive-cte-union-all",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "with recursive chain as (select parent_id, child_id from edges where parent_id = 1 union all select e.parent_id, e.child_id from edges e join chain c on e.parent_id = c.child_id) select child_id from chain order by child_id"
                .to_string(),
        },
        &mut catalogs,
        &wal,
        &node_data_dir,
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(recursive_query.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        recursive_query
    );

    let rows = query_result_rows(recursive_query);

    assert_eq!(
        rows,
        vec![
            vec!["2".to_string()],
            vec!["3".to_string()],
            vec!["4".to_string()],
        ]
    );
}

#[test]
fn call_procedure_executes_branch_sql_as_smoke_test() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_sync",
            "create procedure p_sync() begin if active = 1 then select abs(42); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_sync()".to_string(),
    };

    let response = handle_query_command(
        "req-call-smoke",
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
    assert_eq!(rows, vec![vec!["0".to_string()]]);
}

#[test]
fn local_function_name_is_checked_before_inbuilt_resolution() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "abs",
            "create function abs(p_value int) returns int return p_value",
            Vec::new(),
        )
        .expect("local function should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "select abs(7)".to_string(),
    };

    let response = handle_query_command(
        "req-local-fn-first",
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
    assert_eq!(rows, vec![vec!["7".to_string()]]);
}

#[test]
fn create_select_and_drop_function_work_end_to_end() {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let create_response = handle_query_command(
        "req-create-fn",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "create function f_add_one(p_value int) returns int return p_value".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(create_response.status, connector::ResponseStatus::Applied),
        "unexpected create response: {:?}",
        create_response
    );

    let select_response = handle_query_command(
        "req-select-fn",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select f_add_one(41)".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(select_response.status, connector::ResponseStatus::Applied),
        "unexpected select response: {:?}",
        select_response
    );

    let rows = query_result_rows(select_response);
    assert_eq!(rows, vec![vec!["41".to_string()]]);

    let drop_response = handle_query_command(
        "req-drop-fn",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "drop function f_add_one".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(drop_response.status, connector::ResponseStatus::Applied),
        "unexpected drop response: {:?}",
        drop_response
    );
}

#[test]
fn drop_inbuilt_function_is_rejected_and_inbuilt_remains_callable() {

    let mut catalogs = HashMap::new();
    catalogs.insert(
        "main".to_string(),
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created"),
    );

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let drop_response = handle_query_command(
        "req-drop-inbuilt-abs",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "drop function abs".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(drop_response.status, connector::ResponseStatus::Rejected),
        "unexpected drop response: {:?}",
        drop_response
    );

    let ConnectorResult::Error(message) = drop_response.result else {
        panic!("expected error result");
    };

    assert!(
        message.contains("drop function failed: 'abs' not found"),
        "unexpected drop rejection message: {message}"
    );

    let select_response = handle_query_command(
        "req-select-inbuilt-abs",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select abs(-7)".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(select_response.status, connector::ResponseStatus::Applied),
        "unexpected select response after failed drop: {:?}",
        select_response
    );

    let rows = query_result_rows(select_response);
    assert_eq!(rows, vec![vec!["7".to_string()]]);
    
}

#[test]
fn call_procedure_tears_down_scoped_temporary_tables() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_temp",
            "create procedure p_temp() begin if active = 1 then create temporary table tmp_users (id bigint primary key); else create temporary table tmp_users (id bigint primary key); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id.clone(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_temp()".to_string(),
    };

    let response = handle_query_command(
        "req-call-temp",
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

    let catalog = catalogs
        .values()
        .next()
        .expect("catalog should be present after call");

    assert!(catalog
        .table_ids()
        .into_iter()
        .all(|table_id| !table_id.contains("tmp_users")));
}

#[test]
fn call_procedure_temp_table_insert_and_select_returns_rows_and_cleans_up() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_temp_items",
            "create procedure p_temp_items() begin if active = 1 then create temporary table tmp_items (id bigint primary key); insert into tmp_items (id) values (1); select id from tmp_items order by id; else create temporary table tmp_items (id bigint primary key); insert into tmp_items (id) values (11); insert into tmp_items (id) values (22); select id from tmp_items order by id; end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id.clone(), catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_temp_items()".to_string(),
    };

    let response = handle_query_command(
        "req-call-temp-items",
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

    let ConnectorResult::Query(result) = response.result else {
        panic!("expected query response from CALL")
    };

    let column_names = result
        .columns
        .iter()
        .map(|field| field.field_name.clone())
        .collect::<Vec<_>>();
    assert_eq!(column_names, vec!["id".to_string()]);

    let rows = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| String::from_utf8(cell).expect("cell should be utf8"))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    assert_eq!(rows, vec![vec!["11".to_string()], vec!["22".to_string()]]);

    let catalog = catalogs
        .values()
        .next()
        .expect("catalog should be present after call");

    assert!(catalog
        .table_ids()
        .into_iter()
        .all(|table_id| !table_id.contains("tmp_items")));
}

#[test]
fn call_procedure_argument_bindings_do_not_bleed_between_calls() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_arg_scope",
            "create procedure p_arg_scope(p_active bigint) begin if p_active = 1 then select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let call_on = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_arg_scope(1)".to_string(),
    };

    let response_on = handle_query_command(
        "req-call-arg-on",
        &call_on,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response_on.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        response_on
    );
    assert_eq!(query_result_rows(response_on), vec![vec!["1".to_string()]]);

    let call_off = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_arg_scope(0)".to_string(),
    };

    let response_off = handle_query_command(
        "req-call-arg-off",
        &call_off,
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    );

    assert!(
        matches!(response_off.status, connector::ResponseStatus::Applied),
        "unexpected response: {:?}",
        response_off
    );
    assert_eq!(query_result_rows(response_off), vec![vec!["0".to_string()]]);
}

#[test]
fn call_procedure_returns_out_parameter_values_when_no_resultset_is_emitted() {
    
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_out_value",
            "create procedure p_out_value(in p_in bigint, out p_out bigint) begin set p_out = p_in; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let call_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_out_value(7, out_slot)".to_string(),
    };

    let response = handle_query_command(
        "req-call-out-value",
        &call_query,
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

    let columns = query_result_columns(response.clone());
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].field_name, "out_slot");
    assert_eq!(columns[0].field_type, FieldType::Text);

    assert_eq!(query_result_rows(response), vec![vec!["7".to_string()]]);

}

#[test]
fn call_procedure_returns_inout_parameter_values_when_no_resultset_is_emitted() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_inout_value",
            "create procedure p_inout_value(inout p_state bigint) begin set p_state = 9; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let call_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_inout_value(state_slot)".to_string(),
    };

    let response = handle_query_command(
        "req-call-inout-value",
        &call_query,
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

    let columns = query_result_columns(response.clone());
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].field_name, "state_slot");
    assert_eq!(columns[0].field_type, FieldType::Text);

    assert_eq!(query_result_rows(response), vec![vec!["9".to_string()]]);
}

#[test]
fn call_procedure_supports_local_declare_and_set_statements() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_local_scope",
            "create procedure p_local_scope(p_active bigint) begin if p_active = 1 then declare v_state bigint default 0; set v_state = p_active; else declare v_state bigint default 9; set v_state = 7; end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_local_scope(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-local-scope",
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

    let ConnectorResult::Mutation(mutation) = response.result else {
        panic!("expected mutation response from CALL with local declare/set actions")
    };

    assert_eq!(mutation.affected_rows, 0);
}

#[test]
fn call_procedure_supports_while_with_local_variable_resolution() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_while_local",
            "create procedure p_while_local(p_active bigint) begin if p_active = 1 then while p_active = 1 do set p_active = 0 end while; select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_while_local(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-while-local",
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

    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn call_procedure_supports_repeat_with_local_variable_resolution() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_repeat_local",
            "create procedure p_repeat_local(p_active bigint) begin if p_active = 1 then repeat set p_active = 0 until p_active = 0 end repeat; select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_repeat_local(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-repeat-local",
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

    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn call_procedure_supports_loop_with_leave_directive() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_loop_leave",
            "create procedure p_loop_leave(p_active bigint) begin if p_active = 1 then loop set p_active = 0; leave; end loop; select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_loop_leave(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-loop-leave",
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

    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn call_procedure_supports_iterate_directive_in_while_body() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_iterate_while",
            "create procedure p_iterate_while(p_active bigint) begin if p_active = 1 then while p_active = 1 do set p_active = 0; iterate; end while; select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_iterate_while(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-loop-iterate",
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

    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn call_procedure_supports_declare_continue_handler_for_sqlexception() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_handler_continue",
            "create procedure p_handler_continue(p_active bigint) begin if p_active = 1 then declare continue handler for sqlexception select abs(2); drop table missing_handler_table; else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_handler_continue(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-handler-continue",
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

    assert_eq!(query_result_rows(response), vec![vec!["2".to_string()]]);
}

#[test]
fn call_procedure_supports_declare_exit_handler_for_sqlexception() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_handler_exit",
            "create procedure p_handler_exit(p_active bigint) begin if p_active = 1 then declare exit handler for sqlexception select abs(7); drop table missing_handler_table; select abs(9); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_handler_exit(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-handler-exit",
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

    assert_eq!(query_result_rows(response), vec![vec!["7".to_string()]]);
}

#[test]
fn call_procedure_supports_declare_continue_handler_for_sqlwarning() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_handler_sqlwarning_continue",
            "create procedure p_handler_sqlwarning_continue(p_active bigint) begin if p_active = 1 then declare continue handler for sqlwarning select abs(4); drop table missing_handler_table; else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_handler_sqlwarning_continue(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-handler-sqlwarning-continue",
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

    assert_eq!(query_result_rows(response), vec![vec!["4".to_string()]]);
}

#[test]
fn call_procedure_supports_declare_exit_handler_for_sqlwarning() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_handler_sqlwarning_exit",
            "create procedure p_handler_sqlwarning_exit(p_active bigint) begin if p_active = 1 then declare exit handler for sqlwarning select abs(8); drop table missing_handler_table; select abs(9); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_handler_sqlwarning_exit(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-handler-sqlwarning-exit",
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

    assert_eq!(query_result_rows(response), vec![vec!["8".to_string()]]);
}

#[test]
fn call_procedure_supports_labeled_begin_with_leave_label() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_begin_leave_label",
            "create procedure p_begin_leave_label(p_active bigint) begin if p_active = 1 then outer_block: begin leave outer_block; select abs(9); end; select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_begin_leave_label(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-begin-leave-label",
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

    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn call_procedure_rejects_leave_with_unknown_label() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

    catalog
        .register_stored_procedure(
            "p_leave_bad_label",
            "create procedure p_leave_bad_label(p_active bigint) begin if p_active = 1 then loop leave missing_label; end loop; else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();
    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_leave_bad_label(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-bad-leave-label",
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
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(message.contains("LEAVE target label 'missing_label' is not active"));
}

#[test]
fn call_procedure_supports_cursor_fetch_with_not_found_handler() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

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

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    for id in ["11", "22"] {
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

    catalog
        .register_stored_procedure(
            "p_cursor_not_found",
            "create procedure p_cursor_not_found(p_active bigint) begin if p_active = 1 then declare v_id bigint default 0; declare c cursor for select id from users order by id; declare continue handler for not found select abs(99); open c; fetch c into v_id; fetch c into v_id; fetch c into v_id; close c; select abs(1); else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_cursor_not_found(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-cursor-not-found",
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

    assert_eq!(query_result_rows(response), vec![vec!["1".to_string()]]);
}

#[test]
fn call_procedure_rejects_unhandled_cursor_not_found() {
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    let db_id = catalog.database_id.0.clone();

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

    let table = catalog.table("users").expect("users table should exist");
    let mut row = HashMap::new();
    row.insert("id".to_string(), b"1".to_vec());

    let payload =
        serverlib::encode_row_payload(table.schema(), &row).expect("row payload should encode");

    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

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

    catalog
        .register_stored_procedure(
            "p_cursor_unhandled",
            "create procedure p_cursor_unhandled(p_active bigint) begin if p_active = 1 then declare v_id bigint default 0; declare c cursor for select id from users order by id; open c; fetch c into v_id; fetch c into v_id; close c; select v_id; else select abs(0); end if; end",
            Vec::new(),
        )
        .expect("procedure should register");

    let mut catalogs = HashMap::new();
    catalogs.insert(db_id, catalog);

    let data_query = DataQuery {
        database_id: "main".to_string(),
        sql: "call p_cursor_unhandled(1)".to_string(),
    };

    let response = handle_query_command(
        "req-call-cursor-unhandled",
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
        matches!(response.status, connector::ResponseStatus::Rejected),
        "unexpected response: {:?}",
        response
    );

    let ConnectorResult::Error(message) = response.result else {
        panic!("expected error result")
    };

    assert!(message.contains("cursor fetch reached end of result set"));
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
fn select_fetch_first_rows_only_limits_single_relation_query() {
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
        "req-select-fetch-single-relation",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id desc fetch first 2 rows only".to_string(),
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
fn select_fetch_first_rows_with_ties_keeps_boundary_ties() {
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
            field_name: "score".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for (id, score) in [("1", "99"), ("2", "90"), ("3", "90"), ("4", "80")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("score".to_string(), score.as_bytes().to_vec());

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
        "req-select-fetch-with-ties-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, score from users order by score desc fetch first 2 rows with ties"
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
            vec!["1".to_string(), "99".to_string()],
            vec!["2".to_string(), "90".to_string()],
            vec!["3".to_string(), "90".to_string()],
        ]
    );
}

#[test]
fn select_fetch_first_percent_caps_rows() {
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

    for id in ["1", "2", "3", "4"] {
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
        "req-select-fetch-percent-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users order by id fetch first 50 percent rows only".to_string(),
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
fn select_fetch_first_percent_with_ties_keeps_boundary_ties() {
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
            field_name: "score".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for (id, score) in [("1", "99"), ("2", "90"), ("3", "90"), ("4", "80")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("score".to_string(), score.as_bytes().to_vec());

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
        "req-select-fetch-percent-with-ties-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, score from users order by score desc fetch first 50 percent rows with ties"
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
            vec!["1".to_string(), "99".to_string()],
            vec!["2".to_string(), "90".to_string()],
            vec!["3".to_string(), "90".to_string()],
        ]
    );
}

#[test]
fn union_query_fetch_with_ties_keeps_boundary_ties() {
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
            field_name: "score".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived users table should register");

    for (table_id, id, score) in [
        ("users", "1", "95"),
        ("users", "2", "90"),
        ("archived_users", "3", "90"),
        ("archived_users", "4", "85"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("score".to_string(), score.as_bytes().to_vec());

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
        "req-union-fetch-with-ties-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, score from users union all select id, score from archived_users order by score desc fetch first 2 rows with ties".to_string(),
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
            vec!["1".to_string(), "95".to_string()],
            vec!["2".to_string(), "90".to_string()],
            vec!["3".to_string(), "90".to_string()],
        ]
    );
}

#[test]
fn union_query_fetch_percent_caps_rows() {
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
        .expect("archived users table should register");

    for (table_id, id) in [
        ("users", "1"),
        ("users", "2"),
        ("archived_users", "3"),
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
        "req-union-fetch-percent-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union all select id from archived_users order by id fetch first 50 percent rows only".to_string(),
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
fn union_query_fetch_percent_with_ties_keeps_boundary_ties() {
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
            field_name: "score".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema.clone())
        .expect("users table should register");
    catalog
        .register_table("archived_users", schema)
        .expect("archived users table should register");

    for (table_id, id, score) in [
        ("users", "1", "95"),
        ("users", "2", "90"),
        ("archived_users", "3", "90"),
        ("archived_users", "4", "85"),
    ] {
        let table = catalog.table(table_id).expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("score".to_string(), score.as_bytes().to_vec());

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
        "req-union-fetch-percent-with-ties-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, score from users union all select id, score from archived_users order by score desc fetch first 50 percent rows with ties".to_string(),
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
            vec!["1".to_string(), "95".to_string()],
            vec!["2".to_string(), "90".to_string()],
            vec!["3".to_string(), "90".to_string()],
        ]
    );
}

#[test]
fn select_top_limits_rows() {
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
        "req-select-top-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select top 2 id from users order by id desc".to_string(),
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
fn select_top_percent_caps_rows() {
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

    for id in ["1", "2", "3", "4"] {
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
        "req-select-top-percent-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select top 50 percent id from users order by id".to_string(),
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
fn select_top_percent_with_ties_keeps_boundary_ties() {
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
            field_name: "score".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for (id, score) in [("1", "99"), ("2", "90"), ("3", "90"), ("4", "80")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("score".to_string(), score.as_bytes().to_vec());

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
        "req-select-top-percent-with-ties-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select top 50 percent with ties id, score from users order by score desc"
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
            vec!["1".to_string(), "99".to_string()],
            vec!["2".to_string(), "90".to_string()],
            vec!["3".to_string(), "90".to_string()],
        ]
    );
}

#[test]
fn select_top_with_ties_keeps_boundary_ties() {
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
            field_name: "score".to_string(),
            field_type: FieldType::Int(32),
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        },
    ]);

    catalog
        .register_table("users", schema)
        .expect("users table should register");

    for (id, score) in [("1", "99"), ("2", "90"), ("3", "90"), ("4", "80")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("score".to_string(), score.as_bytes().to_vec());

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
        "req-select-top-with-ties-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select top 2 with ties id, score from users order by score desc".to_string(),
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
            vec!["1".to_string(), "99".to_string()],
            vec!["2".to_string(), "90".to_string()],
            vec!["3".to_string(), "90".to_string()],
        ]
    );
}

#[test]
fn select_prewhere_and_where_combines_filters() {
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
            field_name: "role".to_string(),
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

    for (id, role) in [("1", "admin"), ("1", "user"), ("2", "admin")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("role".to_string(), role.as_bytes().to_vec());

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
        "req-select-prewhere-where-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, role from users prewhere id = 1 where role = 'admin'".to_string(),
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
    assert_eq!(rows, vec![vec!["1".to_string(), "admin".to_string()]]);
}

#[test]
fn select_limit_by_caps_each_group() {
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
            field_name: "team".to_string(),
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

    for (id, team) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("team".to_string(), team.as_bytes().to_vec());

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
        "req-select-limit-by-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, team from users order by id limit 1 by team".to_string(),
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
            vec!["1".to_string(), "a".to_string()],
            vec!["3".to_string(), "b".to_string()],
        ]
    );
}

#[test]
fn select_limit_by_with_offset_applies_global_offset_after_group_caps() {
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
            field_name: "team".to_string(),
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

    for (id, team) in [("1", "a"), ("2", "a"), ("3", "b"), ("4", "b")] {
        let table = catalog.table("users").expect("table should exist");
        let mut row = HashMap::new();
        row.insert("id".to_string(), id.as_bytes().to_vec());
        row.insert("team".to_string(), team.as_bytes().to_vec());

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
        "req-select-limit-by-offset-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id, team from users order by id limit 1 offset 1 by team".to_string(),
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
    assert_eq!(rows, vec![vec!["3".to_string(), "b".to_string()]]);
}

#[test]
fn select_sort_cluster_distribute_by_apply_ordering_compatibility() {
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

    for id in ["3", "1", "2"] {
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

    let sort_rows = query_result_rows(handle_query_command(
        "req-select-sort-by-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users sort by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    ));
    assert_eq!(
        sort_rows,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ]
    );

    let cluster_rows = query_result_rows(handle_query_command(
        "req-select-cluster-by-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users cluster by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    ));
    assert_eq!(
        cluster_rows,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ]
    );

    let distribute_rows = query_result_rows(handle_query_command(
        "req-select-distribute-by-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users distribute by id".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-test",
        1,
        Some("root@localhost".to_string()),
    ));
    assert_eq!(
        distribute_rows,
        vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ]
    );
}

#[test]
fn union_query_applies_query_level_fetch_first_rows_only() {
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
        "req-union-order-fetch-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union all select id from archived_users order by id desc fetch first 2 rows only".to_string(),
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
    assert_eq!(rows, vec![vec!["4".to_string()], vec!["3".to_string()]]);
}

#[test]
fn union_query_applies_query_level_limit_by() {
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
        ("archived_users", "1"),
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
        "req-union-limit-by-1",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select id from users union all select id from archived_users order by id limit 1 by id".to_string(),
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
        unique: false,
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
fn select_user_returns_current_logged_in_session_user() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-select-user-runtime",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select user()".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-user-fn",
        42,
        Some("alice@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec![expected_os_user_identity()]]);
}

#[test]
fn select_session_user_defaults_to_root_without_session_identity() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-select-session-user-default",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select session_user()".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-user-fn",
        42,
        None,
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["root".to_string()]]);
}

#[test]
fn select_session_user_returns_explicit_session_identity() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-select-session-user-explicit",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "select session_user()".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-user-fn",
        42,
        Some("alice@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec!["alice".to_string()]]);
}

#[test]
fn show_user_returns_current_logged_in_session_user() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-show-user-runtime",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show user()".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-user-fn",
        42,
        Some("alice@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec![expected_os_user_identity()]]);
}

#[test]
fn show_user_without_parentheses_returns_current_logged_in_session_user() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");
    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-show-user-runtime-bare",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show user;".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-user-fn",
        42,
        Some("alice@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert_eq!(rows, vec![vec![expected_os_user_identity()]]);
}

#[test]
fn show_privileges_returns_effective_privilege_state() {
    let mut catalogs = HashMap::new();
    let wal = ConcurrentWalManager::in_memory();
    let mut runtime_indexes = RuntimeIndexStore::new();

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("main").expect("catalog should be created");

    let mut acl = serverlib::AccountAclEntry::new(serverlib::UserId("alice".to_string()), "main");
    acl.append_privilege(AccountPrivilege::Select);
    acl.append_grant_option_for_privilege(AccountPrivilege::Select);
    acl.append_privilege(AccountPrivilege::Update);
    catalog.upsert_account_acl_entry(acl);

    catalogs.insert("main".to_string(), catalog);

    let response = handle_query_command(
        "req-show-privileges",
        &DataQuery {
            database_id: "main".to_string(),
            sql: "show privileges".to_string(),
        },
        &mut catalogs,
        &wal,
        &test_node_data_dir(),
        &mut runtime_indexes,
        "session-user-fn",
        42,
        Some("alice@localhost".to_string()),
    );

    let rows = query_result_rows(response);
    assert!(rows.iter().any(|row| {
        row == &vec![
            "alice".to_string(),
            "*".to_string(),
            "*".to_string(),
        ]
    }));
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
                    unique: false,
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
                    unique: false,
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
