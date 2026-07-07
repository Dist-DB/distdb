use std::collections::HashMap;

use crate::{
    ConcurrentWalManager, DatabaseCatalog, SqlCursorFrame, TableSchema, WalStreamMode,
    TriggerEventKind, TriggerTiming,
    VecSqlCursorSource,
};

use super::{
    cleanup_temporary_tables, execute_automatic_triggers_for_event,
    execute_stored_procedure_invocation, execute_stored_procedure_invocation_over_cursor,
    execute_stored_procedure_invocation_over_cursor_with_cleanup,
    execute_stored_procedure_invocation_over_cursor_with_scoped_teardown,
    execute_stored_procedure_invocation_with_cleanup, EntityInvocationSource,
    execute_stored_procedure_invocation_with_scoped_teardown,
};

#[test]
fn execute_stored_procedure_invocation_uses_cached_if_else_plan() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; else select 'off'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist");

    let mut row = HashMap::new();
    row.insert("active".to_string(), b"0".to_vec());

    let result = execute_stored_procedure_invocation(
        &row,
        procedure,
        EntityInvocationSource::DirectedUser,
        &mut |sql| Ok(sql.to_string()),
    )
    .expect("stored procedure invocation should succeed");

    assert_eq!(result, Some("select 'off'".to_string()));

}

#[test]
fn execute_automatic_triggers_for_event_runs_only_matching_triggers() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_trigger(
            "trg_users_bi",
            "create trigger trg_users_bi before insert on users for each row set @x = 1",
            vec!["users".to_string()],
        )
        .expect("before insert trigger should register");

    catalog
        .register_trigger(
            "trg_users_au",
            "create trigger trg_users_au after update on users for each row set @x = 2",
            vec!["users".to_string()],
        )
        .expect("after update trigger should register");

    let mut invoked = Vec::new();

    let result = execute_automatic_triggers_for_event(
        &catalog,
        "users",
        TriggerTiming::Before,
        TriggerEventKind::Insert,
        &mut |sql| {
            invoked.push(sql.to_string());
            Ok(sql.to_string())
        },
    )
    .expect("automatic trigger invocation should succeed");

    assert_eq!(result.len(), 1);
    assert_eq!(invoked.len(), 1);
    assert!(invoked[0].contains("before insert on users"));

}

#[test]
fn execute_stored_procedure_invocation_over_cursor_runs_for_each_row() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; else select 'off'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist");

    let mut rows = Vec::new();

    let mut first = HashMap::new();
    first.insert("active".to_string(), b"1".to_vec());
    rows.push(first);

    let mut second = HashMap::new();
    second.insert("active".to_string(), b"0".to_vec());
    rows.push(second);

    let mut cursor_source = VecSqlCursorSource::new(rows);
    let mut cursor_frame = SqlCursorFrame::new();

    let outcomes = execute_stored_procedure_invocation_over_cursor(
        &mut cursor_source,
        &mut cursor_frame,
        procedure,
        EntityInvocationSource::DirectedUser,
        &mut |sql, _frame| Ok(sql.to_string()),
    )
    .expect("cursor procedure invocation should succeed");

    assert_eq!(
        outcomes,
        vec!["select 'on'".to_string(), "select 'off'".to_string()]
    );

    assert_eq!(cursor_frame.diagnostics.fetched_rows, 2);
    assert!(cursor_frame.diagnostics.not_found);
    assert!(cursor_frame.diagnostics.closed);

}

#[test]
fn execute_stored_procedure_invocation_over_cursor_skips_rows_without_matching_branch() {
    
    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist");

    let mut rows = Vec::new();

    let mut first = HashMap::new();
    first.insert("active".to_string(), b"1".to_vec());
    rows.push(first);

    let mut second = HashMap::new();
    second.insert("active".to_string(), b"0".to_vec());
    rows.push(second);

    let mut cursor_source = VecSqlCursorSource::new(rows);
    let mut cursor_frame = SqlCursorFrame::new();

    let outcomes = execute_stored_procedure_invocation_over_cursor(
        &mut cursor_source,
        &mut cursor_frame,
        procedure,
        EntityInvocationSource::DirectedUser,
        &mut |sql, _frame| Ok(sql.to_string()),
    )
    .expect("cursor procedure invocation should succeed");

    assert_eq!(outcomes, vec!["select 'on'".to_string()]);

}

#[test]
fn execute_stored_procedure_invocation_over_cursor_propagates_action_errors() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; else select 'off'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist");

    let mut rows = Vec::new();
    let mut first = HashMap::new();
    first.insert("active".to_string(), b"1".to_vec());
    rows.push(first);

    let mut cursor_source = VecSqlCursorSource::new(rows);
    let mut cursor_frame = SqlCursorFrame::new();

    let err = execute_stored_procedure_invocation_over_cursor(
        &mut cursor_source,
        &mut cursor_frame,
        procedure,
        EntityInvocationSource::DirectedUser,
        &mut |_sql, _frame| Result::<String, String>::Err("forced action failure".to_string()),
    )
    .expect_err("cursor procedure invocation should fail");

    assert!(err.contains("forced action failure"));
    assert!(cursor_frame.diagnostics.closed);
    
}

#[test]
fn cleanup_temporary_tables_drops_temp_tables_and_streams() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    catalog
        .create_temporary_table("tmp_users", TableSchema::new(Vec::new()))
        .expect("temp table should register");

    assert!(catalog.table("tmp_users").is_some());

    cleanup_temporary_tables(&mut catalog, &wal).expect("cleanup should succeed");

    assert!(catalog.table("tmp_users").is_none());
    assert_eq!(wal.stream_mode("tmp_users"), WalStreamMode::Durable);

}

#[test]
fn execute_stored_procedure_invocation_with_cleanup_runs_cleanup_even_on_success() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    catalog
        .create_temporary_table("tmp_users", TableSchema::new(Vec::new()))
        .expect("temp table should register");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist")
        .clone();

    let mut cleanup = || cleanup_temporary_tables(&mut catalog, &wal);

    let result = execute_stored_procedure_invocation_with_cleanup(
        &HashMap::new(),
        &procedure,
        EntityInvocationSource::DirectedUser,
        &mut |sql| Ok(sql.to_string()),
        &mut cleanup,
    )
    .expect("invocation with cleanup should succeed");

    assert_eq!(result, None);
    assert!(catalog.table("tmp_users").is_none());

}

#[test]
fn execute_stored_procedure_invocation_over_cursor_with_cleanup_runs_cleanup() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    catalog
        .create_temporary_table("tmp_users", TableSchema::new(Vec::new()))
        .expect("temp table should register");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist")
        .clone();

    let mut rows = Vec::new();
    let mut first = HashMap::new();
    first.insert("active".to_string(), b"1".to_vec());
    rows.push(first);

    let mut cursor_source = VecSqlCursorSource::new(rows);
    let mut cursor_frame = SqlCursorFrame::new();

    let mut cleanup = || cleanup_temporary_tables(&mut catalog, &wal);

    let outcomes = execute_stored_procedure_invocation_over_cursor_with_cleanup(
        &mut cursor_source,
        &mut cursor_frame,
        &procedure,
        EntityInvocationSource::DirectedUser,
        &mut |sql, _frame| Ok(sql.to_string()),
        &mut cleanup,
    )
    .expect("cursor invocation with cleanup should succeed");

    assert_eq!(outcomes, vec!["select 'on'".to_string()]);
    assert!(catalog.table("tmp_users").is_none());

}

#[test]
fn execute_stored_procedure_invocation_with_scoped_teardown_cleans_up_owned_tables() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin select 1; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist")
        .clone();

    let result = execute_stored_procedure_invocation_with_scoped_teardown(
        &mut catalog,
        &wal,
        &HashMap::new(),
        &procedure,
        EntityInvocationSource::DirectedUser,
        "session-a",
        &mut |_sql, scope, catalog, wal| {
            let scoped_table_id = scope
                .create_table(
                    catalog,
                    wal,
                    "tmp_users",
                    TableSchema::new(Vec::new()),
                )?;

            assert!(catalog.table(&scoped_table_id).is_some());

            Ok("ok".to_string())
        },
    )
    .expect("scoped invocation should succeed");

    assert_eq!(result, None);
    assert!(catalog
        .table_ids()
        .into_iter()
        .all(|table_id| !table_id.contains("tmp_users")));

}

#[test]
fn scoped_teardown_does_not_bleed_between_procedure_instances() {

    let mut catalog =
        DatabaseCatalog::create_empty_from_name("MainDb").expect("catalog should be created");
    let wal = ConcurrentWalManager::new();

    catalog
        .register_stored_procedure(
            "refresh_accounts",
            "create procedure refresh_accounts() begin if active = 1 then select 'on'; end if; end",
            vec!["accounts".to_string()],
        )
        .expect("procedure register should succeed");

    let procedure = catalog
        .stored_procedure("refresh_accounts")
        .expect("procedure should exist")
        .clone();

    let mut rows = Vec::new();
    let mut row = HashMap::new();
    row.insert("active".to_string(), b"1".to_vec());
    rows.push(row);

    let mut cursor_source = VecSqlCursorSource::new(rows);
    let mut cursor_frame = SqlCursorFrame::new();

    let mut first_scope_table_ids = Vec::new();

    let outcomes = execute_stored_procedure_invocation_over_cursor_with_scoped_teardown(
        &mut catalog,
        &wal,
        &mut cursor_source,
        &mut cursor_frame,
        &procedure,
        EntityInvocationSource::DirectedUser,
        "session-a",
        &mut |_sql, _frame, scope, catalog, wal| {
            let table_id = scope
                .create_table(
                    catalog,
                    wal,
                    "tmp_users",
                    TableSchema::new(Vec::new()),
                )?;
            first_scope_table_ids.push(table_id);
            Ok("ok".to_string())
        },
    )
    .expect("scoped cursor invocation should succeed");

    assert_eq!(outcomes, vec!["ok".to_string()]);

    for table_id in &first_scope_table_ids {
        assert!(catalog.table(table_id).is_none());
    }

    let result = execute_stored_procedure_invocation_with_scoped_teardown(
        &mut catalog,
        &wal,
        &HashMap::new(),
        &procedure,
        EntityInvocationSource::DirectedUser,
        "session-b",
        &mut |_sql, scope, catalog, wal| {
            let table_id = scope
                .create_table(
                    catalog,
                    wal,
                    "tmp_users",
                    TableSchema::new(Vec::new()),
                )?;

            assert!(table_id.contains("session"));
            Ok("ok2".to_string())
        },
    )
    .expect("second scoped invocation should succeed");

    assert_eq!(result, None);

}
