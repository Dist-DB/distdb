use std::collections::HashMap;

use crate::{
    DatabaseCatalog, SqlCursorFrame, TriggerEventKind, TriggerTiming,
    VecSqlCursorSource,
};

use super::{
    execute_automatic_triggers_for_event, execute_stored_procedure_invocation,
    execute_stored_procedure_invocation_over_cursor, EntityInvocationSource,
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
