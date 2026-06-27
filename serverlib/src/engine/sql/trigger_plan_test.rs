use super::*;
use crate::{TriggerEventKind, TriggerTiming};

#[test]
fn parse_trigger_invocation_binding_extracts_before_insert_on_table() {
    let binding = parse_trigger_invocation_binding_from_create_trigger_statement(
        "create trigger trg_users_bi before insert on users for each row set @x = 1",
    )
    .expect("create trigger should parse")
    .expect("binding should be detected");

    assert_eq!(binding.table_id, "users");
    assert_eq!(binding.timing, TriggerTiming::Before);
    assert_eq!(binding.event, TriggerEventKind::Insert);
}

#[test]
fn parse_trigger_invocation_binding_supports_create_or_replace() {
    let binding = parse_trigger_invocation_binding_from_create_trigger_statement(
        "create or replace trigger trg_users_au after update on users for each row set @x = 1",
    )
    .expect("create or replace trigger should parse")
    .expect("binding should be detected");

    assert_eq!(binding.table_id, "users");
    assert_eq!(binding.timing, TriggerTiming::After);
    assert_eq!(binding.event, TriggerEventKind::Update);
}

#[test]
fn parse_trigger_invocation_binding_returns_none_for_non_trigger_sql() {
    let binding = parse_trigger_invocation_binding_from_create_trigger_statement(
        "select * from users",
    )
    .expect("non trigger sql should not fail");

    assert!(binding.is_none());
}
