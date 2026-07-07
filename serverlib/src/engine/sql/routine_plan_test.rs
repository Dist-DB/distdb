use super::*;
use crate::{
    SelectComparisonOp, SelectCondition, SelectPredicate,
};

#[test]
fn parse_if_else_end_plan_parses_if_elseif_else_chain() {
    let plan = parse_if_else_end_plan_from_statement(
        "if active = 1 then select 'on'; elseif active = 0 then select 'off'; else select 'unknown'; end if",
    )
    .expect("if/else/end plan should parse");

    assert_eq!(plan.branches.len(), 2);
    assert!(matches!(
        plan.branches[0].condition,
        SelectCondition::Predicate(SelectPredicate::Comparison {
            op: SelectComparisonOp::Eq,
            ..
        })
    ));
    assert_eq!(plan.branches[0].action_sql, "select 'on'");
    assert_eq!(plan.branches[1].action_sql, "select 'off'");
    assert_eq!(plan.else_action_sql.as_deref(), Some("select 'unknown'"));
}

#[test]
fn parse_if_else_end_plan_parses_if_without_else() {
    let plan = parse_if_else_end_plan_from_statement(
        "if active = 1 then select 'on'; end if",
    )
    .expect("if/else/end plan without else should parse");

    assert_eq!(plan.branches.len(), 1);
    assert!(plan.else_action_sql.is_none());
}

#[test]
fn parse_if_else_end_plan_rejects_missing_end_if() {
    let err = parse_if_else_end_plan_from_statement(
        "if active = 1 then select 'on'",
    )
    .expect_err("missing end if should fail");

    assert!(matches!(
        err,
        crate::SqlParseError::UnsupportedStatement(message)
            if message.contains("IF/ELSE/END")
    ));
}

#[test]
fn parse_if_else_end_plan_from_create_procedure_extracts_if_block() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin if active = 1 then select 'on'; else select 'off'; end if; end",
    )
    .expect("create procedure if block should parse")
    .expect("if block should be detected");

    assert_eq!(plan.branches.len(), 1);
    assert_eq!(plan.else_action_sql.as_deref(), Some("select 'off'"));
}

#[test]
fn parse_if_else_end_plan_from_create_procedure_returns_none_when_body_is_not_if() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin select 1; end",
    )
    .expect("create procedure with non-if body should parse");

    assert!(plan.is_none());
}

#[test]
fn parse_create_procedure_parameter_names_from_statement_extracts_names() {
    let names = parse_create_procedure_parameter_names_from_statement(
        "create procedure p_sync(arg_user_id int, arg_state varchar(20)) begin select 1; end",
    )
    .expect("parameter list should parse");

    assert_eq!(names, vec!["arg_user_id".to_string(), "arg_state".to_string()]);
}

#[test]
fn bind_call_procedure_arguments_maps_values_by_parameter_name() {
    let call_statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "call p_sync(42, 'ready')",
    )
    .expect("call statement should parse")
    .into_iter()
    .next()
    .expect("single call statement should exist");

    let bindings = bind_call_procedure_arguments(
        "create procedure p_sync(arg_user_id int, arg_state varchar(20)) begin select 1; end",
        &call_statement,
    )
    .expect("call arguments should bind");

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0], ("arg_user_id".to_string(), b"42".to_vec()));
    assert_eq!(bindings[1], ("arg_state".to_string(), b"ready".to_vec()));
}

#[test]
fn bind_call_procedure_arguments_rejects_count_mismatch() {
    let call_statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "call p_sync(42)",
    )
    .expect("call statement should parse")
    .into_iter()
    .next()
    .expect("single call statement should exist");

    let err = bind_call_procedure_arguments(
        "create procedure p_sync(arg_user_id int, arg_state varchar(20)) begin select 1; end",
        &call_statement,
    )
    .expect_err("count mismatch should fail");

    assert!(matches!(
        err,
        crate::SqlParseError::UnsupportedStatement(message)
            if message.contains("CALL argument mismatch")
    ));
}
