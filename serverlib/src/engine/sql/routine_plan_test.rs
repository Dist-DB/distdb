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
fn parse_if_else_end_plan_from_create_procedure_extracts_if_block_after_setup_statements() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin set @phase = 'boot'; select id from users limit 1; if active = 1 then select 'on'; else select 'off'; end if; end",
    )
    .expect("create procedure with setup statements should parse")
    .expect("if block should be detected after setup statements");

    assert_eq!(plan.branches.len(), 1);
    assert_eq!(plan.branches[0].action_sql, "select 'on'");
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
fn parse_if_else_end_plan_from_create_procedure_returns_none_for_setup_only_body() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin set @phase = 'boot'; select id from users limit 1; end",
    )
    .expect("setup-only create procedure should parse");

    assert!(plan.is_none());
}

#[test]
fn parse_if_else_end_plan_from_create_procedure_does_not_match_identifier_tokens() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin select if_active from users; select case_count from users; end",
    )
    .expect("identifier tokens should not trigger control-flow extraction");

    assert!(plan.is_none());
}

#[test]
fn parse_if_else_end_plan_from_create_procedure_extracts_searched_case_block() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin case when active = 1 then select 'on'; when active = 0 then select 'off'; else select 'unknown'; end case; end",
    )
    .expect("create procedure searched CASE should parse")
    .expect("searched CASE block should be detected");

    assert_eq!(plan.branches.len(), 2);
    assert_eq!(plan.branches[0].action_sql, "select 'on'");
    assert_eq!(plan.branches[1].action_sql, "select 'off'");
    assert_eq!(plan.else_action_sql.as_deref(), Some("select 'unknown'"));
}

#[test]
fn parse_if_else_end_plan_from_create_procedure_extracts_simple_case_block() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin case active when 1 then select 'on'; when 0 then select 'off'; else select 'unknown'; end case; end",
    )
    .expect("create procedure simple CASE should parse")
    .expect("simple CASE block should be detected");

    assert_eq!(plan.branches.len(), 2);
    assert_eq!(plan.branches[0].action_sql, "select 'on'");
    assert_eq!(plan.branches[1].action_sql, "select 'off'");
    assert_eq!(plan.else_action_sql.as_deref(), Some("select 'unknown'"));
}

#[test]
fn parse_if_else_end_plan_from_create_procedure_extracts_case_block_after_setup_statements() {
    let plan = parse_if_else_end_plan_from_create_procedure_statement(
        "create procedure p_sync() begin set @phase = 'boot'; select id from users limit 1; case active when 1 then select 'on'; when 0 then select 'off'; else select 'unknown'; end case; end",
    )
    .expect("create procedure with setup statements should parse")
    .expect("case block should be detected after setup statements");

    assert_eq!(plan.branches.len(), 2);
    assert_eq!(plan.branches[0].action_sql, "select 'on'");
    assert_eq!(plan.branches[1].action_sql, "select 'off'");
    assert_eq!(plan.else_action_sql.as_deref(), Some("select 'unknown'"));
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
fn parse_create_procedure_parameter_names_from_multiline_as_begin_statement_extracts_names() {
    let names = parse_create_procedure_parameter_names_from_statement(
        "create procedure p_sync(arg_user_id int, arg_state varchar(20))\nas begin\nselect 1;\nend",
    )
    .expect("multiline parameter list should parse");

    assert_eq!(names, vec!["arg_user_id".to_string(), "arg_state".to_string()]);
}

#[test]
fn parse_create_procedure_parameter_declarations_extracts_modes() {
    let parameters = parse_create_procedure_parameter_declarations_from_statement(
        "create procedure p_sync(in p_in int, out p_out int, inout p_mix varchar(20)) begin select 1; end",
    )
    .expect("parameter declarations should parse");

    assert_eq!(
        parameters,
        vec![
            RoutineParameterDeclaration {
                name: "p_in".to_string(),
                mode: RoutineParameterMode::In,
            },
            RoutineParameterDeclaration {
                name: "p_out".to_string(),
                mode: RoutineParameterMode::Out,
            },
            RoutineParameterDeclaration {
                name: "p_mix".to_string(),
                mode: RoutineParameterMode::InOut,
            },
        ]
    );
}

#[test]
fn parse_create_function_parameter_names_from_statement_extracts_names() {
    let names = parse_create_function_parameter_names_from_statement(
        "create function f_sync(arg_user_id int, arg_state varchar(20)) returns varchar(20) return arg_state",
    )
    .expect("function parameter list should parse");

    assert_eq!(names, vec!["arg_user_id".to_string(), "arg_state".to_string()]);
}

#[test]
fn extract_create_function_action_sql_converts_return_expression_to_select() {
    let action_sql = extract_create_function_action_sql(
        "create function f_sync(arg_user_id int) returns int return arg_user_id",
    )
    .expect("return expression should convert to scalar select");

    assert_eq!(action_sql, "select arg_user_id");
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

#[test]
fn bind_call_procedure_arguments_accepts_null_and_placeholder_values() {
    let call_statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "call p_sync(null, ?)"
    )
    .expect("call statement should parse")
    .into_iter()
    .next()
    .expect("single call statement should exist");

    let bindings = bind_call_procedure_arguments(
        "create procedure p_sync(arg_state varchar(20), arg_marker varchar(20)) begin select 1; end",
        &call_statement,
    )
    .expect("call arguments should bind");

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0], ("arg_state".to_string(), b"NULL".to_vec()));
    assert_eq!(bindings[1], ("arg_marker".to_string(), b"?".to_vec()));
}

#[test]
fn bind_call_procedure_arguments_accepts_constant_expressions() {
    let call_statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "call p_sync(1 + 2, abs(-3))"
    )
    .expect("call statement should parse")
    .into_iter()
    .next()
    .expect("single call statement should exist");

    let bindings = bind_call_procedure_arguments(
        "create procedure p_sync(arg_left int, arg_right int) begin select 1; end",
        &call_statement,
    )
    .expect("call arguments should bind");

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0], ("arg_left".to_string(), b"1 + 2".to_vec()));
    assert_eq!(bindings[1], ("arg_right".to_string(), b"3".to_vec()));
}

#[test]
fn bind_call_procedure_argument_bindings_applies_parameter_modes() {
    let call_statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "call p_sync(7, arg_result, arg_state)",
    )
    .expect("call statement should parse")
    .into_iter()
    .next()
    .expect("single call statement should exist");

    let bindings = bind_call_procedure_argument_bindings(
        "create procedure p_sync(in p_in int, out p_out int, inout p_state int) begin select 1; end",
        &call_statement,
    )
    .expect("mode-aware call arguments should bind");

    assert_eq!(bindings.len(), 3);
    assert_eq!(bindings[0].name, "p_in");
    assert_eq!(bindings[0].mode, RoutineParameterMode::In);
    assert_eq!(bindings[0].value, b"7".to_vec());
    assert!(bindings[0].output_target.is_none());

    assert_eq!(bindings[1].name, "p_out");
    assert_eq!(bindings[1].mode, RoutineParameterMode::Out);
    assert_eq!(bindings[1].value, b"NULL".to_vec());
    assert_eq!(bindings[1].output_target.as_deref(), Some("arg_result"));

    assert_eq!(bindings[2].name, "p_state");
    assert_eq!(bindings[2].mode, RoutineParameterMode::InOut);
    assert_eq!(bindings[2].value, b"arg_state".to_vec());
    assert_eq!(bindings[2].output_target.as_deref(), Some("arg_state"));
}

#[test]
fn bind_call_procedure_argument_bindings_rejects_non_identifier_out_target() {
    let call_statement = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::MySqlDialect {},
        "call p_sync(1)",
    )
    .expect("call statement should parse")
    .into_iter()
    .next()
    .expect("single call statement should exist");

    let err = bind_call_procedure_argument_bindings(
        "create procedure p_sync(out p_out int) begin select 1; end",
        &call_statement,
    )
    .expect_err("OUT binding should require identifier target");

    assert!(matches!(
        err,
        crate::SqlParseError::UnsupportedStatement(message)
            if message.contains("OUT argument") && message.contains("identifier target")
    ));
}

#[test]
fn parse_create_procedure_action_statements_splits_top_level_statements() {
    let actions = parse_create_procedure_action_statements(
        "create procedure p_sync() begin set @x = 1; select (@x + 1); select ';'; end",
    )
    .expect("procedure actions should parse");

    assert_eq!(
        actions,
        vec![
            "set @x = 1".to_string(),
            "select (@x + 1)".to_string(),
            "select ';'".to_string(),
        ]
    );
}
