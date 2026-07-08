use std::collections::HashMap;

use sqlparser::{dialect::MySqlDialect, parser::Parser};

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;
use crate::{SelectComparisonOp, SelectCondition, SelectPredicate};

use super::evaluate_case_projection;
use crate::engine::sql::{SelectCaseWhen, SelectExpression};

fn parse_function_expr(sql: &str) -> sqlparser::ast::Function {

    let statements = Parser::parse_sql(&MySqlDialect {}, sql)
        .expect("sql should parse");

    let statement = statements.first().expect("statement should exist");
    let sqlparser::ast::Statement::Query(query) = statement else {
        panic!("statement must be query");
    };

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        panic!("query body must be select");
    };

    let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(function)) =
        &select.projection[0]
    else {
        panic!("projection must be function");
    };

    function.clone()

}

fn evaluate_inbuilt_for_case_test(
    function: &sqlparser::ast::Function,
) -> Result<Option<Vec<u8>>, String> {
    evaluate_inbuilt_sql_function(function)
}

#[test]
fn evaluate_case_projection_returns_first_matching_branch() {

    let mut row = HashMap::new();
    row.insert("active".to_string(), b"1".to_vec());

    let branches = vec![
        (
            SelectCaseWhen::Condition(SelectCondition::Predicate(SelectPredicate::Comparison {
                field_name: "active".to_string(),
                op: SelectComparisonOp::Eq,
                value: b"1".to_vec(),
            })),
            SelectExpression::Literal(b"yes".to_vec()),
        ),
        (
            SelectCaseWhen::Condition(SelectCondition::Predicate(SelectPredicate::Comparison {
                field_name: "active".to_string(),
                op: SelectComparisonOp::Eq,
                value: b"0".to_vec(),
            })),
            SelectExpression::Literal(b"no".to_vec()),
        ),
    ];

    let value = evaluate_case_projection(&row, None, &branches, None, &mut evaluate_inbuilt_for_case_test)
        .expect("case projection should evaluate");

    assert_eq!(value, Some(b"yes".to_vec()));

}

#[test]
fn evaluate_case_projection_uses_else_when_no_branch_matches() {

    let mut row = HashMap::new();
    row.insert("active".to_string(), b"2".to_vec());

    let branches = vec![
        (
            SelectCaseWhen::Condition(SelectCondition::Predicate(SelectPredicate::Comparison {
                field_name: "active".to_string(),
                op: SelectComparisonOp::Eq,
                value: b"1".to_vec(),
            })),
            SelectExpression::Literal(b"yes".to_vec()),
        ),
    ];

    let value = evaluate_case_projection(
        &row,
        None,
        &branches,
        Some(&SelectExpression::Literal(b"unknown".to_vec())),
        &mut evaluate_inbuilt_for_case_test,
    )
    .expect("case projection should evaluate");

    assert_eq!(value, Some(b"unknown".to_vec()));

}

#[test]
fn evaluate_case_projection_supports_simple_case_with_function_result() {
    let mut row = HashMap::new();
    row.insert("state".to_string(), b"on".to_vec());

    let case_operand = SelectExpression::Column {
        field_name: "state".to_string(),
    };

    let branches = vec![(
        SelectCaseWhen::Equals(SelectExpression::Literal(b"on".to_vec())),
        SelectExpression::InbuiltFunction {
            function: parse_function_expr("select upper('yes')"),
        },
    )];

    let value = evaluate_case_projection(
        &row,
        Some(&case_operand),
        &branches,
        None,
        &mut evaluate_inbuilt_for_case_test,
    )
        .expect("simple CASE projection should evaluate");

    assert_eq!(value, Some(b"YES".to_vec()));
}
