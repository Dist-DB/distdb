use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::engine::database::inbuilt::{evaluate_inbuilt_sql_function, is_inbuilt_function};

fn evaluate_expression(sql: &str) -> Option<String> {
    let mut statements = Parser::parse_sql(&MySqlDialect {}, &format!("select {}", sql))
        .expect("expression should parse");

    let Statement::Query(query) = statements.remove(0) else {
        panic!("expected query statement");
    };

    let SetExpr::Select(select) = *query.body else {
        panic!("expected select body");
    };

    let SelectItem::UnnamedExpr(expression) = &select.projection[0] else {
        panic!("expected unnamed expression projection");
    };

    let Expr::Function(function) = expression else {
        panic!("expected function projection, got {:?}", expression);
    };

    evaluate_inbuilt_sql_function(function)
        .expect("function should evaluate")
        .map(|value| String::from_utf8(value).expect("result should be utf8"))
}

#[test]
fn custom_function_registry_exposes_newuuid() {
    assert!(is_inbuilt_function("newuuid"));
}

#[test]
fn newuuid_returns_canonical_uuid_text() {
    let value = evaluate_expression("NEWUUID()").expect("newuuid should return a value");

    let parsed = common::Uuid::parse_str(&value).expect("newuuid result should be a valid UUID");
    assert_eq!(parsed.to_string(), value);
}

#[test]
fn newuuid_returns_distinct_values() {
    let first = evaluate_expression("NEWUUID()").expect("first call should return value");
    let second = evaluate_expression("NEWUUID()").expect("second call should return value");
    assert_ne!(first, second);
}
