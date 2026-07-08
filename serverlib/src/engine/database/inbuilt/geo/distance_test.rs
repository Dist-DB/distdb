use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;

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
fn distance_returns_zero_for_identical_points() {
    let value = evaluate_expression("distance(12.5, -45.1, 12.5, -45.1)")
        .expect("distance should return a value")
        .parse::<f64>()
        .expect("distance should be numeric");

    assert!(value.abs() < 0.000_001);
}

#[test]
fn distance_returns_expected_meters_for_known_points() {
    let value = evaluate_expression("distance(2.3522, 48.8566, -0.1276, 51.5074)")
        .expect("distance should return a value")
        .parse::<f64>()
        .expect("distance should be numeric");

    // Paris -> London ~= 343.5 km (343,500 meters), allow tolerance.
    assert!((value - 343_500.0).abs() < 6_000.0, "value was {value}");
}

#[test]
fn distance_handles_antimeridian_crossing() {
    let value = evaluate_expression("distance(179.9, 0, -179.9, 0)")
        .expect("distance should return a value")
        .parse::<f64>()
        .expect("distance should be numeric");

    // Around 0.2 degrees on equator ~= 22.24km.
    assert!((value - 22_239.0).abs() < 1_500.0, "value was {value}");
}
