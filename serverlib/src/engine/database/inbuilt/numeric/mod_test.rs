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
fn numeric_function_registry_exposes_expected_functions() {

    for function_name in [
        "abs",
        "atan2",
        "ceil",
        "div",
        "greatest",
        "least",
        "log2",
        "pow",
        "rand",
        "truncate",
    ] {
        assert!(is_inbuilt_function(function_name));
    }

}

#[test]
fn evaluate_numeric_functions_matches_mysql_like_samples() {
    let cases = [
        ("abs(-3.5)", Some("3.5")),
        ("acos(1)", Some("0")),
        ("asin(1)", Some("1.5707963267948966")),
        ("atan(1)", Some("0.7853981633974483")),
        ("atan2(0, -1)", Some("3.141592653589793")),
        ("avg(5)", Some("5")),
        ("cos(0)", Some("1")),
        ("round(cot(1), 12)", Some("0.642092615934")),
        ("count(1, null, 2)", Some("2")),
        ("round(degrees(pi()), 0)", Some("180")),
        ("div(5, 2)", Some("2")),
        ("round(exp(1), 12)", Some("2.718281828459")),
        ("greatest(1, 3, 2)", Some("3")),
        ("least(1, 3, 2)", Some("1")),
        ("round(ln(exp(1)), 0)", Some("1")),
        ("round(log(10, 100), 0)", Some("2")),
        ("log10(1000)", Some("3")),
        ("log2(8)", Some("3")),
        ("max(1, null, 3)", Some("3")),
        ("min(1, null, 3)", Some("1")),
        ("mod(10, 3)", Some("1")),
        ("pi()", Some("3.141592653589793")),
        ("pow(2, 3)", Some("8")),
        ("round(radians(180), 12)", Some("3.14159265359")),
        ("round(1234.56, -2)", Some("1200")),
        ("sign(-12)", Some("-1")),
        ("round(sin(pi() / 2), 0)", Some("1")),
        ("sqrt(9)", Some("3")),
        ("sum(1, null, 2)", Some("3")),
        ("tan(0)", Some("0")),
        ("truncate(123.456, 2)", Some("123.45")),
    ];

    for (expression, expected) in cases {
        assert_eq!(evaluate_expression(expression).as_deref(), expected, "{}", expression);
    }
}

#[test]
fn numeric_functions_propagate_null_or_invalid_mysql_results() {
    assert_eq!(evaluate_expression("abs(null)"), None);
    assert_eq!(evaluate_expression("acos(2)"), None);
    assert_eq!(evaluate_expression("greatest(1, null)"), None);
    assert_eq!(evaluate_expression("mod(10, 0)"), None);
    assert_eq!(evaluate_expression("sqrt(-1)"), None);
}

#[test]
fn rand_is_deterministic_with_seed_and_bounded_without_seed() {
    let seeded_once = evaluate_expression("rand(7)").expect("seeded rand should evaluate");
    let seeded_twice = evaluate_expression("rand(7)").expect("seeded rand should evaluate");
    assert_eq!(seeded_once, seeded_twice);

    let random = evaluate_expression("rand()").expect("rand should evaluate");
    let parsed = random.parse::<f64>().expect("rand output should be numeric");
    assert!((0.0..1.0).contains(&parsed));
}
