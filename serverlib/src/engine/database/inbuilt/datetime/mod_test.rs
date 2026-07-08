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
fn datetime_function_registry_exposes_expected_functions() {

    for function_name in [
        "adddate",
        "addtime",
        "curdate",
        "current_date",
        "curtime",
        "current_time",
        "date",
        "date_add",
        "datediff",
        "date_format",
        "date_sub",
        "day",
        "dayname",
        "dayofmonth",
        "dayofweek",
        "dayofyear",
        "extract",
        "from_days",
        "hour",
        "last_day",
        "localtime",
        "localtimestamp",
        "makedate",
        "maketime",
        "microsecond",
        "minute",
        "month",
        "now",
        "period_add",
        "period_diff",
        "quarter",
        "second",
        "sec_to_time",
        "str_to_date",
        "subdate",
        "subtime",
        "sysdate",
        "time",
        "time_format",
        "time_to_sec",
        "timediff",
        "timestamp",
        "to_days",
        "unix_timestamp",
        "week",
        "weekday",
        "weekofyear",
        "year",
        "yearweek",
    ] {
        assert!(is_inbuilt_function(function_name), "{}", function_name);
    }

}

#[test]
fn datetime_functions_match_expected_outputs_for_stable_inputs() {

    let exact_cases = [
        ("datediff('2024-01-10', '2024-01-01')", Some("9")),
        ("day('2024-01-31')", Some("31")),
        ("dayofmonth('2024-01-31')", Some("31")),
        ("dayofweek('2024-01-01')", Some("2")),
        ("dayofyear('2024-12-31')", Some("366")),
        ("hour('12:34:56')", Some("12")),
        ("microsecond('10:00:00.123456')", Some("123456")),
        ("minute('12:34:56')", Some("34")),
        ("month('2024-11-10')", Some("11")),
        ("period_add(202312, 2)", Some("202402")),
        ("period_diff(202402, 202312)", Some("2")),
        ("quarter('2024-11-10')", Some("4")),
        ("second('12:34:56')", Some("56")),
        ("time_to_sec('01:01:01')", Some("3661")),
        ("week('2024-01-01')", Some("1")),
        ("weekday('2024-01-01')", Some("0")),
        ("weekofyear('2024-01-01')", Some("1")),
        ("year('2024-01-01')", Some("2024")),
        ("yearweek('2024-01-01')", Some("202401")),
    ];

    for (expression, expected) in exact_cases {
        assert_eq!(evaluate_expression(expression).as_deref(), expected, "{}", expression);
    }

    let smoke_cases = [
        "adddate('2024-01-01', 10)",
        "addtime('10:00:00', '01:30:00')",
        "date('2024-01-02 03:04:05')",
        "date_add('2024-01-01', 2)",
        "date_format('2024-01-02 03:04:05', '%Y-%m-%d')",
        "date_sub('2024-01-10', 3)",
        "dayname('2024-01-01')",
        "from_days(739282)",
        "last_day('2024-02-03')",
        "makedate(2024, 60)",
        "maketime(12, 34, 56)",
        "sec_to_time(3661)",
        "str_to_date('2024-03-14', '%Y-%m-%d')",
        "subdate('2024-01-10', 3)",
        "subtime('10:30:00', '00:45:00')",
        "time('2024-01-02 03:04:05')",
        "time_format('03:04:05', '%H:%i')",
        "timediff('10:00:00', '09:30:00')",
        "timestamp('2024-01-01', '12:00:00')",
        "to_days('2024-01-01')",
    ];

    for expression in smoke_cases {
        let _ = evaluate_expression(expression);
    }

}

#[test]
fn current_datetime_functions_return_values() {

    for expression in [
        "curdate()",
        "current_date()",
        "curtime()",
        "current_time()",
        "localtime()",
        "localtimestamp()",
        "now()",
        "sysdate()",
        "unix_timestamp()",
    ] {
        assert!(evaluate_expression(expression).is_some(), "{}", expression);
    }

}
