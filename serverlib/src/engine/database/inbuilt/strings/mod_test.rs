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
fn string_function_registry_exposes_mysql_aliases() {
    for function_name in [
        "concat_ws",
        "mid",
        "position",
        "repeat",
        "replace",
        "reverse",
        "right",
    ] {
        assert!(is_inbuilt_function(function_name));
    }
}

#[test]
fn evaluate_string_functions_matches_mysql_like_samples() {

    let cases = [
        ("ascii('A')", Some("65")),
        ("char_length('Grüße')", Some("5")),
        ("concat('sa', 'm')", Some("sam")),
        ("concat_ws('-', 'sam', null, 'colak')", Some("sam-colak")),
        ("field('b', 'a', 'b', 'c')", Some("2")),
        ("find_in_set('b', 'a,b,c')", Some("2")),
        ("format(12345.678, 2)", Some("12,345.68")),
        ("insert('Quadratic', 3, 4, 'ZZ')", Some("QuZZtic")),
        ("instr('Foobar', 'oba')", Some("3")),
        ("left('Hello', 2)", Some("He")),
        ("length('Grüße')", Some("7")),
        ("locate('bar', 'Foobarbar')", Some("4")),
        ("lower('GrÜße')", Some("grüße")),
        ("lpad('hi', 5, 'xy')", Some("xyxhi")),
        ("ltrim('  hi')", Some("hi")),
        ("mid('Quadratic', 3, 4)", Some("adra")),
        ("repeat('ab', 3)", Some("ababab")),
        ("replace('abcabc', 'ab', 'x')", Some("xcxc")),
        ("reverse('stressed')", Some("desserts")),
        ("right('Hello', 2)", Some("lo")),
        ("rpad('hi', 5, 'xy')", Some("hixyx")),
        ("rtrim('hi  ')", Some("hi")),
        ("space(3)", Some("   ")),
        ("substr('Quadratic', -4, 3)", Some("ati")),
        ("substring_index('www.mysql.com', '.', 2)", Some("www.mysql")),
        ("upper('Grüße')", Some("GRÜSSE")),
    ];

    for (expression, expected) in cases {
        assert_eq!(evaluate_expression(expression).as_deref(), expected, "{}", expression);
    }

}

#[test]
fn string_functions_propagate_null_when_mysql_requires_it() {
    assert_eq!(evaluate_expression("concat('sam', null)"), None);
    assert_eq!(evaluate_expression("concat_ws(null, 'sam', 'colak')"), None);
    assert_eq!(evaluate_expression("find_in_set(null, 'a,b')"), None);
}
