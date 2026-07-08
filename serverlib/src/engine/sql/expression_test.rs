use sqlparser::{dialect::MySqlDialect, parser::Parser};

use super::{expression_references_column, SelectExpression};

fn parse_function(sql: &str) -> sqlparser::ast::Function {

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

#[test]
fn expression_references_column_detects_column_arg_functions() {

    let expression = SelectExpression::InbuiltFunction {
        function: parse_function("select concat(email, '!')"),
    };

    assert!(expression_references_column(&expression));

}

#[test]
fn expression_references_column_ignores_literal_only_functions() {

    let expression = SelectExpression::InbuiltFunction {
        function: parse_function("select concat('a', 'b')"),
    };

    assert!(!expression_references_column(&expression));

}
