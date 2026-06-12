use sqlparser::ast::{Expr, SetExpr, Statement, Value};

use crate::engine::database::inbuilt::evaluate_inbuilt_sql_function;

use super::{parse_mysql_statements, InsertRowsPlan, SqlParseError};

pub fn parse_insert_rows_from_statement(
    statement: &str,
) -> Result<InsertRowsPlan, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Insert(insert) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not INSERT".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(insert.table_name.to_string());
    
    let columns = insert
        .columns
        .iter()
        .map(|column| common::normalize_identifier!(&column.value))
        .collect::<Vec<_>>();

    let Some(source) = &insert.source else {
        return Err(SqlParseError::UnsupportedStatement(
            "INSERT without a VALUES source is not supported".to_string(),
        ));
    };

    let SetExpr::Values(values) = &*source.body else {
        return Err(SqlParseError::UnsupportedStatement(
            "only INSERT ... VALUES is currently supported".to_string(),
        ));
    };

    if values.rows.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "INSERT VALUES must contain at least one row".to_string(),
        ));
    }

    let mut rows = Vec::with_capacity(values.rows.len());

    for row in &values.rows {

        if !columns.is_empty() && row.len() != columns.len() {
            return Err(SqlParseError::UnsupportedStatement(
                "INSERT values count must match provided columns".to_string(),
            ));
        }

        let mut parsed_row = Vec::with_capacity(row.len());
        for expr in row {
            parsed_row.push(parse_insert_literal(expr)?);
        }

        rows.push(parsed_row);

    }

    Ok(InsertRowsPlan {
        table_id,
        columns,
        rows,
    })

}

fn parse_insert_literal(expr: &Expr) -> Result<Option<Vec<u8>>, SqlParseError> {
    match expr {
        Expr::Value(value) => parse_insert_value(value),
        Expr::Function(function) => evaluate_inbuilt_sql_function(function).map_err(|err| {
            SqlParseError::UnsupportedStatement(format!("INSERT inbuilt function failed: {err}"))
        }),
        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "INSERT literal '{expr}' is not supported"
        ))),
    }
}

fn parse_insert_value(value: &Value) -> Result<Option<Vec<u8>>, SqlParseError> {

    match value {

        Value::Null => Ok(None),
        Value::Boolean(v) => Ok(Some(v.to_string().into_bytes())),
        Value::Number(v, _) => Ok(Some(v.to_string().into_bytes())),
        Value::SingleQuotedString(v)
        | Value::DoubleQuotedString(v)
        | Value::TripleSingleQuotedString(v)
        | Value::TripleDoubleQuotedString(v)
        | Value::EscapedStringLiteral(v)
        | Value::UnicodeStringLiteral(v)
        | Value::SingleQuotedByteStringLiteral(v)
        | Value::DoubleQuotedByteStringLiteral(v)
        | Value::TripleSingleQuotedByteStringLiteral(v)
        | Value::TripleDoubleQuotedByteStringLiteral(v)
        | Value::SingleQuotedRawStringLiteral(v)
        | Value::DoubleQuotedRawStringLiteral(v)
        | Value::TripleSingleQuotedRawStringLiteral(v)
        | Value::TripleDoubleQuotedRawStringLiteral(v)
        | Value::NationalStringLiteral(v)
        | Value::HexStringLiteral(v) => Ok(Some(v.as_bytes().to_vec())),
        Value::DollarQuotedString(v) => Ok(Some(v.value.as_bytes().to_vec())),
        Value::Placeholder(v) => Err(SqlParseError::UnsupportedStatement(format!(
            "INSERT placeholder '{v}' is not supported"
        ))),

    }

}

#[cfg(test)]
mod tests {
    
    use super::*;

    #[test]
    fn insert_values_helper_extracts_rows() {
        let plan = parse_insert_rows_from_statement(
            "insert into users (id, email, is_active, nickname) values (1, 'sam@example.com', true, null)",
        )
        .expect("insert values should parse");

        assert_eq!(plan.table_id, "users");
        assert_eq!(plan.columns, vec!["id", "email", "is_active", "nickname"]);
        assert_eq!(plan.rows.len(), 1);
        assert_eq!(plan.rows[0][0], Some(b"1".to_vec()));
        assert_eq!(plan.rows[0][1], Some(b"sam@example.com".to_vec()));
        assert_eq!(plan.rows[0][2], Some(b"true".to_vec()));
        assert_eq!(plan.rows[0][3], None);
    }

    #[test]
    fn insert_values_supports_inbuilt_commands() {
        let plan = parse_insert_rows_from_statement(
            "insert into users (created_at, email) values (UNIXTIMESTAMP(), CONCAT('sam', '@example.com'))",
        )
        .expect("insert values with inbuilt commands should parse");

        assert_eq!(plan.rows.len(), 1);
        assert!(plan.rows[0][0].as_ref().is_some_and(|v| !v.is_empty()));
        assert_eq!(plan.rows[0][1], Some(b"sam@example.com".to_vec()));
    }

}
