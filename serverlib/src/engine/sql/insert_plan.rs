use sqlparser::ast::{Expr, SetExpr, Statement, UnaryOperator, Value};

use super::{
    evaluate_sql_function, parse_mysql_statements, parse_select_read_plan_from_statement,
    InsertRowsPlan, InsertRowsSource, SqlParseError,
};

pub fn parse_insert_rows_from_statement(
    statement: &str,
) -> Result<InsertRowsPlan, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    parse_insert_rows_from_parsed_statement(single)

}

pub fn parse_insert_rows_from_parsed_statement(
    statement: &Statement,
) -> Result<InsertRowsPlan, SqlParseError> {

    let Statement::Insert(insert) = statement else {
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

    let source = match source.body.as_ref() {

        SetExpr::Values(values) => {

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

            InsertRowsSource::Values(rows)

        },

        SetExpr::Select(_) => {
            let select_plan = parse_select_read_plan_from_statement(&source.to_string())?;
            InsertRowsSource::Select(select_plan)
        },

        _ => {
            return Err(SqlParseError::UnsupportedStatement(
                "only INSERT ... VALUES or INSERT ... SELECT is currently supported".to_string(),
            ));
        }

    };

    Ok(InsertRowsPlan {
        table_id,
        columns,
        source,
    })

}

fn parse_insert_literal(expr: &Expr) -> Result<Option<Vec<u8>>, SqlParseError> {
    
    match expr {
        
        Expr::Value(value) => parse_insert_value(value),

        Expr::UnaryOp { op, expr } => {

            let Expr::Value(Value::Number(number, _)) = expr.as_ref() else {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "INSERT literal '{expr}' is not supported"
                )));
            };

            match op {
                
                UnaryOperator::Minus => Ok(Some(format!("-{number}").into_bytes())),

                UnaryOperator::Plus => Ok(Some(number.clone().into_bytes())),
                _ => Err(SqlParseError::UnsupportedStatement(format!(
                    "INSERT literal '{expr}' is not supported"
                ))),
                
            }

        },
        
        Expr::Function(function) => evaluate_sql_function(function),
        
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
#[path = "insert_plan_test.rs"]
mod tests;
