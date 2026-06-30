use sqlparser::ast::{AssignmentTarget, Expr, Statement, TableFactor, Value};

use super::{
    derive_relation_pushdown_conditions, evaluate_sql_function, parse_joins_from_table_with_joins,
    parse_mysql_statements, parse_relation_bindings_from_table_with_joins,
    parse_select_condition_from_expr, SqlParseError, UpdateAssignment, UpdateRowsPlan,
};

pub fn parse_update_rows_from_statement(statement: &str) -> Result<UpdateRowsPlan, SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Update {
        table,
        assignments,
        selection,
        ..
    } = single
    else {
        
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not UPDATE".to_string(),
        ));

    };

    let TableFactor::Table { name, .. } = &table.relation else {
        return Err(SqlParseError::UnsupportedStatement(
            "only direct table UPDATE is currently supported".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(&name.to_string());
    let relation_bindings = parse_relation_bindings_from_table_with_joins(Some(table), statement)?;
    let joins = parse_joins_from_table_with_joins(Some(table), statement, &relation_bindings)?;

    if assignments.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE must include at least one SET assignment".to_string(),
        ));
    }

    let mut parsed_assignments = Vec::with_capacity(assignments.len());

    for assignment in assignments {

        let field_name = match &assignment.target {
            
            AssignmentTarget::ColumnName(name) => {

                let Some(last) = name.0.last() else {
                    return Err(SqlParseError::UnsupportedStatement(
                        "UPDATE assignment target is missing".to_string(),
                    ));
                };

                if name.0.len() > 2 {
                    return Err(SqlParseError::UnsupportedStatement(
                        "qualified UPDATE assignment targets are not supported".to_string(),
                    ));
                }

                common::normalize_identifier!(&last.value)
            },

            AssignmentTarget::Tuple(names) => {

                if names.len() != 1 {
                    return Err(SqlParseError::UnsupportedStatement(
                        "tuple UPDATE assignment targets are not supported".to_string(),
                    ));
                }

                let Some(last) = names[0].0.last() else {
                    return Err(SqlParseError::UnsupportedStatement(
                        "UPDATE assignment target is missing".to_string(),
                    ));
                };

                common::normalize_identifier!(&last.value)
            },

        };

        parsed_assignments.push(UpdateAssignment {
            field_name,
            value: parse_update_literal(&assignment.value)?,
        });

    }

    let where_condition = parse_select_condition_from_expr(selection.as_ref(), &relation_bindings)?;
    let pushdown_conditions = derive_relation_pushdown_conditions(
        where_condition.as_ref(),
        &relation_bindings,
        &joins,
    );

    Ok(UpdateRowsPlan {
        table_id,
        relations: relation_bindings,
        joins,
        pushdown_conditions,
        assignments: parsed_assignments,
        where_condition,
    })

}

fn parse_update_literal(expr: &Expr) -> Result<Option<Vec<u8>>, SqlParseError> {

    match expr {
        Expr::Value(value) => parse_update_value(value),
        Expr::Function(function) => evaluate_sql_function(function),
        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "UPDATE assignment value '{expr}' is not supported"
        ))),
    }

}

fn parse_update_value(value: &Value) -> Result<Option<Vec<u8>>, SqlParseError> {

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
            "UPDATE placeholder '{v}' is not supported"
        ))),

    }
    
}


#[cfg(test)]
#[path = "update_plan_test.rs"]
mod tests;
