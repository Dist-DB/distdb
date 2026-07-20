use sqlparser::ast::{
    AssignmentTarget, BinaryOperator, Expr, FunctionArg, FunctionArgExpr,
    FunctionArguments, OnInsert, SetExpr, Statement, UnaryOperator, Value,
};

use super::{
    evaluate_sql_function, parse_mysql_statements, InsertOnDuplicateArithmeticOp,
    InsertOnDuplicateAssignment, InsertOnDuplicateAssignmentValue,
    InsertOnDuplicateAssignmentOperand, UnaryArithmeticOp,
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
    let returning = super::mutation_returning::parse_mutation_returning_plan(
        insert.returning.as_deref(),
        "INSERT",
        std::slice::from_ref(&table_id),
    )?;
    
    let columns = insert
        .columns
        .iter()
        .map(|column| common::normalize_identifier!(&column.value))
        .collect::<Vec<_>>();

    let source = if let Some(source) = &insert.source {

        match source.body.as_ref() {

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
                let select_plan = super::select_plan::parse_select_read_plan_from_query(
                    source.as_ref(),
                    false,
                )?;
                InsertRowsSource::Select(select_plan)
            },

            _ => {
                return Err(SqlParseError::UnsupportedStatement(
                    "only INSERT ... VALUES, INSERT ... DEFAULT VALUES, or INSERT ... SELECT is currently supported".to_string(),
                ));
            }

        }

    } else {

        // MySQL INSERT ... DEFAULT VALUES is represented as an absent source.
        InsertRowsSource::Values(vec![Vec::new()])

    };

    let on_duplicate_update = parse_on_duplicate_assignments(insert.on.as_ref())?;

    Ok(InsertRowsPlan {
        table_id,
        ignore: insert.ignore,
        replace_into: insert.replace_into,
        columns,
        source,
        on_duplicate_update,
        returning,
    })

}

fn parse_on_duplicate_assignments(
    on_insert: Option<&OnInsert>,
) -> Result<Vec<InsertOnDuplicateAssignment>, SqlParseError> {

    let Some(on_insert) = on_insert else {
        return Ok(Vec::new());
    };

    let OnInsert::DuplicateKeyUpdate(assignments) = on_insert else {
        return Err(SqlParseError::UnsupportedStatement(
            "only ON DUPLICATE KEY UPDATE is supported for INSERT upsert modifiers".to_string(),
        ));
    };

    if assignments.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "ON DUPLICATE KEY UPDATE must contain at least one assignment".to_string(),
        ));
    }

    let mut parsed = Vec::with_capacity(assignments.len());

    for assignment in assignments {

        let field_name = match &assignment.target {

            AssignmentTarget::ColumnName(name) => {

                let Some(last) = name.0.last() else {
                    return Err(SqlParseError::UnsupportedStatement(
                        "ON DUPLICATE KEY UPDATE assignment target is missing".to_string(),
                    ));
                };

                common::normalize_identifier!(&last.value)
            }

            AssignmentTarget::Tuple(names) => {

                if names.len() != 1 {
                    return Err(SqlParseError::UnsupportedStatement(
                        "tuple ON DUPLICATE KEY UPDATE assignment targets are not supported"
                            .to_string(),
                    ));
                }

                let Some(last) = names[0].0.last() else {
                    return Err(SqlParseError::UnsupportedStatement(
                        "ON DUPLICATE KEY UPDATE assignment target is missing".to_string(),
                    ));
                };

                common::normalize_identifier!(&last.value)
            }

        };

        parsed.push(InsertOnDuplicateAssignment {
            field_name,
            value: parse_insert_on_duplicate_assignment_value(&assignment.value)?,
        });

    }

    Ok(parsed)

}

fn parse_insert_on_duplicate_assignment_value(
    expr: &Expr,
) -> Result<InsertOnDuplicateAssignmentValue, SqlParseError> {

    if let Expr::Nested(inner) = expr {
        return parse_insert_on_duplicate_assignment_value(inner);
    }

    if let Expr::BinaryOp { left, op, right } = expr {
        let op = parse_insert_on_duplicate_arithmetic_op(op).ok_or_else(|| {
            SqlParseError::UnsupportedStatement(format!(
                "ON DUPLICATE KEY UPDATE assignment value '{expr}' is not supported"
            ))
        })?;

        let left = parse_insert_on_duplicate_assignment_operand(left)?;
        let right = parse_insert_on_duplicate_assignment_operand(right)?;

        return Ok(InsertOnDuplicateAssignmentValue::Arithmetic { left, op, right });
    }

    if let Expr::UnaryOp { .. } = expr {
        let operand = parse_insert_on_duplicate_assignment_operand(expr)?;
        return Ok(unary_insert_on_duplicate_assignment_value(operand));
    }

    match expr {
        Expr::Identifier(ident) => {
            return Ok(InsertOnDuplicateAssignmentValue::ExistingColumn(
                common::normalize_identifier!(&ident.value),
            ));
        }
        Expr::CompoundIdentifier(parts) => {
            let Some(last) = parts.last() else {
                return Err(SqlParseError::UnsupportedStatement(
                    "ON DUPLICATE KEY UPDATE column reference is missing".to_string(),
                ));
            };
            return Ok(InsertOnDuplicateAssignmentValue::ExistingColumn(
                common::normalize_identifier!(&last.value),
            ));
        }
        _ => {}
    }

    if let Expr::Function(function) = expr {
        let function_name = common::normalize_identifier!(&function.name.to_string());
        if function_name == "values" {
            let FunctionArguments::List(list) = &function.args else {
                return Err(SqlParseError::UnsupportedStatement(
                    "ON DUPLICATE KEY UPDATE VALUES() expects exactly one column argument"
                        .to_string(),
                ));
            };

            if list.args.len() != 1 {
                return Err(SqlParseError::UnsupportedStatement(
                    "ON DUPLICATE KEY UPDATE VALUES() expects exactly one column argument"
                        .to_string(),
                ));
            }

            let column_name = match &list.args[0] {
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                    common::normalize_identifier!(&ident.value)
                }
                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                    let Some(last) = parts.last() else {
                        return Err(SqlParseError::UnsupportedStatement(
                            "ON DUPLICATE KEY UPDATE VALUES() requires a valid column identifier"
                                .to_string(),
                        ));
                    };
                    common::normalize_identifier!(&last.value)
                }
                _ => {
                    return Err(SqlParseError::UnsupportedStatement(
                        "ON DUPLICATE KEY UPDATE VALUES() requires a direct column identifier"
                            .to_string(),
                    ));
                }
            };

            return Ok(InsertOnDuplicateAssignmentValue::IncomingColumn(column_name));
        }

        return Ok(InsertOnDuplicateAssignmentValue::FunctionExpression(
            function.to_string(),
        ));
    }

    Ok(InsertOnDuplicateAssignmentValue::Literal(parse_insert_literal(expr)?))

}

fn parse_insert_on_duplicate_arithmetic_op(
    op: &BinaryOperator,
) -> Option<InsertOnDuplicateArithmeticOp> {
    match op {
        BinaryOperator::Plus => Some(InsertOnDuplicateArithmeticOp::Add),
        BinaryOperator::Minus => Some(InsertOnDuplicateArithmeticOp::Subtract),
        BinaryOperator::Multiply => Some(InsertOnDuplicateArithmeticOp::Multiply),
        BinaryOperator::Divide => Some(InsertOnDuplicateArithmeticOp::Divide),
        BinaryOperator::Modulo => Some(InsertOnDuplicateArithmeticOp::Modulo),
        _ => None,
    }
}

fn unary_insert_on_duplicate_assignment_value(
    operand: InsertOnDuplicateAssignmentOperand,
) -> InsertOnDuplicateAssignmentValue {
    InsertOnDuplicateAssignmentValue::Arithmetic {
        left: InsertOnDuplicateAssignmentOperand::Literal(Some(b"0".to_vec())),
        op: InsertOnDuplicateArithmeticOp::Add,
        right: operand,
    }
}

fn parse_insert_on_duplicate_assignment_operand(
    expr: &Expr,
) -> Result<InsertOnDuplicateAssignmentOperand, SqlParseError> {

    match expr {

        Expr::Nested(inner) => parse_insert_on_duplicate_assignment_operand(inner),

        Expr::BinaryOp { left, op, right } => {
            
            let op = parse_insert_on_duplicate_arithmetic_op(op).ok_or_else(|| {
                SqlParseError::UnsupportedStatement(format!(
                    "ON DUPLICATE KEY UPDATE arithmetic operand '{expr}' is not supported"
                ))
            })?;

            Ok(InsertOnDuplicateAssignmentOperand::Arithmetic {
                left: Box::new(parse_insert_on_duplicate_assignment_operand(left)?),
                op,
                right: Box::new(parse_insert_on_duplicate_assignment_operand(right)?),
            })

        },

        Expr::Identifier(ident) => Ok(InsertOnDuplicateAssignmentOperand::ExistingColumn(
            common::normalize_identifier!(&ident.value),
        )),

        Expr::CompoundIdentifier(parts) => {
            
            let Some(last) = parts.last() else {
                return Err(SqlParseError::UnsupportedStatement(
                    "ON DUPLICATE KEY UPDATE arithmetic operand column reference is missing"
                        .to_string(),
                ));
            };

            Ok(InsertOnDuplicateAssignmentOperand::ExistingColumn(
                common::normalize_identifier!(&last.value),
            ))
        
        },

        Expr::Function(function) => {

            let function_name = common::normalize_identifier!(&function.name.to_string());
            if function_name == "values" {
                let FunctionArguments::List(list) = &function.args else {
                    return Err(SqlParseError::UnsupportedStatement(
                        "ON DUPLICATE KEY UPDATE VALUES() expects exactly one column argument"
                            .to_string(),
                    ));
                };

                if list.args.len() != 1 {
                    return Err(SqlParseError::UnsupportedStatement(
                        "ON DUPLICATE KEY UPDATE VALUES() expects exactly one column argument"
                            .to_string(),
                    ));
                }

                let column_name = match &list.args[0] {

                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))) => {
                        common::normalize_identifier!(&ident.value)
                    },

                    FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::CompoundIdentifier(parts))) => {
                        let Some(last) = parts.last() else {
                            return Err(SqlParseError::UnsupportedStatement(
                                "ON DUPLICATE KEY UPDATE VALUES() requires a valid column identifier"
                                    .to_string(),
                            ));
                        };
                        common::normalize_identifier!(&last.value)
                    },

                    _ => {
                        return Err(SqlParseError::UnsupportedStatement(
                            "ON DUPLICATE KEY UPDATE VALUES() requires a direct column identifier"
                                .to_string(),
                        ));
                    }

                };

                return Ok(InsertOnDuplicateAssignmentOperand::IncomingColumn(column_name));
            }

            Ok(InsertOnDuplicateAssignmentOperand::FunctionExpression(
                function.to_string(),
            ))

        },

        Expr::Value(value) => Ok(InsertOnDuplicateAssignmentOperand::Literal(parse_insert_value(value)?)),

        Expr::UnaryOp { op, expr } => {

            let unary_op = match op {
                UnaryOperator::Minus => UnaryArithmeticOp::Minus,
                UnaryOperator::Plus => UnaryArithmeticOp::Plus,
                _ => {
                    return Err(SqlParseError::UnsupportedStatement(format!(
                        "ON DUPLICATE KEY UPDATE arithmetic operand '{expr}' is not supported"
                    )));
                }
            };

            Ok(InsertOnDuplicateAssignmentOperand::Unary {
                op: unary_op,
                operand: Box::new(parse_insert_on_duplicate_assignment_operand(expr)?),
            })

        },

        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "ON DUPLICATE KEY UPDATE arithmetic operand '{expr}' is not supported"
        ))),

    }

}

fn parse_insert_literal(expr: &Expr) -> Result<Option<Vec<u8>>, SqlParseError> {
    
    match expr {

        // MySQL VALUES(DEFAULT) should defer to field default/nullability handling.
        Expr::Identifier(ident)
            if common::normalize_identifier!(&ident.value) == "default" =>
        {
            Ok(None)
        }
        
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

        Value::SingleQuotedString(v) |
        Value::DoubleQuotedString(v) |
        Value::TripleSingleQuotedString(v) |
        Value::TripleDoubleQuotedString(v) |
        Value::EscapedStringLiteral(v) |
        Value::UnicodeStringLiteral(v) |
        Value::SingleQuotedByteStringLiteral(v) |
        Value::DoubleQuotedByteStringLiteral(v) |
        Value::TripleSingleQuotedByteStringLiteral(v) |
        Value::TripleDoubleQuotedByteStringLiteral(v) |
        Value::SingleQuotedRawStringLiteral(v) |
        Value::DoubleQuotedRawStringLiteral(v) |
        Value::TripleSingleQuotedRawStringLiteral(v) |
        Value::TripleDoubleQuotedRawStringLiteral(v) |
        Value::NationalStringLiteral(v) |
        Value::HexStringLiteral(v) => Ok(Some(v.as_bytes().to_vec())),

        Value::DollarQuotedString(v) => Ok(Some(v.value.as_bytes().to_vec())),
        
        // Placeholder tokens are passed through as raw literal bytes in the current
        // query model; bound-parameter substitution is handled outside this parser.
        Value::Placeholder(v) => Ok(Some(v.as_bytes().to_vec())),

    }

}


#[cfg(test)]
#[path = "insert_plan_test.rs"]
mod tests;
