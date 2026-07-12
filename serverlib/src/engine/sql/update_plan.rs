use sqlparser::ast::{AssignmentTarget, BinaryOperator, Expr, Statement, TableFactor, TableWithJoins, UnaryOperator, Value};

use super::{
    derive_relation_pushdown_conditions, parse_joins_from_table_with_joins,
    parse_mysql_statements, parse_relation_bindings_from_table_with_joins,
    text_scan::{find_top_level_keyword, split_top_level_csv_trimmed},
    ORDER_EXPR_ABS_PREFIX, ORDER_EXPR_CEIL_PREFIX, ORDER_EXPR_FLOOR_PREFIX,
    ORDER_EXPR_LENGTH_PREFIX, ORDER_EXPR_LOWER_PREFIX, ORDER_EXPR_LTRIM_PREFIX,
    ORDER_EXPR_REVERSE_PREFIX, ORDER_EXPR_ROUND_PREFIX, ORDER_EXPR_ROUND_SCALE_PREFIX,
    ORDER_EXPR_RTRIM_PREFIX, ORDER_EXPR_TRIM_PREFIX, ORDER_EXPR_UPPER_PREFIX,
    parse_select_condition_from_expr, SelectCondition, SelectJoin, SelectJoinKind,
    SelectOrderByItem, SqlParseError, UpdateArithmeticOp, UpdateAssignment,
    UnaryArithmeticOp, UpdateAssignmentOperand, UpdateAssignmentValue, UpdateRowsPlan,
};

pub fn parse_update_rows_from_statement(statement: &str) -> Result<UpdateRowsPlan, SqlParseError> {

    let core_statement = strip_update_order_limit_suffix(statement);

    let parsed = parse_mysql_statements(&core_statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::Update {
        table,
        assignments,
        from,
        selection,
        returning,
        ..
    } = single
    else {
        
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not UPDATE".to_string(),
        ));

    };

    let TableFactor::Table { name, alias, .. } = &table.relation else {
        return Err(SqlParseError::UnsupportedStatement(
            "only direct table UPDATE is currently supported".to_string(),
        ));
    };

    let table_id = common::normalize_identifier!(&name.to_string());
    let mut returning_qualifiers = vec![table_id.clone()];
    if let Some(alias) = alias.as_ref() {
        returning_qualifiers.push(common::normalize_identifier!(&alias.name.value));
    }

    let returning = super::mutation_returning::parse_mutation_returning_plan(
        returning.as_deref(),
        "UPDATE",
        &returning_qualifiers,
    )?;

    let (relation_bindings, joins) = build_update_relation_graph(table, from.as_ref(), statement)?;

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
            value: parse_update_assignment_value(&assignment.value)?,
        });

    }

    let where_condition = parse_select_condition_from_expr(selection.as_ref(), &relation_bindings)?;
    let pushdown_conditions = derive_relation_pushdown_conditions(
        where_condition.as_ref(),
        &relation_bindings,
        &joins,
    );
    let (order_by, limit) = parse_update_order_by_limit_from_statement_text(
        statement,
        &table_id,
        alias.as_ref(),
    )?;

    Ok(UpdateRowsPlan {
        table_id,
        relations: relation_bindings,
        joins,
        pushdown_conditions,
        order_by,
        limit,
        assignments: parsed_assignments,
        where_condition,
        returning,
    })

}

fn strip_update_order_limit_suffix(statement: &str) -> String {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    let order_by_start = find_top_level_keyword(&lowered, "order by");
    let limit_start = find_top_level_keyword(&lowered, "limit");

    let suffix_start = match (order_by_start, limit_start) {
        (Some(order), Some(limit)) => Some(order.min(limit)),
        (Some(order), None) => Some(order),
        (None, Some(limit)) => Some(limit),
        (None, None) => None,
    };

    if let Some(start) = suffix_start {
        return trimmed[..start].trim_end().to_string();
    }

    trimmed.to_string()

}

fn build_update_relation_graph(
    table: &TableWithJoins,
    from: Option<&TableWithJoins>,
    statement: &str,
) -> Result<(Vec<super::SelectRelation>, Vec<SelectJoin>), SqlParseError> {

    let mut relation_bindings = parse_relation_bindings_from_table_with_joins(Some(table), statement)?;
    let mut joins = parse_joins_from_table_with_joins(Some(table), statement, &relation_bindings)?;

    let Some(from) = from else {
        return Ok((relation_bindings, joins));
    };

    let from_relations = parse_relation_bindings_from_table_with_joins(Some(from), statement)?;
    if from_relations.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE FROM must include at least one relation".to_string(),
        ));
    }

    let from_joins = parse_joins_from_table_with_joins(Some(from), statement, &from_relations)?;

    // UPDATE ... FROM is modeled as a target-first relation graph; the FROM primary relation
    // is connected as an inner cross join and ON constraints inside FROM joins are preserved.
    joins.push(SelectJoin {
        kind: SelectJoinKind::Inner,
        relation: from_relations[0].clone(),
        on_condition: SelectCondition::And(Vec::new()),
    });

    joins.extend(from_joins);
    relation_bindings.extend(from_relations);

    Ok((relation_bindings, joins))

}

fn parse_update_assignment_value(expr: &Expr) -> Result<UpdateAssignmentValue, SqlParseError> {

    match expr {

        Expr::Nested(inner) => parse_update_assignment_value(inner),

        Expr::BinaryOp { left, op, right } => {
            let op = parse_update_arithmetic_op(op).ok_or_else(|| {
                SqlParseError::UnsupportedStatement(format!(
                    "UPDATE assignment value '{expr}' is not supported"
                ))
            })?;

            let left = parse_update_assignment_operand(left)?;
            let right = parse_update_assignment_operand(right)?;

            Ok(UpdateAssignmentValue::Arithmetic { left, op, right })
        },

        Expr::UnaryOp { .. } => {
            let operand = parse_update_assignment_operand(expr)?;
            Ok(unary_update_assignment_value(operand))
        },

        Expr::Identifier(identifier) => Ok(UpdateAssignmentValue::ExistingColumn(
            common::normalize_identifier!(&identifier.value),
        )),

        Expr::CompoundIdentifier(parts) => {

            if parts.len() > 2 {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "UPDATE assignment value '{expr}' is not supported"
                )));
            }

            let Some(last) = parts.last() else {
                return Err(SqlParseError::UnsupportedStatement(
                    "UPDATE assignment column reference is missing".to_string(),
                ));
            };

            Ok(UpdateAssignmentValue::ExistingColumn(
                common::normalize_identifier!(&last.value),
            ))

        },
        
        Expr::Value(value) => Ok(UpdateAssignmentValue::Literal(parse_update_value(value)?)),
        
        Expr::Function(function) => {
            Ok(UpdateAssignmentValue::FunctionExpression(function.to_string()))
        },
        
        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "UPDATE assignment value '{expr}' is not supported"
        ))),

    }

}

fn parse_update_arithmetic_op(op: &BinaryOperator) -> Option<UpdateArithmeticOp> {
    
    match op {
        
        BinaryOperator::Plus => Some(UpdateArithmeticOp::Add),

        BinaryOperator::Minus => Some(UpdateArithmeticOp::Subtract),

        BinaryOperator::Multiply => Some(UpdateArithmeticOp::Multiply),

        BinaryOperator::Divide => Some(UpdateArithmeticOp::Divide),

        BinaryOperator::Modulo => Some(UpdateArithmeticOp::Modulo),

        _ => None,

    }

}

fn unary_update_assignment_value(operand: UpdateAssignmentOperand) -> UpdateAssignmentValue {
    UpdateAssignmentValue::Arithmetic {
        left: UpdateAssignmentOperand::Literal(Some(b"0".to_vec())),
        op: UpdateArithmeticOp::Add,
        right: operand,
    }
}

fn parse_update_assignment_operand(expr: &Expr) -> Result<UpdateAssignmentOperand, SqlParseError> {
    
    match expr {

        Expr::Nested(inner) => parse_update_assignment_operand(inner),

        Expr::BinaryOp { left, op, right } => {
            let op = parse_update_arithmetic_op(op).ok_or_else(|| {
                SqlParseError::UnsupportedStatement(format!(
                    "UPDATE arithmetic operand '{expr}' is not supported"
                ))
            })?;

            Ok(UpdateAssignmentOperand::Arithmetic {
                left: Box::new(parse_update_assignment_operand(left)?),
                op,
                right: Box::new(parse_update_assignment_operand(right)?),
            })
        },

        Expr::Identifier(identifier) => Ok(UpdateAssignmentOperand::ExistingColumn(
            common::normalize_identifier!(&identifier.value),
        )),

        Expr::CompoundIdentifier(parts) => {
            if parts.len() > 2 {
                return Err(SqlParseError::UnsupportedStatement(format!(
                    "UPDATE arithmetic operand '{expr}' is not supported"
                )));
            }

            let Some(last) = parts.last() else {
                return Err(SqlParseError::UnsupportedStatement(
                    "UPDATE arithmetic operand column reference is missing".to_string(),
                ));
            };

            Ok(UpdateAssignmentOperand::ExistingColumn(
                common::normalize_identifier!(&last.value),
            ))
        },

        Expr::Value(value) => Ok(UpdateAssignmentOperand::Literal(parse_update_value(value)?)),

        Expr::Function(function) => {
            Ok(UpdateAssignmentOperand::FunctionExpression(function.to_string()))
        },

        Expr::UnaryOp { op, expr } => {
            let unary_op = match op {
                UnaryOperator::Minus => UnaryArithmeticOp::Minus,
                UnaryOperator::Plus => UnaryArithmeticOp::Plus,
                _ => {
                    return Err(SqlParseError::UnsupportedStatement(format!(
                        "UPDATE arithmetic operand '{expr}' is not supported"
                    )));
                }
            };

            Ok(UpdateAssignmentOperand::Unary {
                op: unary_op,
                operand: Box::new(parse_update_assignment_operand(expr)?),
            })
        },

        _ => Err(SqlParseError::UnsupportedStatement(format!(
            "UPDATE arithmetic operand '{expr}' is not supported"
        ))),

    }

}

fn parse_update_order_by_limit_from_statement_text(
    statement: &str,
    target_table_id: &str,
    target_alias: Option<&sqlparser::ast::TableAlias>,
) -> Result<(Vec<SelectOrderByItem>, Option<usize>), SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    let order_by_start = find_top_level_keyword(&lowered, "order by");
    let limit_start = find_top_level_keyword(&lowered, "limit");

    if order_by_start.is_none() && limit_start.is_none() {
        return Ok((Vec::new(), None));
    }

    if let (Some(order_idx), Some(limit_idx)) = (order_by_start, limit_start)
        && limit_idx < order_idx
    {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE LIMIT before ORDER BY is not supported".to_string(),
        ));
    }

    let order_by = if let Some(order_idx) = order_by_start {
        let order_expr_start = order_idx + "order by".len();
        let order_expr_end = limit_start.unwrap_or(trimmed.len());
        let clause = trimmed[order_expr_start..order_expr_end].trim();
        parse_update_order_by_items_from_text(clause, target_table_id, target_alias)?
    } else {
        Vec::new()
    };

    let limit = if let Some(limit_idx) = limit_start {
        let limit_expr_start = limit_idx + "limit".len();
        let clause = trimmed[limit_expr_start..].trim();
        Some(parse_update_limit_from_text(clause)?)
    } else {
        None
    };

    Ok((order_by, limit))

}

fn parse_update_order_by_items_from_text(
    clause: &str,
    target_table_id: &str,
    target_alias: Option<&sqlparser::ast::TableAlias>,
) -> Result<Vec<SelectOrderByItem>, SqlParseError> {

    if clause.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY requires at least one field".to_string(),
        ));
    }

    let mut table_aliases = vec![target_table_id.to_string()];
    if let Some(alias) = target_alias {
        table_aliases.push(common::normalize_identifier!(&alias.name.value));
    }

    let parts = split_top_level_csv_trimmed(clause);
    let mut items = Vec::with_capacity(parts.len());

    for raw_part in parts {

        let part = raw_part.trim();

        if part.is_empty() {
            continue;
        }

        let tokens = part
            .split_whitespace()
            .map(str::to_string)
            .collect::<Vec<_>>();

        if tokens.is_empty() || tokens.len() > 2 {
            return Err(SqlParseError::UnsupportedStatement(
                "UPDATE ORDER BY currently supports only direct column references"
                    .to_string(),
            ));
        }

        let descending = if tokens.len() == 2 {
            if tokens[1].eq_ignore_ascii_case("desc") {
                true
            } else if tokens[1].eq_ignore_ascii_case("asc") {
                false
            } else {
                return Err(SqlParseError::UnsupportedStatement(
                    "UPDATE ORDER BY direction must be ASC or DESC".to_string(),
                ));
            }
        } else {
            false
        };

        let field_name = parse_update_order_expression_token(tokens[0].as_str(), &table_aliases)?;

        if field_name.is_empty() {
            return Err(SqlParseError::UnsupportedStatement(
                "UPDATE ORDER BY currently supports only direct column references"
                    .to_string(),
            ));
        }

        items.push(SelectOrderByItem {
            field_name,
            descending,
        });
    }

    if items.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY requires at least one field".to_string(),
        ));
    }

    Ok(items)

}

fn is_simple_update_order_identifier(token: &str) -> bool {

    if token.is_empty() {
        return false;
    }

    let parts = token.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 {
        return false;
    }

    parts.iter().all(|part| {
        let segment = part.trim_matches('`');
        !segment.is_empty()
            && segment
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    })

}

fn parse_update_order_expression_token(
    token: &str,
    table_aliases: &[String],
) -> Result<String, SqlParseError> {

    let raw = token.trim();
    let lowered = raw.to_ascii_lowercase();

    if lowered.starts_with("lower(") && raw.ends_with(')') {
        let inner = &raw["lower(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_LOWER_PREFIX}{column}"));
    }

    if lowered.starts_with("upper(") && raw.ends_with(')') {
        let inner = &raw["upper(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_UPPER_PREFIX}{column}"));
    }

    if lowered.starts_with("abs(") && raw.ends_with(')') {
        let inner = &raw["abs(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_ABS_PREFIX}{column}"));
    }

    if lowered.starts_with("length(") && raw.ends_with(')') {
        let inner = &raw["length(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_LENGTH_PREFIX}{column}"));
    }

    if lowered.starts_with("char_length(") && raw.ends_with(')') {
        let inner = &raw["char_length(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_LENGTH_PREFIX}{column}"));
    }

    if lowered.starts_with("len(") && raw.ends_with(')') {
        let inner = &raw["len(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_LENGTH_PREFIX}{column}"));
    }

    if lowered.starts_with("lcase(") && raw.ends_with(')') {
        let inner = &raw["lcase(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_LOWER_PREFIX}{column}"));
    }

    if lowered.starts_with("ucase(") && raw.ends_with(')') {
        let inner = &raw["ucase(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_UPPER_PREFIX}{column}"));
    }

    if lowered.starts_with("reverse(") && raw.ends_with(')') {
        let inner = &raw["reverse(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_REVERSE_PREFIX}{column}"));
    }

    if lowered.starts_with("trim(") && raw.ends_with(')') {
        let inner = &raw["trim(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_TRIM_PREFIX}{column}"));
    }

    if lowered.starts_with("ltrim(") && raw.ends_with(')') {
        let inner = &raw["ltrim(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_LTRIM_PREFIX}{column}"));
    }

    if lowered.starts_with("rtrim(") && raw.ends_with(')') {
        let inner = &raw["rtrim(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_RTRIM_PREFIX}{column}"));
    }

    if lowered.starts_with("ceil(") && raw.ends_with(')') {
        let inner = &raw["ceil(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_CEIL_PREFIX}{column}"));
    }

    if lowered.starts_with("ceiling(") && raw.ends_with(')') {
        let inner = &raw["ceiling(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_CEIL_PREFIX}{column}"));
    }

    if lowered.starts_with("floor(") && raw.ends_with(')') {
        let inner = &raw["floor(".len()..raw.len() - 1];
        let column = parse_update_order_target_column(inner, table_aliases)?;
        return Ok(format!("{ORDER_EXPR_FLOOR_PREFIX}{column}"));
    }

    if lowered.starts_with("round(") && raw.ends_with(')') {
        
        let inner = &raw["round(".len()..raw.len() - 1];
        let args = split_top_level_csv_trimmed(inner);
        if args.len() == 1 {
            let column = parse_update_order_target_column(args[0].as_str(), table_aliases)?;
            return Ok(format!("{ORDER_EXPR_ROUND_PREFIX}{column}"));
        }

        if args.len() == 2 {
            let column = parse_update_order_target_column(args[0].as_str(), table_aliases)?;
            let scale = parse_update_order_round_scale(args[1].as_str())?;
            return Ok(format!("{ORDER_EXPR_ROUND_SCALE_PREFIX}{scale}:{column}"));
        }

        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY ROUND currently supports ROUND(column) or ROUND(column, scale)"
                .to_string(),
        ));

    }

    parse_update_order_target_column(raw, table_aliases)

}

fn parse_update_order_target_column(
    column_token: &str,
    table_aliases: &[String],
) -> Result<String, SqlParseError> {

    let column_token = column_token.trim().trim_matches('`');

    if !is_simple_update_order_identifier(column_token) {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY currently supports direct columns or LOWER/UPPER/ABS/LENGTH/CHAR_LENGTH/LEN/LCASE/UCASE/REVERSE/TRIM/LTRIM/RTRIM/CEIL/CEILING/FLOOR/ROUND(column)"
                .to_string(),
        ));
    }

    let field_name = if let Some((qualifier, column)) = column_token.split_once('.') {
        let normalized_qualifier = common::normalize_identifier!(qualifier.trim_matches('`'));
        if !table_aliases.iter().any(|candidate| candidate == &normalized_qualifier) {
            return Err(SqlParseError::UnsupportedStatement(
                "UPDATE ORDER BY currently supports only target-table column references"
                    .to_string(),
            ));
        }

        common::normalize_identifier!(column.trim_matches('`'))
    } else {
        common::normalize_identifier!(column_token)
    };

    if field_name.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY currently supports direct columns or LOWER/UPPER/ABS/LENGTH/CHAR_LENGTH/LEN/LCASE/UCASE/REVERSE/TRIM/LTRIM/RTRIM/CEIL/CEILING/FLOOR/ROUND(column)"
                .to_string(),
        ));
    }

    Ok(field_name)

}

fn parse_update_order_round_scale(token: &str) -> Result<i32, SqlParseError> {
    
    let trimmed = token.trim();
    
    if trimmed.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY ROUND scale must be an integer literal".to_string(),
        ));
    }

    trimmed.parse::<i32>().map_err(|_| {
        SqlParseError::UnsupportedStatement(
            "UPDATE ORDER BY ROUND scale must be an integer literal".to_string(),
        )
    })

}

fn parse_update_limit_from_text(clause: &str) -> Result<usize, SqlParseError> {

    let mut tokens = clause.split_whitespace();
    let Some(first) = tokens.next() else {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE LIMIT requires a numeric value".to_string(),
        ));
    };

    if first.contains(',') {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE LIMIT offset syntax is not supported yet".to_string(),
        ));
    }

    if tokens.next().is_some() {
        return Err(SqlParseError::UnsupportedStatement(
            "UPDATE LIMIT currently supports only a single numeric literal"
                .to_string(),
        ));
    }

    first.parse::<usize>().map_err(|_| {
        SqlParseError::UnsupportedStatement(format!(
            "UPDATE LIMIT value '{}' is out of range",
            first
        ))
    })

}

fn parse_update_value(value: &Value) -> Result<Option<Vec<u8>>, SqlParseError> {

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
        
        Value::Placeholder(v) => Err(SqlParseError::UnsupportedStatement(format!(
            "UPDATE placeholder '{v}' is not supported"
        ))),

    }
    
}


#[cfg(test)]
#[path = "update_plan_test.rs"]
mod tests;
