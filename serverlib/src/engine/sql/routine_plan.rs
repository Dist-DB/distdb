use super::{
    parse_select_read_plan_from_statement, IfElseEndBranchPlan, IfElseEndPlan, SqlParseError,
};
use sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, FunctionArguments, Statement, Value,
};

pub fn parse_if_else_end_plan_from_statement(
    statement: &str,
) -> Result<IfElseEndPlan, SqlParseError> {
    
    let mut remaining = statement.trim().trim_end_matches(';').trim();
    let lowered = remaining.to_ascii_lowercase();

    if !lowered.starts_with("if ") {
        return Err(SqlParseError::UnsupportedStatement(
            "IF/ELSE/END routine block must start with IF".to_string(),
        ));
    }

    remaining = remaining[2..].trim_start();

    let mut branches = Vec::new();

    loop {

        let lower = remaining.to_ascii_lowercase();
        let then_index = lower.find(" then ").ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "IF/ELSE/END branch is missing THEN".to_string(),
            )
        })?;

        let condition_sql = remaining[..then_index].trim();
        if condition_sql.is_empty() {
            return Err(SqlParseError::UnsupportedStatement(
                "IF/ELSE/END branch condition is empty".to_string(),
            ));
        }

        let condition = parse_if_branch_condition(condition_sql)?;
        let branch_tail = remaining[(then_index + " then ".len())..].trim_start();

        let split = split_action_and_next_clause(branch_tail)?;

        let action_sql = split.action_sql.trim().trim_end_matches(';').trim().to_string();
        if action_sql.is_empty() {
            return Err(SqlParseError::UnsupportedStatement(
                "IF/ELSE/END branch action is empty".to_string(),
            ));
        }

        branches.push(IfElseEndBranchPlan {
            condition,
            action_sql,
        });

        match split.next_clause {

            NextClause::ElseIf => {
                remaining = branch_tail[split.next_clause_index + "elseif ".len()..].trim_start();
            },

            NextClause::ElseIfSpaced => {
                remaining =
                    branch_tail[split.next_clause_index + "else if ".len()..].trim_start();
            },

            NextClause::Else => {
                let else_tail = branch_tail[split.next_clause_index + "else ".len()..].trim_start();
                let lower_else = else_tail.to_ascii_lowercase();
                let end_index = lower_else.rfind(" end if").or_else(|| {
                    if lower_else.ends_with("end if") {
                        Some(lower_else.len() - "end if".len())
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    SqlParseError::UnsupportedStatement(
                        "IF/ELSE/END block is missing END IF".to_string(),
                    )
                })?;

                let else_action_sql = else_tail[..end_index]
                    .trim()
                    .trim_end_matches(';')
                    .trim()
                    .to_string();

                if else_action_sql.is_empty() {
                    return Err(SqlParseError::UnsupportedStatement(
                        "IF/ELSE/END ELSE action is empty".to_string(),
                    ));
                }

                return Ok(IfElseEndPlan {
                    branches,
                    else_action_sql: Some(else_action_sql),
                });
            },

            NextClause::EndIf => {
                return Ok(IfElseEndPlan {
                    branches,
                    else_action_sql: None,
                });
            },

        }

    }

}

pub fn parse_if_else_end_plan_from_create_procedure_statement(
    statement: &str,
) -> Result<Option<IfElseEndPlan>, SqlParseError> {

    let body = extract_create_procedure_body(statement)?;
    let trimmed_body = body.trim().trim_end_matches(';').trim();

    if trimmed_body.is_empty() {
        return Ok(None);
    }

    if !trimmed_body.to_ascii_lowercase().starts_with("if ") {
        return Ok(None);
    }

    parse_if_else_end_plan_from_statement(trimmed_body).map(Some)

}

pub fn parse_create_procedure_parameter_names_from_statement(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create procedure") {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE PROCEDURE".to_string(),
        ));
    }

    let begin_index = lowered.find(" begin ").ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE block is missing BEGIN".to_string(),
        )
    })?;

    let header = &trimmed[..begin_index];
    let Some(open_index) = header.find('(') else {
        return Ok(Vec::new());
    };

    let close_index = matching_close_parenthesis(header, open_index).ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE parameter list is malformed".to_string(),
        )
    })?;

    let raw_params = &header[(open_index + 1)..close_index];
    split_top_level_csv(raw_params)
        .into_iter()
        .map(|param| parse_procedure_parameter_name(&param))
        .collect()

}

pub fn bind_call_procedure_arguments(
    create_procedure_sql: &str,
    call_statement: &Statement,
) -> Result<Vec<(String, Vec<u8>)>, SqlParseError> {

    let parameter_names = parse_create_procedure_parameter_names_from_statement(create_procedure_sql)?;

    let argument_values = parse_call_argument_values(call_statement).map_err(|message| {
        SqlParseError::UnsupportedStatement(format!("CALL argument parse failed: {message}"))
    })?;

    if parameter_names.len() != argument_values.len() {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "CALL argument mismatch: expected {} values but received {}",
            parameter_names.len(),
            argument_values.len(),
        )));
    }

    Ok(parameter_names.into_iter().zip(argument_values).collect())

}

fn extract_create_procedure_body(statement: &str) -> Result<&str, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create procedure") {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE PROCEDURE".to_string(),
        ));
    }

    let begin_index = lowered.find(" begin ").ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE block is missing BEGIN".to_string(),
        )
    })?;

    let after_begin = begin_index + " begin ".len();
    let body_and_end = &trimmed[after_begin..];
    let lowered_body_and_end = body_and_end.to_ascii_lowercase();

    let end_index = lowered_body_and_end.rfind(" end").or_else(|| {
        if lowered_body_and_end.ends_with("end") {
            Some(lowered_body_and_end.len() - "end".len())
        } else {
            None
        }
    })
    .ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE block is missing END".to_string(),
        )
    })?;

    Ok(body_and_end[..end_index].trim())

}

fn parse_if_branch_condition(condition_sql: &str) -> Result<crate::SelectCondition, SqlParseError> {

    let wrapped = format!("select id from __if_eval where {condition_sql}");
    let plan = parse_select_read_plan_from_statement(&wrapped)?;

    plan.where_condition.ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "IF/ELSE/END branch condition could not be parsed".to_string(),
        )
    })

}

fn parse_call_argument_values(statement: &Statement) -> Result<Vec<Vec<u8>>, String> {

    let Statement::Call(function) = statement else {
        return Err("statement is not CALL".to_string());
    };

    let call_args: &[FunctionArg] = match &function.args {
        FunctionArguments::None => &[],
        FunctionArguments::List(list) => list.args.as_slice(),
        FunctionArguments::Subquery(_) => {
            return Err("CALL subquery arguments are not supported".to_string());
        }
    };

    call_args
        .iter()
        .map(call_argument_to_bytes)
        .collect()

}

fn call_argument_to_bytes(argument: &FunctionArg) -> Result<Vec<u8>, String> {

    let expression = match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        _ => return Err("unsupported CALL argument".to_string()),
    };

    expression_to_bytes(expression)

}

fn expression_to_bytes(expression: &Expr) -> Result<Vec<u8>, String> {

    match expression {
        Expr::Value(value) => value_to_bytes(value),

        Expr::UnaryOp { op, expr } => match (op, expr.as_ref()) {
            (sqlparser::ast::UnaryOperator::Plus, Expr::Value(Value::Number(value, _))) => {
                Ok(value.clone().into_bytes())
            }
            (sqlparser::ast::UnaryOperator::Minus, Expr::Value(Value::Number(value, _))) => {
                Ok(format!("-{value}").into_bytes())
            }
            _ => Err("unsupported CALL unary argument".to_string()),
        },

        Expr::Identifier(ident) => Ok(common::normalize_identifier!(&ident.value).into_bytes()),

        Expr::CompoundIdentifier(identifiers) => Ok(identifiers
            .iter()
            .map(|ident| common::normalize_identifier!(&ident.value))
            .collect::<Vec<_>>()
            .join(".")
            .into_bytes()),

        _ => Err("unsupported CALL argument expression".to_string()),
    }

}

fn value_to_bytes(value: &Value) -> Result<Vec<u8>, String> {

    match value {
        Value::Boolean(v) => Ok(v.to_string().into_bytes()),
        Value::Number(v, _) => Ok(v.to_string().into_bytes()),

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
        | Value::HexStringLiteral(v) => Ok(v.as_bytes().to_vec()),

        Value::DollarQuotedString(v) => Ok(v.value.as_bytes().to_vec()),

        Value::Null => Err("CALL NULL arguments are not supported".to_string()),

        Value::Placeholder(v) => Err(format!(
            "CALL placeholder argument '{}' is not supported",
            v
        )),
    }

}

fn parse_procedure_parameter_name(parameter: &str) -> Result<String, SqlParseError> {

    let mut tokens = parameter.split_whitespace();
    let Some(first) = tokens.next() else {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE parameter definition is empty".to_string(),
        ));
    };

    let candidate = if first.eq_ignore_ascii_case("in")
        || first.eq_ignore_ascii_case("out")
        || first.eq_ignore_ascii_case("inout")
    {
        tokens.next().ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "CREATE PROCEDURE parameter name is missing".to_string(),
            )
        })?
    } else {
        first
    };

    Ok(common::normalize_identifier!(candidate.trim_matches('`').trim_matches('"')))

}

fn matching_close_parenthesis(text: &str, open_index: usize) -> Option<usize> {

    let mut depth = 0usize;

    for (index, ch) in text.char_indices().skip(open_index) {
        if ch == '(' {
            depth += 1;
            continue;
        }

        if ch == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(index);
            }
        }
    }

    None

}

fn split_top_level_csv(text: &str) -> Vec<String> {

    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;

    for ch in text.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    parts.push(trimmed.to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        parts.push(trimmed.to_string());
    }

    parts

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextClause {
    ElseIf,
    ElseIfSpaced,
    Else,
    EndIf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ClauseSplit {
    action_sql: String,
    next_clause: NextClause,
    next_clause_index: usize,
}

fn split_action_and_next_clause(branch_tail: &str) -> Result<ClauseSplit, SqlParseError> {

    let lowered = branch_tail.to_ascii_lowercase();

    let candidates = [
        (" elseif ", NextClause::ElseIf, "elseif ".len()),
        (" else if ", NextClause::ElseIfSpaced, "else if ".len()),
        (" else ", NextClause::Else, "else ".len()),
        (" end if", NextClause::EndIf, "end if".len()),
    ];

    let mut earliest: Option<(usize, NextClause, usize)> = None;

    for (marker, clause, clause_len) in candidates {
        if let Some(index) = lowered.find(marker) {
            match earliest {
                Some((current, _, _)) if current <= index => {}
                _ => earliest = Some((index, clause, clause_len)),
            }
        }
    }

    let Some((index, clause, _)) = earliest else {
        return Err(SqlParseError::UnsupportedStatement(
            "IF/ELSE/END branch must end with ELSEIF, ELSE, or END IF".to_string(),
        ));
    };

    let action_sql = branch_tail[..index].trim().to_string();

    Ok(ClauseSplit {
        action_sql,
        next_clause: clause,
        next_clause_index: index + 1,
    })

}

#[cfg(test)]
#[path = "routine_plan_test.rs"]
mod tests;
