use super::{
    evaluate_expression_sql_to_bytes, evaluate_sql_function, parse_select_read_plan_from_statement,
    text_scan::split_top_level_csv_trimmed,
    IfElseEndBranchPlan, IfElseEndPlan, RoutineArgumentBinding,
    RoutineParameterDeclaration, RoutineParameterMode, SqlParseError,
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
                remaining = branch_tail[split.next_clause_index + "else if ".len()..].trim_start();
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

    let body = if statement
        .trim()
        .to_ascii_lowercase()
        .starts_with("create function")
    {
        extract_create_function_action_sql(statement)?
    } else {
        extract_create_procedure_body(statement)?.to_string()
    };

    let trimmed_body = body.trim().trim_end_matches(';').trim();

    if trimmed_body.is_empty() {
        return Ok(None);
    }

    let lowered_body = trimmed_body.to_ascii_lowercase();

    if lowered_body.starts_with("if ") {
        return parse_if_else_end_plan_from_statement(trimmed_body).map(Some);
    }

    if lowered_body.starts_with("case ") {
        return parse_case_plan_from_statement(trimmed_body).map(Some);
    }

    if let Some((start_index, control_flow_kind)) =
        find_top_level_routine_control_flow_start(trimmed_body)
    {
        let control_flow_fragment = trimmed_body[start_index..].trim_start();

        return match control_flow_kind {

            RoutineControlFlowKind::If => {
                parse_if_else_end_plan_from_statement(control_flow_fragment).map(Some)
            },
            
            RoutineControlFlowKind::Case => {
                parse_case_plan_from_statement(control_flow_fragment).map(Some)
            }

        };
    }

    Ok(None)

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutineControlFlowKind {
    If,
    Case,
}

fn find_top_level_routine_control_flow_start(body: &str) -> Option<(usize, RoutineControlFlowKind)> {

    let lowered = body.to_ascii_lowercase();
    let mut best_match: Option<(usize, RoutineControlFlowKind)> = None;

    for (token, kind) in [
        ("if", RoutineControlFlowKind::If),
        ("case", RoutineControlFlowKind::Case),
    ] {
        let mut from = 0usize;

        while let Some(found) = lowered[from..].find(token) {

            let index = from + found;
            let is_boundary = is_keyword_boundary(&lowered, index, token.len());
            let statement_start = is_statement_start_boundary(body, index);

            if is_boundary && statement_start {
                match best_match {
                    Some((current_index, _)) if current_index <= index => {}
                    _ => best_match = Some((index, kind)),
                }
                break;
            }

            from = index + token.len();

        }
    }

    best_match

}

fn is_keyword_boundary(haystack: &str, index: usize, length: usize) -> bool {

    let bytes = haystack.as_bytes();
    let before_ok = index == 0 || !bytes[index - 1].is_ascii_alphanumeric();
    let after_index = index + length;
    let after_ok = after_index >= bytes.len() || !bytes[after_index].is_ascii_alphanumeric();

    before_ok && after_ok

}

fn is_statement_start_boundary(text: &str, index: usize) -> bool {

    if index == 0 {
        return true;
    }

    let prefix = &text[..index];
    for ch in prefix.chars().rev() {
        if ch.is_ascii_whitespace() {
            continue;
        }
        return ch == ';';
    }

    true

}

pub fn parse_create_procedure_parameter_names_from_statement(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    Ok(parse_create_procedure_parameter_declarations_from_statement(statement)?
        .into_iter()
        .map(|parameter| parameter.name)
        .collect())

}

pub fn parse_create_procedure_parameter_declarations_from_statement(
    statement: &str,
) -> Result<Vec<RoutineParameterDeclaration>, SqlParseError> {

    parse_create_procedure_parameter_declarations(statement)

}

pub fn parse_create_function_parameter_names_from_statement(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    parse_create_function_parameter_names(statement)

}

pub fn extract_create_function_action_sql(statement: &str) -> Result<String, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create function") {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE FUNCTION".to_string(),
        ));
    }

    if let Some(begin_index) = find_keyword_boundary_index(&lowered, "begin") {

        let after_begin = begin_index + "begin".len();
        let body_and_end = trimmed[after_begin..].trim_start();
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
                "CREATE FUNCTION block is missing END".to_string(),
            )
        })?;

        return Ok(body_and_end[..end_index].trim().trim_end_matches(';').trim().to_string());

    }

    let Some(return_index) = find_keyword_boundary_index(&lowered, "return") else {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE FUNCTION must contain RETURN or BEGIN".to_string(),
        ));
    };

    let return_expression = trimmed[(return_index + "return".len())..]
        .trim()
        .trim_end_matches(';')
        .trim();

    if return_expression.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE FUNCTION RETURN expression is empty".to_string(),
        ));
    }

    Ok(format!("select {return_expression}"))

}

pub fn extract_create_function_return_expression(
    statement: &str,
) -> Result<Option<String>, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create function") {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE FUNCTION".to_string(),
        ));
    }

    if find_keyword_boundary_index(&lowered, "begin").is_some() {
        return Ok(None);
    }

    let Some(return_index) = find_keyword_boundary_index(&lowered, "return") else {
        return Ok(None);
    };

    let return_expression = trimmed[(return_index + "return".len())..]
        .trim()
        .trim_end_matches(';')
        .trim();

    if return_expression.is_empty() {
        Ok(None)
    } else {
        Ok(Some(return_expression.to_string()))
    }

}

pub fn parse_create_function_return_type_from_statement(
    statement: &str,
) -> Result<Option<String>, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create function") {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE FUNCTION".to_string(),
        ));
    }

    let Some(returns_index) = find_keyword_boundary_index(&lowered, "returns") else {
        return Ok(None);
    };

    let after_returns = trimmed[(returns_index + "returns".len())..].trim_start();
    let lowered_after_returns = after_returns.to_ascii_lowercase();

    let next_keyword_index = find_keyword_boundary_index(&lowered_after_returns, "return")
        .or_else(|| find_keyword_boundary_index(&lowered_after_returns, "begin"));

    let return_type = next_keyword_index
        .map(|index| after_returns[..index].trim())
        .unwrap_or_else(|| after_returns.trim())
        .trim_end_matches(';')
        .trim();

    if return_type.is_empty() {
        Ok(None)
    } else {
        Ok(Some(return_type.to_string()))
    }

}

fn parse_create_procedure_parameter_declarations(
    statement: &str,
) -> Result<Vec<RoutineParameterDeclaration>, SqlParseError> {

    let parameter_segments = parse_create_routine_parameter_segments(statement)?;

    parameter_segments
        .into_iter()
        .map(|parameter| parse_procedure_parameter_declaration(&parameter))
        .collect()

}

fn parse_create_function_parameter_names(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    let parameter_segments = parse_create_routine_parameter_segments(statement)?;

    parameter_segments
        .into_iter()
        .map(|parameter| parse_routine_parameter_name_without_mode(&parameter))
        .collect()

}

fn parse_create_routine_parameter_segments(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create procedure") && !lowered.starts_with("create function") {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE ROUTINE".to_string(),
        ));
    }

    let header_end = find_keyword_boundary_index(&lowered, "begin")
        .or_else(|| find_keyword_boundary_index(&lowered, "returns"))
        .ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CREATE ROUTINE header is missing BEGIN or RETURNS".to_string(),
        )
    })?;

    let header = &trimmed[..header_end];
    let Some(open_index) = header.find('(') else {
        return Ok(Vec::new());
    };

    let close_index = matching_close_parenthesis(header, open_index).ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE parameter list is malformed".to_string(),
        )
    })?;

    let raw_params = &header[(open_index + 1)..close_index];
    Ok(split_top_level_csv_trimmed(raw_params))

}

pub fn bind_call_procedure_argument_bindings(
    create_procedure_sql: &str,
    call_statement: &Statement,
) -> Result<Vec<RoutineArgumentBinding>, SqlParseError> {

    let parameter_declarations =
        parse_create_procedure_parameter_declarations_from_statement(create_procedure_sql)?;

    let call_arguments = parse_call_argument_bindings(call_statement).map_err(|message| {
        SqlParseError::UnsupportedStatement(format!("CALL argument parse failed: {message}"))
    })?;

    if parameter_declarations.len() != call_arguments.len() {
        return Err(SqlParseError::UnsupportedStatement(format!(
            "CALL argument mismatch: expected {} values but received {}",
            parameter_declarations.len(),
            call_arguments.len(),
        )));
    }

    let mut bindings = Vec::with_capacity(parameter_declarations.len());

    for (parameter, call_argument) in parameter_declarations.into_iter().zip(call_arguments) {
        
        let output_target = match parameter.mode {

            RoutineParameterMode::In => None,

            RoutineParameterMode::Out | 
            RoutineParameterMode::InOut => {
                let Some(target) = call_argument.output_target.clone() else {
                    return Err(SqlParseError::UnsupportedStatement(format!(
                        "CALL {} argument '{}' must be an identifier target",
                        parameter.mode,
                        parameter.name,
                    )));
                };
                Some(target)
            },

        };

        let value = match parameter.mode {

            RoutineParameterMode::Out => b"NULL".to_vec(),

            RoutineParameterMode::In | RoutineParameterMode::InOut => call_argument.value,

        };

        bindings.push(RoutineArgumentBinding {
            name: parameter.name,
            mode: parameter.mode,
            value,
            output_target,
        });
    }

    Ok(bindings)

}

pub fn bind_call_procedure_arguments(
    create_procedure_sql: &str,
    call_statement: &Statement,
) -> Result<Vec<(String, Vec<u8>)>, SqlParseError> {

    Ok(bind_call_procedure_argument_bindings(create_procedure_sql, call_statement)?
        .into_iter()
        .map(|binding| (binding.name, binding.value))
        .collect())

}

pub fn parse_create_procedure_action_statements(
    statement: &str,
) -> Result<Vec<String>, SqlParseError> {

    let body = extract_create_procedure_body(statement)?;

    Ok(split_top_level_statement_sql(body)
        .into_iter()
        .map(|statement_sql| statement_sql.trim().trim_end_matches(';').trim().to_string())
        .filter(|statement_sql| !statement_sql.is_empty())
        .collect::<Vec<_>>())

}

fn extract_create_procedure_body(statement: &str) -> Result<&str, SqlParseError> {

    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    if !lowered.starts_with("create procedure") {
        
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE PROCEDURE".to_string(),
        ));

    }

    let begin_index = find_keyword_boundary_index(&lowered, "begin").ok_or_else(|| {

        SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE block is missing BEGIN".to_string(),
        )

    })?;

    let after_begin = begin_index + "begin".len();
    let body_and_end = trimmed[after_begin..].trim_start();
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

fn find_keyword_boundary_index(haystack: &str, keyword: &str) -> Option<usize> {

    let bytes = haystack.as_bytes();
    let mut from = 0usize;

    while let Some(found) = haystack[from..].find(keyword) {

        let idx = from + found;
        let before_ok = idx == 0 || bytes[idx - 1].is_ascii_whitespace();
        let after_idx = idx + keyword.len();
        let after_ok = after_idx >= bytes.len() || bytes[after_idx].is_ascii_whitespace();

        if before_ok && after_ok {
            return Some(idx);
        }

        from = after_idx;

    }

    None

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

fn parse_call_argument_bindings(statement: &Statement) -> Result<Vec<ParsedCallArgument>, String> {

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

    call_args.iter().map(call_argument_to_binding).collect()

}

fn call_argument_to_binding(argument: &FunctionArg) -> Result<ParsedCallArgument, String> {

    let expression = match argument {

        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => expr,
        
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expr),
            ..
        } => expr,
        
        _ => return Err("unsupported CALL argument".to_string()),

    };

    Ok(ParsedCallArgument {
        value: expression_to_bytes(expression)?,
        output_target: expression_output_target(expression),
    })

}

fn expression_output_target(expression: &Expr) -> Option<String> {

    match expression {
        Expr::Identifier(ident) => Some(common::normalize_identifier!(&ident.value)),

        Expr::CompoundIdentifier(identifiers) => Some(identifiers
            .iter()
            .map(|ident| common::normalize_identifier!(&ident.value))
            .collect::<Vec<_>>()
            .join(".")),

        Expr::Nested(inner) => expression_output_target(inner),

        _ => None,
    }

}

fn expression_to_bytes(expression: &Expr) -> Result<Vec<u8>, String> {

    match expression {

        Expr::Value(value) => value_to_bytes(value),

        Expr::UnaryOp { op, expr } => match (op, expr.as_ref()) {

            (sqlparser::ast::UnaryOperator::Plus, Expr::Value(Value::Number(value, _))) => {
                Ok(value.clone().into_bytes())
            },

            (sqlparser::ast::UnaryOperator::Minus, Expr::Value(Value::Number(value, _))) => {
                Ok(format!("-{value}").into_bytes())
            },

            _ => Err("unsupported CALL unary argument".to_string()),

        },

        Expr::Identifier(ident) => Ok(common::normalize_identifier!(&ident.value).into_bytes()),

        Expr::CompoundIdentifier(identifiers) => Ok(identifiers
            .iter()
            .map(|ident| common::normalize_identifier!(&ident.value))
            .collect::<Vec<_>>()
            .join(".")
            .into_bytes()),

        Expr::Nested(inner) => expression_to_bytes(inner),

        _ => {
            evaluate_expression_sql_to_bytes(
                &expression.to_string(),
                &mut |_| None,
                &mut |nested, _| evaluate_sql_function(nested).map_err(|err| err.to_string()),
            )
            .or_else(|_| Ok(expression.to_string().into_bytes()))
        },

    }

}

fn value_to_bytes(value: &Value) -> Result<Vec<u8>, String> {

    match value {

        Value::Boolean(v) => Ok(v.to_string().into_bytes()),

        Value::Number(v, _) => Ok(v.to_string().into_bytes()),

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
        Value::HexStringLiteral(v) => Ok(v.as_bytes().to_vec()),

        Value::DollarQuotedString(v) => Ok(v.value.as_bytes().to_vec()),

        Value::Null => Ok(b"NULL".to_vec()),

        Value::Placeholder(v) => Ok(v.as_bytes().to_vec()),

    }

}

fn split_top_level_statement_sql(text: &str) -> Vec<String> {

    let mut statements = Vec::new();
    let mut current = String::new();
    let mut depth = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escaped = false;

    for ch in text.chars() {

        if in_single_quote {
            current.push(ch);

            if escaped {
                escaped = false;
                continue;
            }

            if ch == '\\' {
                escaped = true;
                continue;
            }

            if ch == '\'' {
                in_single_quote = false;
            }

            continue;
        }

        if in_double_quote {
            current.push(ch);

            if escaped {
                escaped = false;
                continue;
            }

            if ch == '\\' {
                escaped = true;
                continue;
            }

            if ch == '"' {
                in_double_quote = false;
            }

            continue;
        }

        match ch {

            '\'' => {
                in_single_quote = true;
                current.push(ch);
            },

            '"' => {
                in_double_quote = true;
                current.push(ch);
            },

            '(' => {
                depth += 1;
                current.push(ch);
            },

            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            },

            ';' if depth == 0 => {
                let statement = current.trim();
                if !statement.is_empty() {
                    statements.push(statement.to_string());
                }
                current.clear();
            },

            _ => current.push(ch),

        }

    }

    let statement = current.trim();
    if !statement.is_empty() {
        statements.push(statement.to_string());
    }

    statements

}

fn parse_procedure_parameter_declaration(
    parameter: &str,
) -> Result<RoutineParameterDeclaration, SqlParseError> {

    let mut tokens = parameter.split_whitespace();

    let Some(first) = tokens.next() else {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE PROCEDURE parameter definition is empty".to_string(),
        ));
    };

    let (mode, candidate) = if first.eq_ignore_ascii_case("in") {
        let name = tokens.next().ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "CREATE PROCEDURE parameter name is missing".to_string(),
            )
        })?;
        (RoutineParameterMode::In, name)
    } else if first.eq_ignore_ascii_case("out") {
        let name = tokens.next().ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "CREATE PROCEDURE parameter name is missing".to_string(),
            )
        })?;
        (RoutineParameterMode::Out, name)
    } else if first.eq_ignore_ascii_case("inout") {
        let name = tokens.next().ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "CREATE PROCEDURE parameter name is missing".to_string(),
            )
        })?;
        (RoutineParameterMode::InOut, name)
    } else {
        (RoutineParameterMode::In, first)
    };

    Ok(RoutineParameterDeclaration {
        name: common::normalize_identifier!(candidate.trim_matches('`').trim_matches('"')),
        mode,
    })

}

fn parse_routine_parameter_name_without_mode(parameter: &str) -> Result<String, SqlParseError> {

    let mut tokens = parameter.split_whitespace();

    let Some(first) = tokens.next() else {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE FUNCTION parameter definition is empty".to_string(),
        ));
    };

    Ok(common::normalize_identifier!(first.trim_matches('`').trim_matches('"')))

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NextClause {
    ElseIf,
    ElseIfSpaced,
    Else,
    EndIf,
}

#[derive(Debug, Clone)]
struct ParsedCallArgument {
    value: Vec<u8>,
    output_target: Option<String>,
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
                Some((current, _, _)) if current <= index => {},
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

fn parse_case_plan_from_statement(statement: &str) -> Result<IfElseEndPlan, SqlParseError> {

    let normalized = statement.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    if !lowered.starts_with("case") {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE routine block must start with CASE".to_string(),
        ));
    }

    let after_case = normalized["case".len()..].trim_start();
    let lowered_after_case = after_case.to_ascii_lowercase();
    let when_index = find_keyword_boundary_index(&lowered_after_case, "when").ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CASE routine block must contain at least one WHEN branch".to_string(),
        )
    })?;

    let prefix = after_case[..when_index].trim();
    let case_operand = if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_string())
    };

    let mut remaining = after_case[when_index..].trim_start().to_string();
    let mut branches = Vec::new();
    let mut else_action_sql = None;

    loop {

        let lowered_remaining = remaining.to_ascii_lowercase();

        if lowered_remaining.starts_with("when") {
            let parsed = parse_case_when_clause(&remaining, case_operand.as_deref())?;
            branches.push(parsed.branch);
            remaining = parsed.remaining;
            continue;
        }

        if lowered_remaining.starts_with("else") {
            let parsed_else = parse_case_else_clause(&remaining)?;
            else_action_sql = Some(parsed_else);
            break;
        }

        if lowered_remaining.starts_with("end") {
            ensure_case_end_clause(&remaining)?;
            break;
        }

        return Err(SqlParseError::UnsupportedStatement(
            "CASE routine block must continue with WHEN, ELSE, or END CASE".to_string(),
        ));

    }

    if branches.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE routine block must include at least one WHEN branch".to_string(),
        ));
    }

    Ok(IfElseEndPlan {
        branches,
        else_action_sql,
    })

}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CaseWhenParseResult {
    branch: IfElseEndBranchPlan,
    remaining: String,
}

fn parse_case_when_clause(
    remaining: &str,
    case_operand: Option<&str>,
) -> Result<CaseWhenParseResult, SqlParseError> {

    let lowered_remaining = remaining.to_ascii_lowercase();
    if !lowered_remaining.starts_with("when") {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE branch must start with WHEN".to_string(),
        ));
    }

    let after_when = remaining["when".len()..].trim_start();
    let lowered_after_when = after_when.to_ascii_lowercase();
    let then_index = find_keyword_boundary_index(&lowered_after_when, "then").ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CASE branch is missing THEN".to_string(),
        )
    })?;

    let when_fragment = after_when[..then_index].trim();
    if when_fragment.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE WHEN fragment is empty".to_string(),
        ));
    }

    let condition_sql = if let Some(operand) = case_operand {
        format!("{operand} = {when_fragment}")
    } else {
        when_fragment.to_string()
    };

    let condition = parse_if_branch_condition(&condition_sql)?;

    let after_then = after_when[(then_index + "then".len())..].trim_start();
    let split = split_case_action_and_next_clause(after_then)?;

    let action_sql = split.action_sql.trim().trim_end_matches(';').trim().to_string();
    if action_sql.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE branch action is empty".to_string(),
        ));
    }

    let remaining_tail = after_then[split.next_clause_index..].trim_start().to_string();

    Ok(CaseWhenParseResult {
        branch: IfElseEndBranchPlan {
            condition,
            action_sql,
        },
        remaining: remaining_tail,
    })

}

fn parse_case_else_clause(remaining: &str) -> Result<String, SqlParseError> {

    let lowered_remaining = remaining.to_ascii_lowercase();
    if !lowered_remaining.starts_with("else") {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE ELSE clause must start with ELSE".to_string(),
        ));
    }

    let after_else = remaining["else".len()..].trim_start();
    let lowered_after_else = after_else.to_ascii_lowercase();
    let end_index = find_keyword_boundary_index(&lowered_after_else, "end").ok_or_else(|| {
        SqlParseError::UnsupportedStatement(
            "CASE ELSE clause is missing END CASE".to_string(),
        )
    })?;

    ensure_case_end_clause(&after_else[end_index..])?;

    let else_action_sql = after_else[..end_index]
        .trim()
        .trim_end_matches(';')
        .trim()
        .to_string();

    if else_action_sql.is_empty() {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE ELSE action is empty".to_string(),
        ));
    }

    Ok(else_action_sql)

}

fn ensure_case_end_clause(fragment: &str) -> Result<(), SqlParseError> {

    let lowered = fragment.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    if lowered.starts_with("end case") {
        Ok(())
    } else {
        Err(SqlParseError::UnsupportedStatement(
            "CASE routine block must terminate with END CASE".to_string(),
        ))
    }
    
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CaseClauseSplit {
    action_sql: String,
    next_clause_index: usize,
}

fn split_case_action_and_next_clause(branch_tail: &str) -> Result<CaseClauseSplit, SqlParseError> {

    let lowered = branch_tail.to_ascii_lowercase();
    let mut earliest: Option<usize> = None;

    for keyword in ["when", "else", "end"] {
        if let Some(index) = find_keyword_boundary_index(&lowered, keyword) {
            match earliest {
                Some(current) if current <= index => {}
                _ => earliest = Some(index),
            }
        }
    }

    let Some(index) = earliest else {
        return Err(SqlParseError::UnsupportedStatement(
            "CASE branch must end with WHEN, ELSE, or END CASE".to_string(),
        ));
    };

    let action_sql = branch_tail[..index].trim().to_string();

    Ok(CaseClauseSplit {
        action_sql,
        next_clause_index: index,
    })

}

#[cfg(test)]
#[path = "routine_plan_test.rs"]
mod tests;
