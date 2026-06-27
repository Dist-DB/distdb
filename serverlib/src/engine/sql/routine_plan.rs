use super::{
    parse_select_read_plan_from_statement, IfElseEndBranchPlan, IfElseEndPlan, SqlParseError,
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
            }

            NextClause::ElseIfSpaced => {
                remaining =
                    branch_tail[split.next_clause_index + "else if ".len()..].trim_start();
            }

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
            }

            NextClause::EndIf => {
                return Ok(IfElseEndPlan {
                    branches,
                    else_action_sql: None,
                });
            }
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
