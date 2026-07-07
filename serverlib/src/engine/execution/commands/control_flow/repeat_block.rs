use super::LoopControlDirective;

pub fn execute_local_repeat_block<T, FCond, FExec>(
    action_sql: &str,
    max_iterations: usize,
    context: &mut T,
    evaluate_condition: &mut FCond,
    execute_body: &mut FExec,
) -> Result<(), String>
where
    FCond: FnMut(&mut T, &str) -> Result<bool, String>,
    FExec: FnMut(&mut T, &str) -> Result<LoopControlDirective, String>,
{

    let (body_sql, until_condition_sql) = parse_local_repeat_block(action_sql)?;

    for _ in 0..max_iterations {
        match execute_body(context, body_sql.as_str())? {
            LoopControlDirective::None => {}
            LoopControlDirective::Iterate => continue,
            LoopControlDirective::Leave => return Ok(()),
        }

        if evaluate_condition(context, until_condition_sql.as_str())? {
            return Ok(());
        }
    }

    Err("call action repeat execution failed: exceeded max iteration limit".to_string())

}

fn parse_local_repeat_block(action_sql: &str) -> Result<(String, String), String> {

    let normalized = action_sql.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    if !lowered.starts_with("repeat ") {
        return Err("repeat parse failed: statement must start with REPEAT".to_string());
    }

    let until_index = find_keyword_boundary_index_in_text(&lowered, "until")
        .ok_or_else(|| "repeat parse failed: UNTIL is missing".to_string())?;
    let end_repeat_index = lowered
        .rfind("end repeat")
        .ok_or_else(|| "repeat parse failed: END REPEAT is missing".to_string())?;

    if end_repeat_index <= until_index {
        return Err("repeat parse failed: block layout is invalid".to_string());
    }

    let body_sql = normalized["repeat".len()..until_index].trim().to_string();
    let until_condition_sql = normalized[(until_index + "until".len())..end_repeat_index]
        .trim()
        .to_string();

    if until_condition_sql.is_empty() {
        return Err("repeat parse failed: UNTIL condition is empty".to_string());
    }

    Ok((body_sql, until_condition_sql))

}

fn find_keyword_boundary_index_in_text(haystack: &str, keyword: &str) -> Option<usize> {
    
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
