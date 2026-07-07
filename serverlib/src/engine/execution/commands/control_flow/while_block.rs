use super::LoopControlDirective;

pub fn execute_local_while_block<T, FCond, FExec>(
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
    
    let (condition_sql, body_sql) = parse_local_while_block(action_sql)?;

    for _ in 0..max_iterations {
        if !evaluate_condition(context, condition_sql.as_str())? {
            return Ok(());
        }

        match execute_body(context, body_sql.as_str())? {
            LoopControlDirective::None => {}
            LoopControlDirective::Iterate => continue,
            LoopControlDirective::Leave => return Ok(()),
        }
    }

    Err("call action while execution failed: exceeded max iteration limit".to_string())

}

fn parse_local_while_block(action_sql: &str) -> Result<(String, String), String> {

    let normalized = action_sql.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    if !lowered.starts_with("while ") {
        return Err("while parse failed: statement must start with WHILE".to_string());
    }

    let do_index = find_keyword_boundary_index_in_text(&lowered, "do")
        .ok_or_else(|| "while parse failed: DO is missing".to_string())?;
    let end_while_index = lowered
        .rfind("end while")
        .ok_or_else(|| "while parse failed: END WHILE is missing".to_string())?;

    if end_while_index <= do_index {
        return Err("while parse failed: block layout is invalid".to_string());
    }

    let condition_sql = normalized["while".len()..do_index].trim().to_string();
    let body_sql = normalized[(do_index + "do".len())..end_while_index]
        .trim()
        .to_string();

    if condition_sql.is_empty() {
        return Err("while parse failed: condition is empty".to_string());
    }

    Ok((condition_sql, body_sql))

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
