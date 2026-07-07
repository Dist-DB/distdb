use super::LoopControlDirective;

pub fn execute_local_repeat_block<T, FCond, FExec>(
    action_sql: &str,
    max_iterations: usize,
    context: &mut T,
    evaluate_condition: &mut FCond,
    execute_body: &mut FExec,
) -> Result<LoopControlDirective, String>
where
    FCond: FnMut(&mut T, &str) -> Result<bool, String>,
    FExec: FnMut(&mut T, &str) -> Result<LoopControlDirective, String>,
{

    let (loop_label, body_sql, until_condition_sql) = parse_local_repeat_block(action_sql)?;

    for _ in 0..max_iterations {

        match execute_body(context, body_sql.as_str())? {

            LoopControlDirective::None => {},
            
            LoopControlDirective::Iterate(target) => {
                if loop_target_matches_label(target.as_deref(), loop_label.as_deref()) {
                    continue;
                }

                return Ok(LoopControlDirective::Iterate(target));
            },

            LoopControlDirective::Leave(target) => {
                if loop_target_matches_label(target.as_deref(), loop_label.as_deref()) {
                    return Ok(LoopControlDirective::None);
                }

                return Ok(LoopControlDirective::Leave(target));
            },

        }

        if evaluate_condition(context, until_condition_sql.as_str())? {
            return Ok(LoopControlDirective::None);
        }

    }

    Err("call action repeat execution failed: exceeded max iteration limit".to_string())

}

fn parse_local_repeat_block(action_sql: &str) -> Result<(Option<String>, String, String), String> {

    let normalized = action_sql.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    let (loop_label, repeat_start_index) = parse_repeat_start(&lowered)
        .ok_or_else(|| "repeat parse failed: statement must start with REPEAT or <label>: REPEAT".to_string())?;

    let until_index = find_keyword_boundary_index_in_text(&lowered, "until")
        .ok_or_else(|| "repeat parse failed: UNTIL is missing".to_string())?;
    
    let end_repeat_index = lowered
        .rfind("end repeat")
        .ok_or_else(|| "repeat parse failed: END REPEAT is missing".to_string())?;

    if end_repeat_index <= until_index {
        return Err("repeat parse failed: block layout is invalid".to_string());
    }

    let body_sql = normalized[(repeat_start_index + "repeat".len())..until_index].trim().to_string();
    let until_condition_sql = normalized[(until_index + "until".len())..end_repeat_index]
        .trim()
        .to_string();

    if until_condition_sql.is_empty() {
        return Err("repeat parse failed: UNTIL condition is empty".to_string());
    }

    Ok((loop_label, body_sql, until_condition_sql))

}

fn parse_repeat_start(lowered: &str) -> Option<(Option<String>, usize)> {

    if lowered.starts_with("repeat ") {
        return Some((None, 0));
    }

    let colon_index = lowered.find(':')?;
    let label = lowered[..colon_index].trim();
    if label.is_empty() || !label.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }

    let rest = lowered[(colon_index + 1)..].trim_start();
    if rest.starts_with("repeat ") {
        let repeat_start = lowered.len() - rest.len();
        return Some((Some(label.to_string()), repeat_start));
    }

    None

}

fn loop_target_matches_label(target: Option<&str>, current_label: Option<&str>) -> bool {

    match target {
        None => true,
        Some(target_label) => current_label
            .map(|label| label.eq_ignore_ascii_case(target_label))
            .unwrap_or(false),
    }

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
