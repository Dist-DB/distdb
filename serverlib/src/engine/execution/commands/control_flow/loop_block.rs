#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoopControlDirective {
    None,
    Leave(Option<String>),
    Iterate(Option<String>),
}

pub fn execute_local_loop_block<T, FExec>(
    action_sql: &str,
    max_iterations: usize,
    context: &mut T,
    execute_body: &mut FExec,
) -> Result<LoopControlDirective, String>
where
    FExec: FnMut(&mut T, &str) -> Result<LoopControlDirective, String>,
{

    let (loop_label, body_sql) = parse_local_loop_block(action_sql)?;

    for _ in 0..max_iterations {
        
        match execute_body(context, body_sql.as_str())? {

            LoopControlDirective::None => continue,
            
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
            }

        }

    }

    Err("call action loop execution failed: exceeded max iteration limit".to_string())

}

fn parse_local_loop_block(action_sql: &str) -> Result<(Option<String>, String), String> {

    let normalized = action_sql.trim().trim_end_matches(';').trim();
    let lowered = normalized.to_ascii_lowercase();

    let (loop_label, loop_start_index) = find_loop_start_label_and_index(&lowered)
        .ok_or_else(|| "loop parse failed: statement must start with LOOP or <label>: LOOP".to_string())?;

    let end_loop_index = lowered
        .rfind("end loop")
        .ok_or_else(|| "loop parse failed: END LOOP is missing".to_string())?;

    if end_loop_index <= loop_start_index {
        return Err("loop parse failed: block layout is invalid".to_string());
    }

    let body_sql = normalized[(loop_start_index + "loop".len())..end_loop_index]
        .trim()
        .to_string();

    Ok((loop_label, body_sql))

}

fn find_loop_start_label_and_index(lowered: &str) -> Option<(Option<String>, usize)> {

    if lowered.starts_with("loop") {
        return Some((None, 0));
    }

    let colon_index = lowered.find(':')?;
    let label = lowered[..colon_index].trim();
    if label.is_empty() || !label.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return None;
    }

    let rest = lowered[(colon_index + 1)..].trim_start();
    if rest.starts_with("loop") {
        let prefix_len = lowered.len() - rest.len();
        return Some((Some(label.to_string()), prefix_len));
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
