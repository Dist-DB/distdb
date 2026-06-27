use super::{
    SqlParseError, TriggerEventKind, TriggerInvocationBinding, TriggerTiming,
};

pub fn parse_trigger_invocation_binding_from_create_trigger_statement(
    statement: &str,
) -> Result<Option<TriggerInvocationBinding>, SqlParseError> {
    let trimmed = statement.trim().trim_end_matches(';').trim();
    let lowered = trimmed.to_ascii_lowercase();

    let trigger_prefix = if lowered.starts_with("create trigger ") {
        "create trigger "
    } else if lowered.starts_with("create or replace trigger ") {
        "create or replace trigger "
    } else {
        return Ok(None);
    };

    let remainder = trimmed[trigger_prefix.len()..].trim_start();
    let lowered_remainder = remainder.to_ascii_lowercase();

    let before_index = lowered_remainder.find(" before ");
    let after_index = lowered_remainder.find(" after ");

    let (timing, timing_index, timing_token_len) = match (before_index, after_index) {
        (Some(before), Some(after)) if before < after => {
            (TriggerTiming::Before, before, " before ".len())
        }
        (Some(_before), Some(after)) => (TriggerTiming::After, after, " after ".len()),
        (Some(before), None) => (TriggerTiming::Before, before, " before ".len()),
        (None, Some(after)) => (TriggerTiming::After, after, " after ".len()),
        (None, None) => {
            return Err(SqlParseError::UnsupportedStatement(
                "CREATE TRIGGER is missing BEFORE/AFTER".to_string(),
            ))
        }
    };

    let tail = remainder[(timing_index + timing_token_len)..].trim_start();
    let lowered_tail = tail.to_ascii_lowercase();

    let (event, event_token_len) = if lowered_tail.starts_with("insert ") {
        (TriggerEventKind::Insert, "insert ".len())
    } else if lowered_tail.starts_with("update ") {
        (TriggerEventKind::Update, "update ".len())
    } else if lowered_tail.starts_with("delete ") {
        (TriggerEventKind::Delete, "delete ".len())
    } else {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE TRIGGER is missing INSERT/UPDATE/DELETE event".to_string(),
        ));
    };

    let after_event = tail[event_token_len..].trim_start();
    let lowered_after_event = after_event.to_ascii_lowercase();

    let after_on = if lowered_after_event.starts_with("on ") {
        after_event["on ".len()..].trim_start()
    } else if let Some(on_index) = lowered_after_event.find(" on ") {
        after_event[(on_index + " on ".len())..].trim_start()
    } else {
        return Err(SqlParseError::UnsupportedStatement(
            "CREATE TRIGGER is missing ON <table> clause".to_string(),
        ));
    };
    let table_id = after_on
        .split_whitespace()
        .next()
        .map(|table| common::normalize_identifier!(table))
        .ok_or_else(|| {
            SqlParseError::UnsupportedStatement(
                "CREATE TRIGGER ON clause is missing table identifier".to_string(),
            )
        })?;

    Ok(Some(TriggerInvocationBinding {
        table_id,
        timing,
        event,
    }))
}

#[cfg(test)]
#[path = "trigger_plan_test.rs"]
mod tests;
