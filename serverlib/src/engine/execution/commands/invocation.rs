use crate::{
    DatabaseCatalog, DatabaseStoredProcedure, DatabaseTrigger, TriggerEventKind, TriggerTiming,
};

use super::super::ConditionValueProvider;
use super::control_flow::{
    execute_if_else_end_from_create_procedure_sql, execute_if_else_end_plan,
    execute_sql_cursor, CursorDirective, SqlCursorFrame, SqlCursorSource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityInvocationSource {
    DirectedUser,
    AutomaticEvent,
}

pub fn execute_stored_procedure_invocation<R, E>(
    provider: &dyn ConditionValueProvider,
    procedure: &DatabaseStoredProcedure,
    _source: EntityInvocationSource,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
{

    if let Some(plan) = procedure.if_else_end_plan() {
        return execute_if_else_end_plan(provider, plan, execute_action);
    }

    execute_if_else_end_from_create_procedure_sql(provider, &procedure.sql, execute_action)

}

pub fn execute_stored_procedure_invocation_over_cursor<S, R, E>(
    cursor_source: &mut S,
    cursor_frame: &mut SqlCursorFrame,
    procedure: &DatabaseStoredProcedure,
    source: EntityInvocationSource,
    execute_action: &mut E,
) -> Result<Vec<R>, String>
where
    S: SqlCursorSource,
    E: FnMut(&str, &SqlCursorFrame) -> Result<R, String>,
{

    let mut outcomes = Vec::new();

    execute_sql_cursor(cursor_source, cursor_frame, &mut |frame| {
        if let Some(outcome) = execute_stored_procedure_invocation(
            frame,
            procedure,
            source,
            &mut |sql| execute_action(sql, frame),
        )? {
            outcomes.push(outcome);
        }

        Ok(CursorDirective::<()>::Next)
    })?;

    Ok(outcomes)

}

pub fn execute_trigger_invocation<R, E>(
    trigger: &DatabaseTrigger,
    _source: EntityInvocationSource,
    execute_action: &mut E,
) -> Result<R, String>
where
    E: FnMut(&str) -> Result<R, String>,
{
    execute_action(&trigger.sql)
}

pub fn execute_automatic_triggers_for_event<R, E>(
    catalog: &DatabaseCatalog,
    table_id: &str,
    timing: TriggerTiming,
    event: TriggerEventKind,
    execute_action: &mut E,
) -> Result<Vec<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
{

    let mut outcomes = Vec::new();

    for trigger in catalog.triggers_for_event(table_id, timing, event) {
        outcomes.push(execute_trigger_invocation(
            trigger,
            EntityInvocationSource::AutomaticEvent,
            execute_action,
        )?);
    }

    Ok(outcomes)
    
}

#[cfg(test)]
#[path = "invocation_test.rs"]
mod tests;
