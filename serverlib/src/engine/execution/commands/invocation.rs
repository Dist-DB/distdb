use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseError, DatabaseStoredProcedure,
    DatabaseTrigger, TriggerEventKind, TriggerTiming,
};
use crate::engine::sql::parse_create_procedure_action_statements;

use super::scoped_table::ScopedEphemeralTableScope;
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

    if let Some(ir) = procedure.compiled_ir() {
        if let Some(plan) = ir.if_else_end_plan() {
            return execute_if_else_end_plan(provider, plan, execute_action);
        }
    }

    if let Some(result) =
        execute_if_else_end_from_create_procedure_sql(provider, &procedure.sql, execute_action)?
    {
        return Ok(Some(result));
    }

    let action_statements = parse_create_procedure_action_statements(&procedure.sql)
        .map_err(|err| format!("stored procedure action parse failed: {err}"))?;

    let mut last_result = None;

    for action_sql in action_statements {
        last_result = Some(execute_action(&action_sql)?);
    }

    Ok(last_result)

}

pub fn cleanup_temporary_tables(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
) -> Result<(), String> {

    let temporary_tables = catalog
        .table_ids()
        .into_iter()
        .filter(|table_id| catalog.table(table_id).is_some_and(|table| table.is_temporary()))
        .collect::<Vec<_>>();

    for table_id in temporary_tables {
        
        match catalog.drop_table(&table_id) {
            
            Ok(()) | Err(DatabaseError::TableNotFound) => {},
            
            Err(err) => {
                return Err(format!("temporary table cleanup failed: {err}"));
            }

        }

        wal.delete_stream(&table_id)
            .map_err(|err| format!("temporary table cleanup failed: {err}"))?;
    
    }

    Ok(())

}

pub fn execute_stored_procedure_invocation_with_cleanup<R, E, C>(
    provider: &dyn ConditionValueProvider,
    procedure: &DatabaseStoredProcedure,
    source: EntityInvocationSource,
    execute_action: &mut E,
    cleanup: &mut C,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
    C: FnMut() -> Result<(), String>,
{

    let invocation_result = execute_stored_procedure_invocation(
        provider,
        procedure,
        source,
        execute_action,
    );

    let cleanup_result = cleanup();

    match (invocation_result, cleanup_result) {

        (Ok(result), Ok(())) => Ok(result),

        (Err(err), Ok(())) => Err(err),

        (Ok(_), Err(cleanup_err)) => Err(cleanup_err),

        (Err(err), Err(cleanup_err)) => Err(format!("{err}; cleanup failed: {cleanup_err}")),
        
    }

}

pub fn execute_stored_procedure_invocation_with_scoped_teardown<R, E>(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    provider: &dyn ConditionValueProvider,
    procedure: &DatabaseStoredProcedure,
    source: EntityInvocationSource,
    session_id: &str,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str, &mut ScopedEphemeralTableScope, &mut DatabaseCatalog, &ConcurrentWalManager) -> Result<R, String>,
{

    let mut scope = ScopedEphemeralTableScope::new(format!(
        "proc_{}_{}",
        common::normalize_identifier!(session_id),
        &procedure.procedure_id,
    ));

    let invocation_result = execute_stored_procedure_invocation(
        provider,
        procedure,
        source,
        &mut |sql| execute_action(sql, &mut scope, catalog, wal),
    );

    let cleanup_result = scope.cleanup(catalog, wal);

    match (invocation_result, cleanup_result) {

        (Ok(result), Ok(())) => Ok(result),

        (Err(err), Ok(())) => Err(err),

        (Ok(_), Err(cleanup_err)) => Err(format!("temporary table scoped cleanup failed: {cleanup_err}")),

        (Err(err), Err(cleanup_err)) => Err(format!("{err}; temporary table scoped cleanup failed: {cleanup_err}")),

    }

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

pub fn execute_stored_procedure_invocation_over_cursor_with_cleanup<S, R, E, C>(
    cursor_source: &mut S,
    cursor_frame: &mut SqlCursorFrame,
    procedure: &DatabaseStoredProcedure,
    source: EntityInvocationSource,
    execute_action: &mut E,
    cleanup: &mut C,
) -> Result<Vec<R>, String>
where
    S: SqlCursorSource,
    E: FnMut(&str, &SqlCursorFrame) -> Result<R, String>,
    C: FnMut() -> Result<(), String>,
{

    let result = execute_stored_procedure_invocation_over_cursor(
        cursor_source,
        cursor_frame,
        procedure,
        source,
        execute_action,
    );

    let cleanup_result = cleanup();

    match (result, cleanup_result) {

        (Ok(outcomes), Ok(())) => Ok(outcomes),

        (Err(err), Ok(())) => Err(err),

        (Ok(_), Err(cleanup_err)) => Err(cleanup_err),

        (Err(err), Err(cleanup_err)) => Err(format!("{err}; cleanup failed: {cleanup_err}")),

    }

}

pub fn execute_stored_procedure_invocation_over_cursor_with_scoped_teardown<S, R, E>(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    cursor_source: &mut S,
    cursor_frame: &mut SqlCursorFrame,
    procedure: &DatabaseStoredProcedure,
    source: EntityInvocationSource,
    session_id: &str,
    execute_action: &mut E,
) -> Result<Vec<R>, String>
where
    S: SqlCursorSource,
    E: FnMut(
        &str,
        &SqlCursorFrame,
        &mut ScopedEphemeralTableScope,
        &mut DatabaseCatalog,
        &ConcurrentWalManager,
    ) -> Result<R, String>,
{

    let mut scope = ScopedEphemeralTableScope::new(format!(
        "proc_{}_{}",
        common::normalize_identifier!(session_id),
        &procedure.procedure_id,
    ));

    let result = execute_stored_procedure_invocation_over_cursor(
        cursor_source,
        cursor_frame,
        procedure,
        source,
        &mut |sql, frame| execute_action(sql, frame, &mut scope, catalog, wal),
    );

    let cleanup_result = scope.cleanup(catalog, wal);

    match (result, cleanup_result) {
        
        (Ok(outcomes), Ok(())) => Ok(outcomes),

        (Err(err), Ok(())) => Err(err),

        (Ok(_), Err(cleanup_err)) => Err(format!("temporary table scoped cleanup failed: {cleanup_err}")),

        (Err(err), Err(cleanup_err)) => Err(format!("{err}; temporary table scoped cleanup failed: {cleanup_err}")),

    }

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
