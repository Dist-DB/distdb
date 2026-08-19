use crate::{
    ConcurrentWalManager, DatabaseCatalog, DatabaseError, DatabaseStoredProcedure,
    DatabaseTrigger, RuntimeIndexStore, TriggerEventKind, TriggerTiming,
};
use crate::engine::execution::access::clear_cached_table_state;
use crate::engine::sql::{
    evaluate_expression_sql_to_bytes, parse_create_procedure_action_statements,
    parse_if_else_end_plan_from_create_procedure_statement,
};

use super::scoped_table::ScopedEphemeralTableScope;
use super::super::ConditionValueProvider;
use super::control_flow::{
    condition_matches_provider, execute_if_else_end_block,
    execute_sql_cursor, CursorDirective, SqlCursorFrame, SqlCursorSource,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityInvocationSource {
    DirectedUser,
    AutomaticEvent,
}

fn execute_if_else_branch_block<R, E, P>(
    provider: &dyn ConditionValueProvider,
    plan: &crate::IfElseEndPlan,
    predicate_matches: &mut P,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
    P: FnMut(&dyn ConditionValueProvider, &crate::SelectCondition) -> Result<bool, String>,
{

    let block = super::control_flow::IfElseEndBlock {
        branches: plan
            .branches
            .iter()
            .map(|branch| super::control_flow::ControlFlowBranch {
                condition: branch.condition.clone(),
                action: branch.action_sql.as_str(),
            })
            .collect::<Vec<_>>(),
        else_branch: plan.else_action_sql.as_deref(),
    };

    execute_if_else_end_block(
        provider,
        &block,
        &mut |candidate, condition| predicate_matches(candidate, condition),
        &mut |action_sql| execute_action(action_sql),
    )

}

fn execute_action_sequence<'a, R, E>(
    action_statements: impl Iterator<Item = &'a str>,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
{

    let mut last_result = None;

    for action_sql in action_statements {
        last_result = Some(execute_action(action_sql)?);
    }

    Ok(last_result)

}

fn execute_stored_procedure_invocation_with_predicate_matcher<R, E, P>(
    provider: &dyn ConditionValueProvider,
    procedure: &DatabaseStoredProcedure,
    predicate_matches: &mut P,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
    P: FnMut(&dyn ConditionValueProvider, &crate::SelectCondition) -> Result<bool, String>,
{

    let compiled_artifact = procedure.compiled_artifact_for_invocation();
    let ir = &compiled_artifact.ir;

    {
        if let Some(plan) = ir.if_else_end_plan() {
            return execute_if_else_branch_block(provider, plan, predicate_matches, execute_action);
        }

        if let Some(action_statements) = ir.action_statements() {
            return execute_action_sequence(
                action_statements.iter().map(|statement| statement.as_str()),
                execute_action,
            );
        }
    }

    if let Some(plan) = parse_if_else_end_plan_from_create_procedure_statement(&procedure.sql)
        .map_err(|err| format!("IF/ELSE/END routine parse failed: {err}"))?
    {
        return execute_if_else_branch_block(provider, &plan, predicate_matches, execute_action);
    }

    let action_statements = parse_create_procedure_action_statements(&procedure.sql)
        .map_err(|err| format!("stored procedure action parse failed: {err}"))?;

    execute_action_sequence(
        action_statements.iter().map(|statement| statement.as_str()),
        execute_action,
    )

}

fn combine_invocation_and_cleanup<T>(
    invocation_result: Result<T, String>,
    cleanup_result: Result<(), String>,
    cleanup_failure_prefix: Option<&str>,
) -> Result<T, String> {

    match (invocation_result, cleanup_result) {

        (Ok(result), Ok(())) => Ok(result),

        (Err(err), Ok(())) => Err(err),

        (Ok(_), Err(cleanup_err)) => {
            if let Some(prefix) = cleanup_failure_prefix {
                Err(format!("{prefix}: {cleanup_err}"))
            } else {
                Err(cleanup_err)
            }
        },

        (Err(err), Err(cleanup_err)) => {
            if let Some(prefix) = cleanup_failure_prefix {
                Err(format!("{err}; {prefix}: {cleanup_err}"))
            } else {
                Err(format!("{err}; cleanup failed: {cleanup_err}"))
            }
        },

    }

}

fn execute_with_cleanup<T, F, C>(execute: F, cleanup: &mut C) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
    C: FnMut() -> Result<(), String>,
{
    let invocation_result = execute();
    let cleanup_result = cleanup();
    combine_invocation_and_cleanup(invocation_result, cleanup_result, None)
}

fn with_scoped_ephemeral_teardown<T, F>(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
    procedure: &DatabaseStoredProcedure,
    session_id: &str,
    execute: F,
) -> Result<T, String>
where
    F: FnOnce(
        &mut ScopedEphemeralTableScope,
        &mut DatabaseCatalog,
        &ConcurrentWalManager,
    ) -> Result<T, String>,
{

    let mut scope = ScopedEphemeralTableScope::new(format!(
        "proc_{}_{}",
        common::normalize_identifier!(session_id),
        procedure.procedure_id,
    ));

    let invocation_result = execute(&mut scope, catalog, wal);
    let cleanup_result = scope.cleanup(catalog, wal);

    combine_invocation_and_cleanup(
        invocation_result,
        cleanup_result,
        Some("temporary table scoped cleanup failed"),
    )

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

    execute_stored_procedure_invocation_with_predicate_matcher(
        provider,
        procedure,
        &mut |candidate, condition| condition_matches_provider(candidate, condition),
        execute_action,
    )

}

fn condition_matches_provider_with_sql_lookup(
    provider: &dyn ConditionValueProvider,
    condition: &crate::SelectCondition,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
) -> Result<bool, String> {

    crate::engine::execution::row_matches_condition_with_result_and_expression(
        provider,
        Some(condition),
        &mut |_, _| Ok(std::collections::HashSet::new()),
        &mut |_, _| Ok(false),
        &mut |_, _| Ok(None),
        &mut |candidate, expression_sql| {
            evaluate_expression_sql_to_bytes(
                expression_sql,
                &mut |field_name| candidate.value(field_name).cloned(),
                &mut |function, lookup| {
                    crate::engine::execution::execute_sql_function_with_lookup(
                        catalog,
                        wal,
                        runtime_indexes,
                        function,
                        lookup,
                    )
                },
            )
            .map(Some)
        },
    )

}

pub fn execute_stored_procedure_invocation_with_sql_lookup_context<R, E>(
    provider: &dyn ConditionValueProvider,
    procedure: &DatabaseStoredProcedure,
    _source: EntityInvocationSource,
    catalog: &DatabaseCatalog,
    wal: &ConcurrentWalManager,
    runtime_indexes: &RuntimeIndexStore,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
{

    execute_stored_procedure_invocation_with_predicate_matcher(
        provider,
        procedure,
        &mut |candidate, condition| {
            condition_matches_provider_with_sql_lookup(
                candidate,
                condition,
                catalog,
                wal,
                runtime_indexes,
            )
        },
        execute_action,
    )

}

pub fn cleanup_temporary_tables(
    catalog: &mut DatabaseCatalog,
    wal: &ConcurrentWalManager,
) -> Result<(), String> {

    let temporary_tables = catalog
        .table_ids()
        .into_iter()
        .filter(|table_id| {
            catalog
                .table_handle(table_id)
                .and_then(|handle| handle.table_snapshot())
                .is_some_and(|table| table.is_temporary())
        })
        .collect::<Vec<_>>();

    for table_id in temporary_tables {
        
        let stream_id = catalog.entity_wal_stream_id(&table_id);
        
        match catalog.drop_table(&table_id) {
            
            Ok(()) | Err(DatabaseError::TableNotFound) => {},
            
            Err(err) => {
                return Err(format!("temporary table cleanup failed: {err}"));
            }

        }

        let stream_id = stream_id.unwrap_or_else(|| table_id.clone());

        clear_cached_table_state(wal.cache_scope_id(), &table_id, &stream_id);

        if wal.stream_mode(&stream_id) == crate::WalStreamMode::Ephemeral {
            wal.clear_stream_records(&stream_id)
                .map_err(|err| format!("temporary table cleanup failed: {err}"))?;
        }

        wal.delete_stream(&stream_id)
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

    execute_with_cleanup(
        || execute_stored_procedure_invocation(provider, procedure, source, execute_action),
        cleanup,
    )

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

    with_scoped_ephemeral_teardown(catalog, wal, procedure, session_id, |scope, catalog, wal| {
        execute_stored_procedure_invocation(
            provider,
            procedure,
            source,
            &mut |sql| execute_action(sql, scope, catalog, wal),
        )
    })

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

    execute_with_cleanup(
        || {
            execute_stored_procedure_invocation_over_cursor(
                cursor_source,
                cursor_frame,
                procedure,
                source,
                execute_action,
            )
        },
        cleanup,
    )

}

#[expect(clippy::too_many_arguments, reason = "scoped teardown requires explicit runtime dependencies")]
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

    with_scoped_ephemeral_teardown(catalog, wal, procedure, session_id, |scope, catalog, wal| {
        execute_stored_procedure_invocation_over_cursor(
            cursor_source,
            cursor_frame,
            procedure,
            source,
            &mut |sql, frame| execute_action(sql, frame, scope, catalog, wal),
        )
    })

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
            &trigger,
            EntityInvocationSource::AutomaticEvent,
            execute_action,
        )?);
    }

    Ok(outcomes)
    
}

#[cfg(test)]
#[path = "invocation_test.rs"]
mod tests;
