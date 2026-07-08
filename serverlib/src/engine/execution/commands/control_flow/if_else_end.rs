use std::collections::HashSet;

use crate::SelectCondition;
use crate::engine::sql::{
    parse_if_else_end_plan_from_create_procedure_statement, IfElseEndPlan,
};

use super::super::super::{
    row_matches_condition_with_result, ConditionValueProvider,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlFlowBranch<T> {
    pub condition: SelectCondition,
    pub action: T,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IfElseEndBlock<T> {
    pub branches: Vec<ControlFlowBranch<T>>,
    pub else_branch: Option<T>,
}

pub fn condition_matches_provider(
    provider: &dyn ConditionValueProvider,
    condition: &SelectCondition,
) -> Result<bool, String> {

    row_matches_condition_with_result(
        provider,
        Some(condition),
        &mut |_, _| Ok(HashSet::new()),
        &mut |_, _| Ok(false),
        &mut |_, _| Ok(None),
    )

}

pub fn execute_if_else_end_block<T, R, P, E>(
    provider: &dyn ConditionValueProvider,
    block: &IfElseEndBlock<T>,
    predicate_matches: &mut P,
    execute_branch: &mut E,
) -> Result<Option<R>, String>
where
    P: FnMut(&dyn ConditionValueProvider, &SelectCondition) -> Result<bool, String>,
    E: FnMut(&T) -> Result<R, String>,
{

    for branch in &block.branches {
        if predicate_matches(provider, &branch.condition)? {
            return Ok(Some(execute_branch(&branch.action)?));
        }
    }

    if let Some(else_action) = block.else_branch.as_ref() {
        return Ok(Some(execute_branch(else_action)?));
    }

    Ok(None)
    
}

pub fn execute_if_else_end_plan<'a, R, E>(
    provider: &dyn ConditionValueProvider,
    plan: &'a IfElseEndPlan,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&'a str) -> Result<R, String>,
{

    let block = IfElseEndBlock {
        branches: plan
            .branches
            .iter()
            .map(|branch| ControlFlowBranch {
                condition: branch.condition.clone(),
                action: branch.action_sql.as_str(),
            })
            .collect::<Vec<_>>(),
        else_branch: plan.else_action_sql.as_deref(),
    };

    execute_if_else_end_block(
        provider,
        &block,
        &mut |candidate, condition| condition_matches_provider(candidate, condition),
        &mut |action_sql| execute_action(action_sql),
    )

}

pub fn execute_if_else_end_from_create_procedure_sql<R, E>(
    provider: &dyn ConditionValueProvider,
    procedure_sql: &str,
    execute_action: &mut E,
) -> Result<Option<R>, String>
where
    E: FnMut(&str) -> Result<R, String>,
{
    
    let Some(plan) = parse_if_else_end_plan_from_create_procedure_statement(procedure_sql)
        .map_err(|err| format!("IF/ELSE/END routine parse failed: {err}"))?
    else {
        return Ok(None);
    };

    execute_if_else_end_plan(provider, &plan, execute_action)
    
}

#[cfg(test)]
#[path = "if_else_end_test.rs"]
mod tests;
