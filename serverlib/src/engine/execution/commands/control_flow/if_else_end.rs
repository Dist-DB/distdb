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
mod tests {

    use std::collections::HashMap;

    use crate::{
        CursorDirective, SqlCursorFrame, VecSqlCursorSource,
        SelectComparisonOp, SelectCondition, SelectPredicate,
    };

    use super::{
        super::cursor::execute_sql_cursor,
        condition_matches_provider, execute_if_else_end_block, execute_if_else_end_plan,
        execute_if_else_end_from_create_procedure_sql,
        ControlFlowBranch, IfElseEndBlock,
    };

    #[test]
    fn execute_if_else_end_block_runs_first_matching_branch() {

        let mut row = HashMap::new();
        row.insert("active".to_string(), b"1".to_vec());

        let block = IfElseEndBlock {
            
            branches: vec![

                ControlFlowBranch {
                    condition: SelectCondition::Predicate(SelectPredicate::Comparison {
                        field_name: "active".to_string(),
                        op: SelectComparisonOp::Eq,
                        value: b"1".to_vec(),
                    }),
                    action: "enabled".to_string(),
                },

                ControlFlowBranch {
                    condition: SelectCondition::Predicate(SelectPredicate::Comparison {
                        field_name: "active".to_string(),
                        op: SelectComparisonOp::Eq,
                        value: b"0".to_vec(),
                    }),
                    action: "disabled".to_string(),
                },

            ],
            
            else_branch: Some("unknown".to_string()),

        };

        let result = execute_if_else_end_block(
            &row,
            &block,
            &mut |provider, condition| condition_matches_provider(provider, condition),
            &mut |action| Ok(action.clone()),
        )
        .expect("if/else/end block should execute");

        assert_eq!(result, Some("enabled".to_string()));

    }

    #[test]
    fn execute_if_else_end_block_uses_else_when_no_branch_matches() {

        let mut row = HashMap::new();
        row.insert("active".to_string(), b"2".to_vec());

        let block = IfElseEndBlock {

            branches: vec![ControlFlowBranch {
                condition: SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: "active".to_string(),
                    op: SelectComparisonOp::Eq,
                    value: b"1".to_vec(),
                }),
                action: "enabled".to_string(),
            }],
            else_branch: Some("fallback".to_string()),
        
        };

        let result = execute_if_else_end_block(
            &row,
            &block,
            &mut |provider, condition| condition_matches_provider(provider, condition),
            &mut |action| Ok(action.clone()),
        )
        .expect("if/else/end block should execute");

        assert_eq!(result, Some("fallback".to_string()));

    }

    #[test]
    fn execute_if_else_end_plan_runs_branch_action_sql() {
        let mut row = HashMap::new();
        row.insert("active".to_string(), b"1".to_vec());

        let plan = crate::IfElseEndPlan {
            branches: vec![crate::IfElseEndBranchPlan {
                condition: SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: "active".to_string(),
                    op: SelectComparisonOp::Eq,
                    value: b"1".to_vec(),
                }),
                action_sql: "select 'enabled'".to_string(),
            }],
            else_action_sql: Some("select 'disabled'".to_string()),
        };

        let result = execute_if_else_end_plan(&row, &plan, &mut |sql| {
            Ok(sql.to_string())
        })
        .expect("if/else/end plan execution should succeed");

        assert_eq!(result, Some("select 'enabled'".to_string()));
    }

    #[test]
    fn execute_if_else_end_from_create_procedure_sql_executes_matching_action() {
        let mut row = HashMap::new();
        row.insert("active".to_string(), b"0".to_vec());

        let procedure_sql =
            "create procedure p_sync() begin if active = 1 then select 'on'; else select 'off'; end if; end";

        let result = execute_if_else_end_from_create_procedure_sql(
            &row,
            procedure_sql,
            &mut |action_sql| Ok(action_sql.to_string()),
        )
        .expect("create procedure IF/ELSE/END should execute");

        assert_eq!(result, Some("select 'off'".to_string()));
    }

    #[test]
    fn execute_if_else_end_from_create_procedure_sql_executes_searched_case_branch() {
        let mut row = HashMap::new();
        row.insert("active".to_string(), b"1".to_vec());

        let procedure_sql =
            "create procedure p_case() begin case when active = 1 then select 'on'; when active = 0 then select 'off'; else select 'unknown'; end case; end";

        let result = execute_if_else_end_from_create_procedure_sql(
            &row,
            procedure_sql,
            &mut |action_sql| Ok(action_sql.to_string()),
        )
        .expect("create procedure CASE should execute");

        assert_eq!(result, Some("select 'on'".to_string()));
    }

    #[test]
    fn execute_if_else_end_from_create_procedure_sql_executes_simple_case_branch() {
        let mut row = HashMap::new();
        row.insert("active".to_string(), b"0".to_vec());

        let procedure_sql =
            "create procedure p_case() begin case active when 1 then select 'on'; when 0 then select 'off'; else select 'unknown'; end case; end";

        let result = execute_if_else_end_from_create_procedure_sql(
            &row,
            procedure_sql,
            &mut |action_sql| Ok(action_sql.to_string()),
        )
        .expect("create procedure simple CASE should execute");

        assert_eq!(result, Some("select 'off'".to_string()));
    }

    #[test]
    fn execute_if_else_end_plan_prefers_local_binding_over_row_value_in_cursor_frame() {
        let plan = crate::IfElseEndPlan {
            branches: vec![crate::IfElseEndBranchPlan {
                condition: SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: "active".to_string(),
                    op: SelectComparisonOp::Eq,
                    value: b"1".to_vec(),
                }),
                action_sql: "select 'on'".to_string(),
            }],
            else_action_sql: Some("select 'off'".to_string()),
        };

        let mut source = VecSqlCursorSource::new(vec![{
            let mut row = HashMap::new();
            row.insert("active".to_string(), b"0".to_vec());
            row
        }]);

        let mut frame = SqlCursorFrame::new();

        let evaluated = execute_sql_cursor(&mut source, &mut frame, &mut |cursor_frame| {
            cursor_frame.set_local_binding("active", b"1".to_vec());
            let result = execute_if_else_end_plan(cursor_frame, &plan, &mut |sql| {
                Ok(sql.to_string())
            })?;
            Ok(CursorDirective::Return(result))
        })
        .expect("cursor execution should succeed")
        .expect("return value should be set");

        assert_eq!(evaluated, Some("select 'on'".to_string()));
    }

    #[test]
    fn execute_case_from_create_procedure_prefers_local_binding_over_row_value_in_cursor_frame() {
        let procedure_sql =
            "create procedure p_case() begin case active when 1 then select 'on'; else select 'off'; end case; end";

        let mut source = VecSqlCursorSource::new(vec![{
            let mut row = HashMap::new();
            row.insert("active".to_string(), b"0".to_vec());
            row
        }]);

        let mut frame = SqlCursorFrame::new();

        let evaluated = execute_sql_cursor(&mut source, &mut frame, &mut |cursor_frame| {
            cursor_frame.set_local_binding("active", b"1".to_vec());
            let result = execute_if_else_end_from_create_procedure_sql(
                cursor_frame,
                procedure_sql,
                &mut |sql| Ok(sql.to_string()),
            )?;

            Ok(CursorDirective::Return(result))
        })
        .expect("cursor execution should succeed")
        .expect("return value should be set");

        assert_eq!(evaluated, Some("select 'on'".to_string()));
    }
}
