use std::collections::HashSet;

use crate::engine::sql::{
    SelectCaseWhen, SelectExpression, SqlFunctionEvaluationStrategy,
};
use crate::{compare_row_value, render_stored_field_value, SelectComparisonOp};

use super::super::super::{
    row_matches_condition_with_result, ConditionValueProvider,
};

pub fn evaluate_case_projection<E>(
    provider: &dyn ConditionValueProvider,
    operand: Option<&SelectExpression>,
    branches: &[(SelectCaseWhen, SelectExpression)],
    else_value: Option<&SelectExpression>,
    evaluate_function: &mut E,
) -> Result<Option<Vec<u8>>, String>
where
    E: SqlFunctionEvaluationStrategy,
{
    
    let resolved_operand = operand
        .map(|value| resolve_case_value(provider, value, evaluate_function))
        .transpose()?
        .flatten();

    for (branch_when, value) in branches {
        let branch_matches = match branch_when {

            SelectCaseWhen::Condition(condition) => row_matches_condition_with_result(
                provider,
                Some(condition),
                &mut |_, _| Ok(HashSet::new()),
                &mut |_, _| Ok(false),
                &mut |_, _| Ok(None),
            )?,

            SelectCaseWhen::Equals(expected) => {
                let resolved_expected = resolve_case_value(provider, expected, evaluate_function)?;
                matches!((&resolved_operand, resolved_expected), (Some(left), Some(right)) if compare_row_value(left, &right, &SelectComparisonOp::Eq))
            }
            
        };

        if branch_matches {
            #[expect(clippy::needless_question_mark, reason="the question mark is necessary to propagate the error from resolve_case_value")]
            return Ok(resolve_case_value(provider, value, evaluate_function)?);
        }
    }

    else_value
        .map(|value| resolve_case_value(provider, value, evaluate_function))
        .transpose()
        .map(|value| value.flatten())

}

fn resolve_case_value(
    provider: &dyn ConditionValueProvider,
    value: &SelectExpression,
    evaluate_function: &mut impl SqlFunctionEvaluationStrategy,
) -> Result<Option<Vec<u8>>, String> {

    match value {

        SelectExpression::Null => Ok(None),

        SelectExpression::Literal(value) => Ok(Some(value.clone())),

        SelectExpression::Column { field_name } => Ok(provider
            .value(field_name)
            .map(|value| render_stored_field_value(value))),

        SelectExpression::InbuiltFunction { function } => evaluate_function
            .evaluate(
                function,
                &mut |field_name| provider
                    .value(field_name)
                    .map(|value| render_stored_field_value(value)),
            )
            .map_err(|err| format!("case evaluation function failed: {err}")),
    
    }

}

#[cfg(test)]
#[path = "case_when_test.rs"]
mod tests;
