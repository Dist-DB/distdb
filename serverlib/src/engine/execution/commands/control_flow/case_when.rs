use std::collections::HashSet;

use crate::engine::sql::{
    evaluate_sql_function_with_lookup, SelectCaseWhen, SelectExpression,
};
use crate::{compare_row_value, render_stored_field_value, SelectComparisonOp};

use super::super::super::{
    row_matches_condition_with_result, ConditionValueProvider,
};

pub fn evaluate_case_projection(
    provider: &dyn ConditionValueProvider,
    operand: Option<&SelectExpression>,
    branches: &[(SelectCaseWhen, SelectExpression)],
    else_value: Option<&SelectExpression>,
) -> Result<Option<Vec<u8>>, String> {

    let resolved_operand = operand
        .map(|value| resolve_case_value(provider, value))
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
                let resolved_expected = resolve_case_value(provider, expected)?;
                matches!((&resolved_operand, resolved_expected), (Some(left), Some(right)) if compare_row_value(left, &right, &SelectComparisonOp::Eq))
            }
            
        };

        if branch_matches {
            #[expect(clippy::needless_question_mark, reason="the question mark is necessary to propagate the error from resolve_case_value")]
            return Ok(resolve_case_value(provider, value)?);
        }

    }

    else_value
        .map(|value| resolve_case_value(provider, value))
        .transpose()
        .map(|value| value.flatten())

}

fn resolve_case_value(
    provider: &dyn ConditionValueProvider,
    value: &SelectExpression,
) -> Result<Option<Vec<u8>>, String> {

    match value {
        
        SelectExpression::Null => Ok(None),

        SelectExpression::Literal(value) => Ok(Some(value.clone())),

        SelectExpression::Column { field_name } => Ok(provider
            .value(field_name)
            .map(|value| render_stored_field_value(value))),

        SelectExpression::InbuiltFunction { function } => evaluate_sql_function_with_lookup(
            function,
            &mut |field_name| provider
                .value(field_name)
                .map(|value| render_stored_field_value(value)),
        )
            .map_err(|err| format!("case evaluation function failed: {err}")),

    }

}

#[cfg(test)]
mod tests {
    
    use std::collections::HashMap;

    use sqlparser::{dialect::MySqlDialect, parser::Parser};

    use crate::{SelectComparisonOp, SelectCondition, SelectPredicate};

    use super::evaluate_case_projection;
    use crate::engine::sql::{SelectCaseWhen, SelectExpression};

    fn parse_function_expr(sql: &str) -> sqlparser::ast::Function {

        let statements = Parser::parse_sql(&MySqlDialect {}, sql)
            .expect("sql should parse");

        let statement = statements.first().expect("statement should exist");
        let sqlparser::ast::Statement::Query(query) = statement else {
            panic!("statement must be query");
        };

        let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("query body must be select");
        };

        let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Function(function)) =
            &select.projection[0]
        else {
            panic!("projection must be function");
        };

        function.clone()
        
    }

    #[test]
    fn evaluate_case_projection_returns_first_matching_branch() {

        let mut row = HashMap::new();
        row.insert("active".to_string(), b"1".to_vec());

        let branches = vec![
            (
                SelectCaseWhen::Condition(SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: "active".to_string(),
                    op: SelectComparisonOp::Eq,
                    value: b"1".to_vec(),
                })),
                SelectExpression::Literal(b"yes".to_vec()),
            ),
            (
                SelectCaseWhen::Condition(SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: "active".to_string(),
                    op: SelectComparisonOp::Eq,
                    value: b"0".to_vec(),
                })),
                SelectExpression::Literal(b"no".to_vec()),
            ),
        ];

        let value = evaluate_case_projection(&row, None, &branches, None)
            .expect("case projection should evaluate");

        assert_eq!(value, Some(b"yes".to_vec()));

    }

    #[test]
    fn evaluate_case_projection_uses_else_when_no_branch_matches() {

        let mut row = HashMap::new();
        row.insert("active".to_string(), b"2".to_vec());

        let branches = vec![
            (
                SelectCaseWhen::Condition(SelectCondition::Predicate(SelectPredicate::Comparison {
                    field_name: "active".to_string(),
                    op: SelectComparisonOp::Eq,
                    value: b"1".to_vec(),
                })),
                SelectExpression::Literal(b"yes".to_vec()),
            ),
        ];

        let value = evaluate_case_projection(
            &row,
            None,
            &branches,
            Some(&SelectExpression::Literal(b"unknown".to_vec())),
        )
        .expect("case projection should evaluate");

        assert_eq!(value, Some(b"unknown".to_vec()));

    }

    #[test]
    fn evaluate_case_projection_supports_simple_case_with_function_result() {
        let mut row = HashMap::new();
        row.insert("state".to_string(), b"on".to_vec());

        let case_operand = SelectExpression::Column {
            field_name: "state".to_string(),
        };

        let branches = vec![(
            SelectCaseWhen::Equals(SelectExpression::Literal(b"on".to_vec())),
            SelectExpression::InbuiltFunction {
                function: parse_function_expr("select upper('yes')"),
            },
        )];

        let value = evaluate_case_projection(&row, Some(&case_operand), &branches, None)
            .expect("simple CASE projection should evaluate");

        assert_eq!(value, Some(b"YES".to_vec()));
    }

}
