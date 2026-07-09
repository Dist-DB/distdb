use std::collections::HashMap;

use crate::{FieldDef, FieldIndex, FieldType, SelectProjectionItem, SelectReadPlan};
use crate::engine::sql::{
    expression_references_column, SelectCaseWhen, SqlFunctionEvaluationStrategy,
};
use crate::engine::execution::SelectExecutionResult;
use crate::engine::execution::commands::control_flow::evaluate_case_projection;

use super::post_processing::{
    apply_select_post_processing, column_metadata_with_visibility, strip_hidden_output_columns,
};

pub fn execute_projection_only_select_plan<E>(
    read_plan: &SelectReadPlan,
    evaluate_function: &mut E,
) -> Result<SelectExecutionResult, String>
where
    E: SqlFunctionEvaluationStrategy,
{

    let mut columns = Vec::with_capacity(read_plan.projection_items.len());
    let mut row = Vec::with_capacity(read_plan.projection_items.len());

    for (seq, projection_item) in read_plan.projection_items.iter().enumerate() {

        match projection_item {

            SelectProjectionItem::InbuiltFunction {
                output_name,
                function,
            } => {

                let value = evaluate_function.evaluate(function, &mut |_| None).map_err(|err| {
                    format!("select failed: inbuilt projection evaluation failed: {err}")
                })?;

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                row.push(value.unwrap_or_else(|| b"NULL".to_vec()));

            },

            SelectProjectionItem::Column { .. } => {
                return Err("select without FROM only supports inbuilt projection functions".to_string());
            },

            SelectProjectionItem::WindowFunction { output_name, .. } => {

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                row.push(Vec::new());

            },

            SelectProjectionItem::Case { .. } => {

                let SelectProjectionItem::Case {
                    output_name,
                    operand,
                    branches,
                    else_value,
                } = projection_item
                else {
                    unreachable!("case projection match arm should only receive CASE projections")
                };

                if case_projection_requires_row_context(operand.as_ref(), branches, else_value.as_ref()) {
                    return Err(
                        "select without FROM CASE projections support only row-independent expressions"
                            .to_string(),
                    );
                }

                let row_provider = HashMap::<String, Vec<u8>>::new();
                let value = evaluate_case_projection(
                    &row_provider,
                    operand.as_ref(),
                    branches,
                    else_value.as_ref(),
                    evaluate_function,
                )?;

                columns.push(FieldDef {
                    seqno: (seq + 1) as u32,
                    field_name: output_name.clone(),
                    field_type: FieldType::Text,
                    nullable: true,
                    indexed: FieldIndex::None,
                    default_value: None,
                    metadata: None,
                });

                row.push(value.unwrap_or_else(|| b"NULL".to_vec()));

            },

            SelectProjectionItem::Wildcard { .. } => {
                return Err("select without FROM does not support wildcard projections".to_string());
            }
            
        }

    }

    let rows = apply_select_post_processing(vec![row], &columns, read_plan, &read_plan.projection_items)?;

    Ok(SelectExecutionResult {
        columns,
        rows,
    })

}

fn case_projection_requires_row_context(
    operand: Option<&crate::engine::sql::SelectExpression>,
    branches: &[(SelectCaseWhen, crate::engine::sql::SelectExpression)],
    else_value: Option<&crate::engine::sql::SelectExpression>,
) -> bool {

    if operand.is_some_and(expression_references_column) {
        return true;
    }

    if else_value.is_some_and(expression_references_column) {
        return true;
    }

    branches.iter().any(|(branch_when, value)| {
        let branch_references_row = match branch_when {
            SelectCaseWhen::Condition(_) => true,
            SelectCaseWhen::Equals(expected) => expression_references_column(expected),
        };

        branch_references_row || expression_references_column(value)
    })

}
