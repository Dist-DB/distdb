use std::collections::HashSet;

use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::engine::database::inbuilt::{
    evaluate_inbuilt_sql_function, evaluate_inbuilt_sql_function_with_context,
    inbuilt_sql_runtime_context, is_inbuilt_function,
};

use super::SqlParseError;

pub fn is_supported_sql_function(function_name: &str) -> bool {
    is_inbuilt_function(function_name)
}

pub fn evaluate_sql_function(function: &Function) -> Result<Option<Vec<u8>>, SqlParseError> {
    evaluate_inbuilt_sql_function(function).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("SQL function evaluation failed: {err}"))
    })
}

pub fn evaluate_sql_function_with_lookup(
    function: &Function,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, SqlParseError> {
    let mut context = inbuilt_sql_runtime_context();
    context.argument_bindings.clear();

    for field_name in sql_function_column_references(function) {
        if let Some(value) = lookup(&field_name)
            .or_else(|| {
                field_name
                    .split_once('.')
                    .and_then(|(_, column)| lookup(column))
            })
        {
            context.argument_bindings.insert(field_name.clone(), value.clone());

            if let Some((_, column_name)) = field_name.split_once('.') {
                context
                    .argument_bindings
                    .entry(column_name.to_string())
                    .or_insert(value);
            }
        }
    }

    evaluate_inbuilt_sql_function_with_context(function, &context).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("SQL function evaluation failed: {err}"))
    })
}

pub fn sql_function_references_column(function: &Function) -> bool {
    !sql_function_column_references(function).is_empty()
}

fn sql_function_column_references(function: &Function) -> HashSet<String> {
    let mut references = HashSet::new();

    let args = match &function.args {
        FunctionArguments::None => return references,
        FunctionArguments::List(list) => &list.args,
        FunctionArguments::Subquery(_) => return references,
    };

    for argument in args {
        collect_function_argument_references(argument, &mut references);
    }

    references
}

fn collect_function_argument_references(
    argument: &FunctionArg,
    references: &mut HashSet<String>,
) {
    let expression = match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,
        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expression),
            ..
        } => expression,
        _ => return,
    };

    collect_expression_references(expression, references);
}

fn collect_expression_references(expression: &Expr, references: &mut HashSet<String>) {
    match expression {
        Expr::Identifier(identifier) => {
            references.insert(common::normalize_identifier!(&identifier.value));
        }

        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            references.insert(format!(
                "{}.{}",
                common::normalize_identifier!(&parts[0].value),
                common::normalize_identifier!(&parts[1].value)
            ));
        }

        Expr::Nested(inner) => collect_expression_references(inner, references),

        Expr::UnaryOp { expr, .. } => collect_expression_references(expr, references),

        Expr::BinaryOp { left, right, .. } => {
            collect_expression_references(left, references);
            collect_expression_references(right, references);
        }

        Expr::Function(function) => {
            for nested_ref in sql_function_column_references(function) {
                references.insert(nested_ref);
            }
        }

        _ => {}
    }
}