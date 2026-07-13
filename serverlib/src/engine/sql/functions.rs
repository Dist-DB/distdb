use std::collections::HashSet;

use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, UnaryOperator, Value};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::engine::database::inbuilt::{
    evaluate_inbuilt_sql_function, evaluate_inbuilt_sql_function_with_context,
    inbuilt_sql_runtime_context, is_inbuilt_function,
};

use super::SqlParseError;

pub trait SqlFunctionEvaluationStrategy {
    fn evaluate(
        &mut self,
        function: &Function,
        lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>;
}

pub struct LookupAwareSqlFunctionEvaluator<F>(pub F);

pub fn with_lookup_sql_function_evaluator<F>(
    evaluator: F,
) -> LookupAwareSqlFunctionEvaluator<F>
where
    F: for<'a, 'b> FnMut(
        &'a Function,
        &'b mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>,
{
    LookupAwareSqlFunctionEvaluator(evaluator)
}

impl<F> SqlFunctionEvaluationStrategy for F
where
    F: for<'a> FnMut(&'a Function) -> Result<Option<Vec<u8>>, String>,
{
    fn evaluate(
        &mut self,
        function: &Function,
        _lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        self(function)
    }
}

impl<F> SqlFunctionEvaluationStrategy for LookupAwareSqlFunctionEvaluator<F>
where
    F: for<'a, 'b> FnMut(
        &'a Function,
        &'b mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>,
{
    fn evaluate(
        &mut self,
        function: &Function,
        lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String> {
        (self.0)(function, lookup)
    }
}

pub fn is_supported_sql_function(function_name: &str) -> bool {
    is_inbuilt_function(function_name) || !function_name.trim().is_empty()
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

    evaluate_inbuilt_sql_function_with_lookup(function, lookup).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("SQL function evaluation failed: {err}"))
    })

}

pub fn evaluate_inbuilt_sql_function_with_lookup(
    function: &Function,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
) -> Result<Option<Vec<u8>>, String> {

    let mut context = inbuilt_sql_runtime_context();
    let mut merged_bindings = context.argument_bindings.clone();

    for field_name in sql_function_column_references(function) {

        if let Some(value) = lookup(&field_name)
            .or_else(|| {
                field_name
                    .split_once('.')
                    .and_then(|(_, column)| lookup(column))
            })
        {
            merged_bindings.insert(field_name.clone(), value.clone());

            if let Some((_, column_name)) = field_name.split_once('.') {
                merged_bindings
                    .entry(column_name.to_string())
                    .or_insert(value);
            }
        }

    }

    context.argument_bindings = merged_bindings;

    evaluate_inbuilt_sql_function_with_context(function, &context)

}

pub fn function_argument_values(
    function: &Function,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    evaluate_nested_function: &mut dyn FnMut(
        &Function,
        &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>,
) -> Result<Vec<Vec<u8>>, String> {

    let args = match &function.args {

        FunctionArguments::None => return Ok(Vec::new()),

        FunctionArguments::List(list) => &list.args,

        FunctionArguments::Subquery(_) => {
            return Err("function subquery arguments are not supported".to_string());
        }

    };

    args.iter()
        .map(|argument| function_argument_to_bytes(argument, lookup, evaluate_nested_function))
        .collect()

}

pub fn evaluate_expression_sql_to_bytes(
    expression_sql: &str,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    evaluate_nested_function: &mut dyn FnMut(
        &Function,
        &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>,
) -> Result<Vec<u8>, String> {

    let statement_sql = format!("select {expression_sql}");
    
    let statements = Parser::parse_sql(&MySqlDialect {}, &statement_sql)
        .map_err(|err| format!("expression parse failed: {err}"))?;

    let Some(sqlparser::ast::Statement::Query(query)) = statements.first() else {
        return Err("expression parse failed: expected SELECT wrapper".to_string());
    };

    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return Err("expression parse failed: expected simple SELECT body".to_string());
    };

    let Some(sqlparser::ast::SelectItem::UnnamedExpr(expression)) = select.projection.first() else {
        return Err("expression parse failed: expected single unnamed projection".to_string());
    };

    expression_to_argument_bytes(expression, lookup, evaluate_nested_function)

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

fn function_argument_to_bytes(
    argument: &FunctionArg,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    evaluate_nested_function: &mut dyn FnMut(
        &Function,
        &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>,
) -> Result<Vec<u8>, String> {

    let expression = match argument {
        
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => expression,

        FunctionArg::Named {
            arg: FunctionArgExpr::Expr(expression),
            ..
        } => expression,

        _ => return Err("unsupported function argument".to_string()),

    };

    expression_to_argument_bytes(expression, lookup, evaluate_nested_function)

}

fn expression_to_argument_bytes(
    expression: &Expr,
    lookup: &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    evaluate_nested_function: &mut dyn FnMut(
        &Function,
        &mut dyn FnMut(&str) -> Option<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>, String>,
) -> Result<Vec<u8>, String> {

    match expression {

        Expr::Value(value) => value_to_argument_bytes(value),

        Expr::UnaryOp { op, expr } => match (op, expr.as_ref()) {
            (UnaryOperator::Plus, Expr::Value(Value::Number(value, _))) => {
                Ok(value.clone().into_bytes())
            }
            (UnaryOperator::Minus, Expr::Value(Value::Number(value, _))) => {
                Ok(format!("-{value}").into_bytes())
            }
            _ => Err("unsupported function unary argument".to_string()),
        },

        Expr::Identifier(identifier) => lookup(&common::normalize_identifier!(&identifier.value))
            .ok_or_else(|| format!("unresolved function argument '{}': missing column binding", identifier.value)),

        Expr::CompoundIdentifier(parts) => {
            let qualified = parts
                .iter()
                .map(|part| common::normalize_identifier!(&part.value))
                .collect::<Vec<_>>()
                .join(".");

            lookup(&qualified)
                .or_else(|| qualified.split_once('.').and_then(|(_, column)| lookup(column)))
                .ok_or_else(|| format!("unresolved function argument '{qualified}': missing column binding"))
        },

        Expr::Function(function) => evaluate_nested_function(function, lookup)
            .map(|value| value.unwrap_or_else(|| b"NULL".to_vec())),

        Expr::Nested(inner) => {
            expression_to_argument_bytes(inner, lookup, evaluate_nested_function)
        },

        _ => Err(format!("unsupported function argument expression '{expression}'")),

    }

}

fn value_to_argument_bytes(value: &Value) -> Result<Vec<u8>, String> {

    match value {
        
        Value::Null => Ok(b"NULL".to_vec()),
        
        Value::Boolean(value) => Ok(value.to_string().into_bytes()),
        
        Value::Number(value, _) => Ok(value.to_string().into_bytes()),

        Value::SingleQuotedString(value) |
        Value::DoubleQuotedString(value) |
        Value::TripleSingleQuotedString(value) |
        Value::TripleDoubleQuotedString(value) |
        Value::EscapedStringLiteral(value) |
        Value::UnicodeStringLiteral(value) |
        Value::SingleQuotedByteStringLiteral(value) |
        Value::DoubleQuotedByteStringLiteral(value) |
        Value::TripleSingleQuotedByteStringLiteral(value) |
        Value::TripleDoubleQuotedByteStringLiteral(value) |
        Value::SingleQuotedRawStringLiteral(value) |
        Value::DoubleQuotedRawStringLiteral(value) |
        Value::TripleSingleQuotedRawStringLiteral(value) |
        Value::TripleDoubleQuotedRawStringLiteral(value) |
        Value::NationalStringLiteral(value) |
        Value::HexStringLiteral(value) => Ok(value.as_bytes().to_vec()),

        Value::DollarQuotedString(value) => Ok(value.value.as_bytes().to_vec()),

        Value::Placeholder(value) => Err(format!("unsupported function argument placeholder '{value}'")),
        
    }

}

fn collect_expression_references(expression: &Expr, references: &mut HashSet<String>) {

    match expression {

        Expr::Identifier(identifier) => {
            references.insert(common::normalize_identifier!(&identifier.value));
        },

        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            references.insert(format!(
                "{}.{}",
                common::normalize_identifier!(&parts[0].value),
                common::normalize_identifier!(&parts[1].value)
            ));
        },

        Expr::Nested(inner) => collect_expression_references(inner, references),

        Expr::UnaryOp { expr, .. } => collect_expression_references(expr, references),

        Expr::BinaryOp { left, right, .. } => {
            collect_expression_references(left, references);
            collect_expression_references(right, references);
        },

        Expr::Function(function) => {
            for nested_ref in sql_function_column_references(function) {
                references.insert(nested_ref);
            }
        }

        _ => {}

    }
    
}