use sqlparser::ast::{Expr, Function, Value};
use std::borrow::Cow;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::{
    evaluate_argument_expression,
    function_argument_expr,
    function_args,
    inbuilt_sql_runtime_context,
};

pub struct LookupCommand;

// Custom non-MySQL8 helper:
// LOOKUP(<table>, <rowid>, <column>, <default>)
// Semantics target: select ifnull(<column>, <default>) from <table> where <primarykey>=<rowid>

impl InbuiltServerCommand for LookupCommand {

    fn name(&self) -> &'static str {
        "LOOKUP"
    }

    fn usage(&self) -> Cow<'static, str> {
        Cow::Borrowed("LOOKUP(<table>, <rowid>, <column>, <default>)")
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        if args.len() != 4 {
            return Err(format!(
                "{} requires 4 arguments: <table>, <rowid>, <column>, <default>",
                self.name()
            ));
        }

        let table_name = parse_identifier_or_string_arg(args, 0, "table")?;
        let row_id = evaluate_argument_expression(function_argument_expr(&args[1])?)?
            .map(|value| String::from_utf8_lossy(&value).into_owned())
            .unwrap_or_default();
        let column_name = parse_identifier_or_string_arg(args, 2, "column")?;
        let default_value = evaluate_argument_expression(function_argument_expr(&args[3])?)?;

        let context = inbuilt_sql_runtime_context();

        // Binding-first resolution for the custom lookup primitive.
        // Callers that provide inquery-bound values can populate one of these keys.
        let lookup_keys = [
            format!("{}.{}.{}", table_name, row_id, column_name),
            format!("{}.{}", table_name, column_name),
            column_name.clone(),
        ];

        for key in lookup_keys {
            let normalized = common::normalize_identifier!(key);
            if let Some(value) = context.argument_bindings.get(&normalized) {
                if is_null_like(value) {
                    return Ok(default_value);
                }
                return Ok(Some(value.clone()));
            }
        }

        Ok(default_value)

    }

}

fn parse_identifier_or_string_arg(
    args: &[sqlparser::ast::FunctionArg],
    index: usize,
    role: &str,
) -> Result<String, String> {

    let expr = function_argument_expr(&args[index])?;

    match expr {

        Expr::Identifier(identifier) => Ok(common::normalize_identifier!(&identifier.value)),

        Expr::CompoundIdentifier(parts) => {
            let normalized = parts
                .iter()
                .map(|part| common::normalize_identifier!(&part.value))
                .collect::<Vec<_>>()
                .join(".");
            Ok(normalized)
        }

        Expr::Value(
            Value::SingleQuotedString(value)
            | Value::DoubleQuotedString(value)
            | Value::TripleSingleQuotedString(value)
            | Value::TripleDoubleQuotedString(value),
        ) => Ok(common::normalize_identifier!(value)),

        _ => Err(format!(
            "{} argument '{}' must be an identifier or string literal",
            index + 1,
            role
        )),

    }

}

fn is_null_like(value: &[u8]) -> bool {
    String::from_utf8_lossy(value).trim().eq_ignore_ascii_case("null")
}
