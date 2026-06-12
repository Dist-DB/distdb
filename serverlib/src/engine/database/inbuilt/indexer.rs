use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Value};

use super::command::InbuiltServerCommand;
use super::concat::ConcatCommand;
use super::unixtimestamp::UnixTimestampCommand;

pub fn is_inbuilt_function(function_name: &str) -> bool {
    resolve_command(function_name).is_some()
}

pub fn evaluate_inbuilt_sql_function(function: &Function) -> Result<Option<Vec<u8>>, String> {
    let function_name = function.name.to_string();
    let Some(command) = resolve_command(&function_name) else {
        return Err(format!("unsupported inbuilt function '{}'", function_name));
    };

    command.evaluate(function)
}

pub(super) fn evaluate_argument_expression(expression: &Expr) -> Result<Option<Vec<u8>>, String> {
    match expression {
        Expr::Nested(inner) => evaluate_argument_expression(inner),

        Expr::Value(value) => value_to_bytes(value),

        Expr::Function(function) => evaluate_inbuilt_sql_function(function),

        _ => Err("inbuilt command arguments currently support only literals and inbuilt nested calls".to_string()),
    }
}

pub(super) fn function_argument_expr(argument: &FunctionArg) -> Result<&Expr, String> {
    match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        FunctionArg::Named { arg, .. } => match arg {
            FunctionArgExpr::Expr(expr) => Ok(expr),
            _ => Err("unsupported inbuilt command argument".to_string()),
        },
        _ => Err("unsupported inbuilt command argument".to_string()),
    }
}

pub(super) fn function_args(function: &Function) -> Result<&[FunctionArg], String> {
    match &function.args {
        FunctionArguments::None => Ok(&[]),
        FunctionArguments::List(list) => Ok(&list.args),
        FunctionArguments::Subquery(_) => {
            Err("subquery function arguments are not supported for inbuilt commands".to_string())
        }
    }
}

fn resolve_command(function_name: &str) -> Option<&'static dyn InbuiltServerCommand> {
    static UNIX_TIMESTAMP: UnixTimestampCommand = UnixTimestampCommand;
    static CONCAT: ConcatCommand = ConcatCommand;

    let normalized = normalize_name(function_name);

    match normalized.as_str() {
        "unixtimestamp" | "unix_timestamp" => Some(&UNIX_TIMESTAMP),
        "concat" => Some(&CONCAT),
        _ => None,
    }
}

fn normalize_name(function_name: &str) -> String {
    function_name
        .chars()
        .filter(|ch| *ch != '`' && *ch != '"')
        .collect::<String>()
        .to_ascii_lowercase()
}

fn value_to_bytes(value: &Value) -> Result<Option<Vec<u8>>, String> {
    
    match value {
        Value::Null => Ok(None),
        Value::Boolean(v) => Ok(Some(v.to_string().into_bytes())),
        Value::Number(v, _) => Ok(Some(v.to_string().into_bytes())),

        Value::SingleQuotedString(v)
        | Value::DoubleQuotedString(v)
        | Value::TripleSingleQuotedString(v)
        | Value::TripleDoubleQuotedString(v)
        | Value::EscapedStringLiteral(v)
        | Value::UnicodeStringLiteral(v)
        | Value::SingleQuotedByteStringLiteral(v)
        | Value::DoubleQuotedByteStringLiteral(v)
        | Value::TripleSingleQuotedByteStringLiteral(v)
        | Value::TripleDoubleQuotedByteStringLiteral(v)
        | Value::SingleQuotedRawStringLiteral(v)
        | Value::DoubleQuotedRawStringLiteral(v)
        | Value::TripleSingleQuotedRawStringLiteral(v)
        | Value::TripleDoubleQuotedRawStringLiteral(v)
        | Value::NationalStringLiteral(v)
        | Value::HexStringLiteral(v) => Ok(Some(v.as_bytes().to_vec())),

        Value::DollarQuotedString(v) => Ok(Some(v.value.as_bytes().to_vec())),

        Value::Placeholder(v) => Err(format!(
            "inbuilt command placeholder '{}' is not supported",
            v
        )),
    }
    
}
