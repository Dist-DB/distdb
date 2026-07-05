use sqlparser::ast::FunctionArg;

use crate::engine::database::inbuilt::indexer::{
    evaluate_argument_expression,
    function_argument_expr,
    inbuilt_sql_runtime_context,
};

pub(super) fn expect_arg_count(
    args: &[FunctionArg],
    min: usize,
    max: usize,
    function_name: &str,
) -> Result<(), String> {
    
    if args.len() < min || args.len() > max {
        let usage = usage_for_clarity(function_name, min, max);
        if min == max {
            return Err(format!(
                "{} requires {} argument(s); usage: {}",
                function_name,
                min,
                usage
            ));
        }
        return Err(format!(
            "{} requires between {} and {} arguments; usage: {}",
            function_name,
            min,
            max,
            usage
        ));
    }

    Ok(())

}

fn usage_for_clarity(function_name: &str, min: usize, max: usize) -> String {

    if min == 0 && max == 0 {
        return format!("{}()", function_name);
    }

    if min == max {
        let args = (1..=min)
            .map(|idx| format!("<arg{}>", idx))
            .collect::<Vec<_>>()
            .join(", ");
        return format!("{}({})", function_name, args);
    }

    if max == usize::MAX {
        if min == 0 {
            return format!("{}(<arg1>, ...)", function_name);
        }

        let mut required = (1..=min)
            .map(|idx| format!("<arg{}>", idx))
            .collect::<Vec<_>>()
            .join(", ");
        required.push_str(&format!(", [<arg{}>, ...]", min + 1));
        return format!("{}({})", function_name, required);
    }

    format!("{}(<arg1>, ...)", function_name)

}

pub(super) fn evaluate_bytes_arg(args: &[FunctionArg], index: usize) -> Result<Option<Vec<u8>>, String> {
    let expr = function_argument_expr(&args[index])?;
    evaluate_argument_expression(expr)
}

pub(super) fn evaluate_string_arg(args: &[FunctionArg], index: usize) -> Result<Option<String>, String> {
    Ok(evaluate_bytes_arg(args, index)?
        .map(|value| String::from_utf8_lossy(&value).into_owned()))
}

pub(super) fn evaluate_i64_arg(args: &[FunctionArg], index: usize) -> Result<Option<i64>, String> {

    let Some(value) = evaluate_string_arg(args, index)? else {
        return Ok(None);
    };

    let trimmed = value.trim();
    if let Ok(parsed) = trimmed.parse::<i64>() {
        return Ok(Some(parsed));
    }

    trimmed
        .parse::<f64>()
        .map(|parsed| Some(parsed.trunc() as i64))
        .map_err(|_| format!("argument {} must be numeric", index + 1))

}

pub(super) fn string_result(value: impl Into<String>) -> Option<Vec<u8>> {
    Some(value.into().into_bytes())
}

pub(super) fn number_result<T: ToString>(value: T) -> Option<Vec<u8>> {
    Some(value.to_string().into_bytes())
}

pub(super) fn runtime_current_user() -> String {
    let context = inbuilt_sql_runtime_context();
    context
        .current_user
        .or_else(|| std::env::var("USER").ok().map(|user| format!("{}@localhost", user)))
        .unwrap_or_else(|| "distdb@localhost".to_string())
}

pub(super) fn runtime_session_user() -> String {
    let context = inbuilt_sql_runtime_context();
    context
        .session_user
        .unwrap_or_else(runtime_current_user)
}

pub(super) fn runtime_system_user() -> String {
    let context = inbuilt_sql_runtime_context();
    context
        .system_user
        .unwrap_or_else(runtime_current_user)
}

pub(super) fn runtime_database() -> Option<String> {
    let context = inbuilt_sql_runtime_context();
    context.current_database
}

pub(super) fn runtime_connection_id() -> i64 {
    let context = inbuilt_sql_runtime_context();
    context.connection_id.unwrap_or(1)
}

pub(super) fn runtime_last_insert_id() -> i64 {
    let context = inbuilt_sql_runtime_context();
    context.last_insert_id.unwrap_or(0)
}

pub(super) fn runtime_version() -> String {
    let context = inbuilt_sql_runtime_context();
    context
        .version
        .unwrap_or_else(|| format!("distdb-{}", env!("CARGO_PKG_VERSION")))
}

pub(super) fn is_truthy(value: &[u8]) -> bool {

    let text = String::from_utf8_lossy(value);
    let normalized = text.trim().to_ascii_lowercase();

    if normalized.is_empty() {
        return false;
    }

    if let Ok(number) = normalized.parse::<f64>() {
        return number != 0.0;
    }

    normalized != "false" && normalized != "null"

}

pub(super) fn parse_base(value: i64) -> Option<u32> {
    
    let base = value.unsigned_abs() as u32;

    if (2..=36).contains(&base) {
        Some(base)
    } else {
        None
    }

}

pub(super) fn parse_signed_in_base(text: &str, base: u32) -> Option<i128> {
    
    let trimmed = text.trim();

    if trimmed.is_empty() {
        return None;
    }

    let negative = trimmed.starts_with('-');
    let magnitude = trimmed.trim_start_matches(['+', '-']);

    if magnitude.is_empty() {
        return None;
    }

    let parsed = i128::from_str_radix(magnitude, base).ok()?;
    Some(if negative { -parsed } else { parsed })

}

pub(super) fn format_signed_in_base(value: i128, base: u32) -> String {

    if value == 0 {
        return "0".to_string();
    }

    let negative = value < 0;
    let mut magnitude = value.unsigned_abs();
    let radix = base as u128;
    let mut digits = Vec::new();

    while magnitude > 0 {
        let digit = (magnitude % radix) as u8;
        let symbol = if digit < 10 {
            (b'0' + digit) as char
        } else {
            (b'A' + (digit - 10)) as char
        };
        digits.push(symbol);
        magnitude /= radix;
    }

    digits.reverse();

    let mut rendered = String::new();
    if negative {
        rendered.push('-');
    }
    
    rendered.extend(digits);
    rendered

}
