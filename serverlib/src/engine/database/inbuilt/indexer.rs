
use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, 
    Expr, 
    Function, 
    FunctionArg, 
    FunctionArgExpr, 
    FunctionArguments, 
    UnaryOperator, 
    Value
};

use super::command::InbuiltServerCommand;

use super::strings;
use super::datetime;
use super::numeric;
use super::advanced;
use super::geo;
use super::custom;

#[derive(Clone, Debug, Default)]
pub struct InbuiltSqlRuntimeContext {
    pub current_database: Option<String>,
    pub current_user: Option<String>,
    pub session_user: Option<String>,
    pub system_user: Option<String>,
    pub connection_id: Option<i64>,
    pub last_insert_id: Option<i64>,
    pub version: Option<String>,
    pub argument_bindings: HashMap<String, Vec<u8>>,
}

thread_local! {
    static INBUILT_RUNTIME_CONTEXT_STACK: RefCell<Vec<InbuiltSqlRuntimeContext>> = const { RefCell::new(Vec::new()) };
}

const REGISTERED_INBUILT_FUNCTION_NAMES: &[&str] = &[
    "distance",
    "ascii",
    "char_length",
    "character_length",
    "concat",
    "concat_w",
    "concat_ws",
    "field",
    "find_in_set",
    "format",
    "insert",
    "instr",
    "left",
    "length",
    "locate",
    "lpad",
    "rpad",
    "ltrim",
    "mid",
    "position",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rtrim",
    "space",
    "substr",
    "substring",
    "upper",
    "ucase",
    "lower",
    "lcase",
    "substring_index",
    "trim",
    "adddate",
    "addtime",
    "curdate",
    "current_date",
    "curtime",
    "current_time",
    "date",
    "date_add",
    "datediff",
    "date_format",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "extract",
    "from_days",
    "hour",
    "last_day",
    "localtime",
    "localtimestamp",
    "makedate",
    "maketime",
    "microsecond",
    "minute",
    "month",
    "now",
    "period_add",
    "period_diff",
    "quarter",
    "sec_to_time",
    "second",
    "str_to_date",
    "subdate",
    "date_sub",
    "subtime",
    "sysdate",
    "time_format",
    "time_to_sec",
    "time",
    "timediff",
    "timestamp",
    "to_days",
    "unixtimestamp",
    "unix_timestamp",
    "week",
    "weekday",
    "weekofyear",
    "year",
    "yearweek",
    "abs",
    "acos",
    "asin",
    "atan",
    "atan2",
    "avg",
    "ceil",
    "ceiling",
    "cos",
    "count",
    "cot",
    "degrees",
    "div",
    "exp",
    "floor",
    "greatest",
    "least",
    "ln",
    "log",
    "log10",
    "log2",
    "max",
    "min",
    "mod",
    "pi",
    "pow",
    "power",
    "radians",
    "rand",
    "round",
    "sign",
    "sin",
    "sqrt",
    "sum",
    "tan",
    "truncate",
    "bin",
    "binary",
    "case",
    "cast",
    "coalesce",
    "connection_id",
    "conv",
    "convert",
    "current_user",
    "database",
    "if",
    "ifnull",
    "isnull",
    "last_insert_id",
    "nullif",
    "session_user",
    "system_user",
    "user",
    "version",
    "lookup",
];

pub fn with_inbuilt_sql_runtime_context<T>(
    context: &InbuiltSqlRuntimeContext,
    callback: impl FnOnce() -> T,
) -> T {

    INBUILT_RUNTIME_CONTEXT_STACK.with(|stack| {
        stack.borrow_mut().push(context.clone());
    });

    let outcome = callback();

    INBUILT_RUNTIME_CONTEXT_STACK.with(|stack| {
        let _ = stack.borrow_mut().pop();
    });

    outcome

}

pub fn inbuilt_sql_runtime_context() -> InbuiltSqlRuntimeContext {

    INBUILT_RUNTIME_CONTEXT_STACK.with(|stack| {
        stack
            .borrow()
            .last()
            .cloned()
            .unwrap_or_default()
    })

}

pub fn is_inbuilt_function(function_name: &str) -> bool {
    resolve_command(function_name).is_some()
}

pub fn registered_inbuilt_function_names() -> &'static [&'static str] {
    REGISTERED_INBUILT_FUNCTION_NAMES
}

pub fn evaluate_inbuilt_sql_function(function: &Function) -> Result<Option<Vec<u8>>, String> {

    let function_name = function.name.to_string();
    let Some(command) = resolve_command(&function_name) else {
        return Err(format!("unsupported inbuilt function '{}'", function_name));
    };

    command
        .evaluate(function)
        .map_err(|err| {
            if err.to_ascii_lowercase().contains("usage:") {
                err
            } else {
                format!("{}; usage: {}", err, command.usage())
            }
        })

}

pub fn evaluate_inbuilt_sql_function_with_context(
    function: &Function,
    context: &InbuiltSqlRuntimeContext,
) -> Result<Option<Vec<u8>>, String> {
    with_inbuilt_sql_runtime_context(context, || evaluate_inbuilt_sql_function(function))
}

pub(super) fn evaluate_argument_expression(expression: &Expr) -> Result<Option<Vec<u8>>, String> {

    match expression {

        Expr::Nested(inner) => evaluate_argument_expression(inner),

        Expr::Value(value) => value_to_bytes(value),

        Expr::Identifier(identifier) => {
            let field_name = common::normalize_identifier!(&identifier.value);
            let context = inbuilt_sql_runtime_context();

            Ok(context.argument_bindings.get(&field_name).cloned())
        }

        Expr::CompoundIdentifier(parts) => {
            if parts.len() != 2 {
                return Err(
                    "inbuilt command compound identifiers currently support only qualifier.column"
                        .to_string(),
                );
            }

            let qualified_name = format!(
                "{}.{}",
                common::normalize_identifier!(&parts[0].value),
                common::normalize_identifier!(&parts[1].value)
            );

            let context = inbuilt_sql_runtime_context();

            if let Some(value) = context.argument_bindings.get(&qualified_name) {
                return Ok(Some(value.clone()));
            }

            Ok(context
                .argument_bindings
                .get(&common::normalize_identifier!(&parts[1].value))
                .cloned())
        }

        Expr::UnaryOp { op, expr } => match (op, expr.as_ref()) {

            (UnaryOperator::Plus, Expr::Value(Value::Number(value, _))) => {
                Ok(Some(value.as_bytes().to_vec()))
            },

            (UnaryOperator::Minus, Expr::Value(Value::Number(value, _))) => {
                Ok(Some(format!("-{}", value).into_bytes()))
            },

            (UnaryOperator::Plus, inner) => {
                let Some(value) = evaluate_numeric_expression(inner)? else {
                    return Ok(None);
                };
                Ok(Some(format_number_result(value)))
            },

            (UnaryOperator::Minus, inner) => {
                let Some(value) = evaluate_numeric_expression(inner)? else {
                    return Ok(None);
                };
                Ok(Some(format_number_result(-value)))
            },

            _ => Err("inbuilt command unary arguments currently support only numeric literals".to_string()),

        },

        Expr::BinaryOp { left, op, right } => {

            let Some(left_value) = evaluate_numeric_expression(left)? else {
                return Ok(None);
            };

            let Some(right_value) = evaluate_numeric_expression(right)? else {
                return Ok(None);
            };

            let result = match op {
                BinaryOperator::Plus => left_value + right_value,
                BinaryOperator::Minus => left_value - right_value,
                BinaryOperator::Multiply => left_value * right_value,
                BinaryOperator::Divide => left_value / right_value,
                BinaryOperator::Modulo => left_value % right_value,
                _ => {
                    return Err(
                        "inbuilt command binary arguments currently support only numeric arithmetic"
                            .to_string(),
                    )
                }
            };

            if !result.is_finite() {
                return Ok(None);
            }

            Ok(Some(format_number_result(result)))

        }

        Expr::Function(function) => evaluate_inbuilt_sql_function(function),

        _ => Err("inbuilt command arguments currently support literals, column references, and inbuilt nested calls".to_string()),
        
    }

}

fn evaluate_numeric_expression(expression: &Expr) -> Result<Option<f64>, String> {

    let Some(value) = evaluate_argument_expression(expression)? else {
        return Ok(None);
    };

    let text = String::from_utf8_lossy(&value);

    text.trim()
        .parse::<f64>()
        .map(Some)
        .map_err(|_| "inbuilt command arithmetic expressions must evaluate to numeric values".to_string())

}

fn format_number_result(value: f64) -> Vec<u8> {

    let mut text = if value == 0.0 {
        "0".to_string()
    } else {
        value.to_string()
    };

    if text.contains('.') && !text.contains('e') && !text.contains('E') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }

    text.into_bytes()

}

pub(super) fn function_argument_expr(argument: &FunctionArg) -> Result<&Expr, String> {

    match argument {
        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => Ok(expr),
        FunctionArg::Named { arg: FunctionArgExpr::Expr(expr), .. } => Ok(expr),
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

    let normalized = normalize_name(function_name);

    // we mirror MySQL's function name normalization and resolution rules, which are case-insensitive and ignore backticks and double quotes
    // https://www.w3schools.com/mySQL/mysql_ref_functions.asp

    match normalized.as_ref() {

        // geo functions
        
        "distance"                              => Some(&geo::distance::DistanceCommand),

        // string functions
        
        "ascii"                                 => Some(&strings::ascii::AsciiCommand),
        "char_length" | "character_length"      => Some(&strings::char_length::CharLengthCommand),
        "concat"                                => Some(&strings::concat::ConcatCommand),
        "concat_w" | "concat_ws"                => Some(&strings::concat_w::ConcatWCommand),
        "field"                                 => Some(&strings::field::FieldCommand),
        "find_in_set"                           => Some(&strings::find_in_set::FindInSetCommand),
        "format"                                => Some(&strings::format::FormatCommand),
        "insert"                                => Some(&strings::insert::InsertCommand),
        "instr"                                 => Some(&strings::instr::InstrCommand),
        "left"                                  => Some(&strings::left::LeftCommand),
        "length"                                => Some(&strings::length::LengthCommand),
        "locate"                                => Some(&strings::locate::LocateCommand),
        "lpad"                                  => Some(&strings::lpad::LpadCommand),
        "rpad"                                  => Some(&strings::rpad::RpadCommand),
        "ltrim"                                 => Some(&strings::ltrim::LtrimCommand),
        "mid"                                   => Some(&strings::mid::MidCommand),
        "position"                              => Some(&strings::position::PositionCommand),
        "repeat"                                => Some(&strings::repeat::RepeatCommand),
        "replace"                               => Some(&strings::replace::ReplaceCommand),
        "reverse"                               => Some(&strings::reverse::ReverseCommand),
        "right"                                 => Some(&strings::right::RightCommand),
        "rtrim"                                 => Some(&strings::rtrim::RtrimCommand),
        "space"                                 => Some(&strings::space::SpaceCommand),
        "substr" | "substring"                  => Some(&strings::substr::SubstrCommand),
        "upper" | "ucase"                       => Some(&strings::upper::UpperCommand),
        "lower" | "lcase"                       => Some(&strings::lower::LowerCommand),
        "substring_index"                       => Some(&strings::substring_index::SubstringIndexCommand),
        "trim"                                  => Some(&strings::trim::TrimCommand),        
        
        // date and time functions
        
        "adddate"                               => Some(&datetime::adddate::AddDateCommand),
        "addtime"                               => Some(&datetime::addtime::AddTimeCommand),
        "curdate" | "current_date"              => Some(&datetime::curdate::CurDateCommand),
        "curtime" | "current_time"              => Some(&datetime::curtime::CurTimeCommand),
        "date"                                  => Some(&datetime::date::DateCommand),
        "date_add"                              => Some(&datetime::adddate::AddDateCommand),
        "datediff"                              => Some(&datetime::datediff::DateDiffCommand),
        "date_format"                           => Some(&datetime::date_format::DateFormatCommand),
        "day"                                   => Some(&datetime::day::DayCommand),
        "dayname"                               => Some(&datetime::dayname::DayNameCommand),
        "dayofmonth"                            => Some(&datetime::dayofmonth::DayOfMonthCommand),
        "dayofweek"                             => Some(&datetime::dayofweek::DayOfWeekCommand),
        "dayofyear"                             => Some(&datetime::dayofyear::DayOfYearCommand),
        "extract"                               => Some(&datetime::extract::ExtractCommand),
        "from_days"                             => Some(&datetime::from_days::FromDaysCommand),
        "hour"                                  => Some(&datetime::hour::HourCommand),
        "last_day"                              => Some(&datetime::last_day::LastDayCommand),
        "localtime"                             => Some(&datetime::localtime::LocalTimeCommand),
        "localtimestamp"                        => Some(&datetime::localtimestamp::LocalTimestampCommand),
        "makedate"                              => Some(&datetime::makedate::MakeDateCommand),
        "maketime"                              => Some(&datetime::maketime::MakeTimeCommand),
        "microsecond"                           => Some(&datetime::microsecond::MicrosecondCommand),
        "minute"                                => Some(&datetime::minute::MinuteCommand),
        "month"                                 => Some(&datetime::month::MonthCommand),
        "now"                                   => Some(&datetime::now::NowCommand),
        "period_add"                            => Some(&datetime::period_add::PeriodAddCommand),
        "period_diff"                           => Some(&datetime::period_diff::PeriodDiffCommand),
        "quarter"                               => Some(&datetime::quarter::QuarterCommand),
        "sec_to_time"                           => Some(&datetime::sec_to_time::SecToTimeCommand),
        "second"                                => Some(&datetime::second::SecondCommand),
        "str_to_date"                           => Some(&datetime::str_to_date::StrToDateCommand),
        "subdate"                               => Some(&datetime::subdate::SubDateCommand),
        "date_sub"                              => Some(&datetime::subdate::SubDateCommand),
        "subtime"                               => Some(&datetime::subtime::SubTimeCommand),
        "sysdate"                               => Some(&datetime::sysdate::SysDateCommand),
        "time_format"                           => Some(&datetime::time_format::TimeFormatCommand),
        "time_to_sec"                           => Some(&datetime::time_to_sec::TimeToSecCommand),
        "time"                                  => Some(&datetime::time::TimeCommand),
        "timediff"                              => Some(&datetime::timediff::TimeDiffCommand),
        "timestamp"                             => Some(&datetime::timestamp::TimestampCommand),
        "to_days"                               => Some(&datetime::to_days::ToDaysCommand),
        "unixtimestamp" | "unix_timestamp"      => Some(&datetime::unixtimestamp::UnixTimestampCommand),
        "week"                                  => Some(&datetime::week::WeekCommand),
        "weekday"                               => Some(&datetime::weekday::WeekdayCommand),
        "weekofyear"                            => Some(&datetime::weekofyear::WeekOfYearCommand),
        "year"                                  => Some(&datetime::year::YearCommand),
        "yearweek"                              => Some(&datetime::yearweek::YearWeekCommand),

        // numeric functions (many of which also work as aggregate functions)

        "abs"                                   => Some(&numeric::abs::AbsCommand),
        "acos"                                  => Some(&numeric::acos::AcosCommand),
        "asin"                                  => Some(&numeric::asin::AsinCommand),
        "atan"                                  => Some(&numeric::atan::AtanCommand),
        "atan2"                                 => Some(&numeric::atan2::Atan2Command),
        "avg"                                   => Some(&numeric::avg::AvgCommand),
        "ceil" | "ceiling"                      => Some(&numeric::ceil::CeilCommand),
        "cos"                                   => Some(&numeric::cos::CosCommand),
        "count"                                 => Some(&numeric::count::CountCommand),
        "cot"                                   => Some(&numeric::cot::CotCommand),
        "degrees"                               => Some(&numeric::degrees::DegreesCommand),
        "div"                                   => Some(&numeric::div::DivCommand),
        "exp"                                   => Some(&numeric::exp::ExpCommand),
        "floor"                                 => Some(&numeric::floor::FloorCommand),
        "greatest"                              => Some(&numeric::greatest::GreatestCommand),
        "least"                                 => Some(&numeric::least::LeastCommand),
        "ln"                                    => Some(&numeric::ln::LnCommand),
        "log"                                   => Some(&numeric::log::LogCommand),
        "log10"                                 => Some(&numeric::log10::Log10Command),
        "log2"                                  => Some(&numeric::log2::Log2Command),
        "max"                                   => Some(&numeric::max::MaxCommand),
        "min"                                   => Some(&numeric::min::MinCommand),
        "mod"                                   => Some(&numeric::modulo::ModuloCommand),
        "pi"                                    => Some(&numeric::pi::PiCommand),
        "pow" | "power"                         => Some(&numeric::pow::PowCommand),
        "radians"                               => Some(&numeric::radians::RadiansCommand),
        "rand"                                  => Some(&numeric::rand::RandCommand),
        "round"                                 => Some(&numeric::round::RoundCommand),
        "sign"                                  => Some(&numeric::sign::SignCommand),
        "sin"                                   => Some(&numeric::sin::SinCommand),
        "sqrt"                                  => Some(&numeric::sqrt::SqrtCommand),
        "sum"                                   => Some(&numeric::sum::SumCommand),
        "tan"                                   => Some(&numeric::tan::TanCommand),
        "truncate"                              => Some(&numeric::truncate::TruncateCommand),

        // advanced functions

        "bin"                                   => Some(&advanced::bin::BinCommand),
        "binary"                                => Some(&advanced::binary::BinaryCommand),
        "case"                                  => Some(&advanced::case::CaseCommand),
        "cast"                                  => Some(&advanced::cast::CastCommand),
        "coalesce"                              => Some(&advanced::coalesce::CoalesceCommand),
        "connection_id"                         => Some(&advanced::connection_id::ConnectionIdCommand),
        "conv"                                  => Some(&advanced::conv::ConvCommand),
        "convert"                               => Some(&advanced::convert::ConvertCommand),
        "current_user"                          => Some(&advanced::current_user::CurrentUserCommand),
        "database"                              => Some(&advanced::database::DatabaseCommand),
        "if"                                    => Some(&advanced::ifcommand::IfCommand),
        "ifnull"                                => Some(&advanced::ifnull::IfNullCommand),
        "isnull"                                => Some(&advanced::isnull::IsNullCommand),
        "last_insert_id"                        => Some(&advanced::last_insert_id::LastInsertIdCommand),
        "nullif"                                => Some(&advanced::nullif::NullIfCommand),
        "session_user"                          => Some(&advanced::session_user::SessionUserCommand),
        "system_user"                           => Some(&advanced::system_user::SystemUserCommand),
        "user"                                  => Some(&advanced::user::UserCommand),
        "version"                               => Some(&advanced::version::VersionCommand),

        // custom non-MySQL functions

        "lookup"                                => Some(&custom::lookup::LookupCommand),

        _ => None,

    }

}

fn normalize_name(function_name: &str) -> Cow<'_, str> {

    let needs_strip_quotes = function_name
        .as_bytes()
        .iter()
        .any(|byte| *byte == b'`' || *byte == b'"');

    let needs_lowercase = function_name
        .as_bytes()
        .iter()
        .any(|byte| byte.is_ascii_uppercase());

    if !needs_strip_quotes && !needs_lowercase {
        return Cow::Borrowed(function_name);
    }

    let mut normalized = String::with_capacity(function_name.len());

    for ch in function_name.chars() {
        if ch == '`' || ch == '"' {
            continue;
        }
        normalized.push(ch.to_ascii_lowercase());
    }

    Cow::Owned(normalized)

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

#[cfg(test)]
#[path = "indexer_test.rs"]
mod tests;
