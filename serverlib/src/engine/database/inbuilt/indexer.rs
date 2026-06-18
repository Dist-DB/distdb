use sqlparser::ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, Value};

use super::command::InbuiltServerCommand;

use super::strings;
use super::datetime;
use super::numeric;
use super::advanced;

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

    match normalized.as_str() {
        
        // string functions
        
        "unixtimestamp" | "unix_timestamp" => Some(&UnixTimestampCommand),
        "ascii" => Some(&strings::ascii::AsciiCommand),
        "char_length" | "character_length" => Some(&strings::char_length::CharLengthCommand),
        "concat" => Some(&strings::concat::ConcatCommand),
        "concat_w" => Some(&strings::concat_w::ConcatWCommand),
        "field" => Some(&strings::field::FieldCommand),
        "find_in_set" => Some(&strings::find_in_set::FindInSetCommand),
        "format" => Some(&strings::format::FormatCommand),
        "insert" => Some(&strings::insert::InsertCommand),
        "instr" => Some(&strings::instr::InstrCommand),
        "left" => Some(&strings::left::LeftCommand),
        "length" => Some(&strings::length::LengthCommand),
        "locate" => Some(&strings::locate::LocateCommand),
        "lpad" => Some(&strings::lpad::LpadCommand),
        "rpad" => Some(&strings::rpad::RpadCommand),
        "ltrim" => Some(&strings::ltrim::LtrimCommand),
        "rtrim" => Some(&strings::rtrim::RtrimCommand),
        "space" => Some(&strings::space::SpaceCommand),
        "substr" | "substring" => Some(&strings::substr::SubstrCommand),
        "upper" | "ucase" => Some(&strings::upper::UpperCommand),
        "lower" | "lcase" => Some(&strings::lower::LowerCommand),
        "substring_index" => Some(&strings::substring_index::SubstringIndexCommand),
        "trim" => Some(&strings::trim::TrimCommand),        
        
        // date and time functions
        
        "adddate" => Some(&datetime::adddate::AddDateCommand),
        "addtime" => Some(&datetime::addtime::AddTimeCommand),
        "curdate" => Some(&datetime::curdate::CurDateCommand),
        "curtime" => Some(&datetime::curtime::CurTimeCommand),
        "date" => Some(&datetime::date::DateCommand),
        "datediff" => Some(&datetime::datediff::DateDiffCommand),
        "date_format" => Some(&datetime::date_format::DateFormatCommand),
        "day" => Some(&datetime::day::DayCommand),
        "dayname" => Some(&datetime::dayname::DayNameCommand),
        "dayofmonth" => Some(&datetime::dayofmonth::DayOfMonthCommand),
        "dayofweek" => Some(&datetime::dayofweek::DayOfWeekCommand),
        "dayofyear" => Some(&datetime::dayofyear::DayOfYearCommand),
        "extract" => Some(&datetime::extract::ExtractCommand),
        "from_days" => Some(&datetime::from_days::FromDaysCommand),
        "hour" => Some(&datetime::hour::HourCommand),
        "last_day" => Some(&datetime::last_day::LastDayCommand),
        "localtime" => Some(&datetime::localtime::LocalTimeCommand),
        "localtimestamp" => Some(&datetime::localtimestamp::LocalTimestampCommand),
        "makedate" => Some(&datetime::makedate::MakeDateCommand),
        "maketime" => Some(&datetime::maketime::MakeTimeCommand),
        "microsecond" => Some(&datetime::microsecond::MicrosecondCommand),
        "minute" => Some(&datetime::minute::MinuteCommand),
        "month" => Some(&datetime::month::MonthCommand),
        "now" => Some(&datetime::now::NowCommand),
        "period_add" => Some(&datetime::period_add::PeriodAddCommand),
        "period_diff" => Some(&datetime::period_diff::PeriodDiffCommand),
        "quarter" => Some(&datetime::quarter::QuarterCommand),
        "sec_to_time" => Some(&datetime::sec_to_time::SecToTimeCommand),
        "second" => Some(&datetime::second::SecondCommand),
        "str_to_date" => Some(&datetime::str_to_date::StrToDateCommand),
        "subdate" => Some(&datetime::subdate::SubDateCommand),
        "subtime" => Some(&datetime::subtime::SubTimeCommand),
        "sysdate" => Some(&datetime::sysdate::SysDateCommand),
        "time_format" => Some(&datetime::time_format::TimeFormatCommand),
        "time_to_sec" => Some(&datetime::time_to_sec::TimeToSecCommand),
        "time" => Some(&datetime::time::TimeCommand),
        "timediff" => Some(&datetime::timediff::TimeDiffCommand),
        "timestamp" => Some(&datetime::timestamp::TimestampCommand),
        "to_days" => Some(&datetime::to_days::ToDaysCommand),
        "week" => Some(&datetime::week::WeekCommand),
        "weekday" => Some(&datetime::weekday::WeekdayCommand),
        "weekofyear" => Some(&datetime::weekofyear::WeekOfYearCommand),
        "year" => Some(&datetime::year::YearCommand),
        "yearweek" => Some(&datetime::yearweek::YearWeekCommand),

        // numeric functions

        "abs" => Some(&numeric::abs::AbsCommand),
        "acos" => Some(&numeric::acos::AcosCommand),
        "asin" => Some(&numeric::asin::AsinCommand),
        "atan" => Some(&numeric::atan::AtanCommand),
        "atan2" => Some(&numeric::atan2::Atan2Command),
        "avg" => Some(&numeric::avg::AvgCommand),
        "ceil" | "ceiling" => Some(&numeric::ceil::CeilCommand),
        "cos" => Some(&numeric::cos::CosCommand),
        "cot" => Some(&numeric::cot::CotCommand),
        "degrees" => Some(&numeric::degrees::DegreesCommand),
        "div" => Some(&numeric::div::DivCommand),
        "exp" => Some(&numeric::exp::ExpCommand),
        "floor" => Some(&numeric::floor::FloorCommand),
        "greatest" => Some(&numeric::greatest::GreatestCommand),
        "least" => Some(&numeric::least::LeastCommand),
        "ln" => Some(&numeric::ln::LnCommand),
        "log" => Some(&numeric::log::LogCommand),
        "log10" => Some(&numeric::log10::Log10Command),
        "log2" => Some(&numeric::log2::Log2Command),
        "mod" => Some(&numeric::modulo::ModuloCommand),
        "pi" => Some(&numeric::pi::PiCommand),
        "pow" | "power" => Some(&numeric::pow::PowCommand),
        "radians" => Some(&numeric::radians::RadiansCommand),
        "rand" => Some(&numeric::rand::RandCommand),
        "round" => Some(&numeric::round::RoundCommand),
        "sign" => Some(&numeric::sign::SignCommand),
        "sin" => Some(&numeric::sin::SinCommand),
        "sqrt" => Some(&numeric::sqrt::SqrtCommand),
        "sum" => Some(&numeric::sum::SumCommand),
        "tan" => Some(&numeric::tan::TanCommand),
        "truncate" => Some(&numeric::truncate::TruncateCommand),

        // advanced functions

        "bin" => Some(&advanced::bin::BinCommand),
        "binary" => Some(&advanced::binary::BinaryCommand),
        "case" => Some(&advanced::case::CaseCommand),
        "cast" => Some(&advanced::cast::CastCommand),
        "coalesce" => Some(&advanced::coalesce::CoalesceCommand),
        "connection_id" => Some(&advanced::connection_id::ConnectionIdCommand),
        "conv" => Some(&advanced::conv::ConvCommand),
        "convert" => Some(&advanced::convert::ConvertCommand),
        "current_user" => Some(&advanced::current_user::CurrentUserCommand),
        "database" => Some(&advanced::database::DatabaseCommand),
        "if" => Some(&advanced::ifcommand::IfCommand),
        "ifnull" => Some(&advanced::ifnull::IfNullCommand),
        "isnull" => Some(&advanced::isnull::IsNullCommand),
        "last_insert_id" => Some(&advanced::last_insert_id::LastInsertIdCommand),
        "nullif" => Some(&advanced::nullif::NullIfCommand),
        "session_user" => Some(&advanced::session_user::SessionUserCommand),
        "system_user" => Some(&advanced::system_user::SystemUserCommand),
        "user" => Some(&advanced::user::UserCommand),
        "version" => Some(&advanced::version::VersionCommand),

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
