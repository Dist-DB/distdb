use sqlparser::ast::{Expr, Function, Value};

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::{function_argument_expr, function_args};

use super::helpers::{
    extract_day,
    extract_day_of_week,
    extract_day_of_year,
    extract_hour,
    extract_microsecond,
    extract_minute,
    extract_month,
    extract_quarter,
    extract_second,
    extract_week,
    extract_weekday,
    extract_year,
    extract_yearweek,
    evaluate_string_arg,
    expect_arg_count,
    number_result,
};

pub struct ExtractCommand;

// returns the extracted part of a date

impl InbuiltServerCommand for ExtractCommand {

    fn name(&self) -> &'static str {
        "EXTRACT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let unit_expr = function_argument_expr(&args[0])?;

        let unit = match unit_expr {

            Expr::Identifier(identifier) => identifier.value.to_ascii_lowercase(),

            Expr::Value(Value::SingleQuotedString(value))
            | Expr::Value(Value::DoubleQuotedString(value)) => value.trim().to_ascii_lowercase(),

            _ => return Err("EXTRACT requires a supported unit identifier".to_string()),
            
        };

        let Some(value) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        let result = match unit.as_str() {
            "year" => extract_year(&value),
            "month" => extract_month(&value),
            "day" | "dayofmonth" => extract_day(&value),
            "hour" => extract_hour(&value),
            "minute" => extract_minute(&value),
            "second" => extract_second(&value),
            "microsecond" => extract_microsecond(&value),
            "quarter" => extract_quarter(&value),
            "week" => extract_week(&value),
            "weekday" => extract_weekday(&value),
            "dayofweek" => extract_day_of_week(&value),
            "dayofyear" => extract_day_of_year(&value),
            "yearweek" => extract_yearweek(&value),
            _ => return Err(format!("unsupported EXTRACT unit '{}'", unit)),
        };

        Ok(result.and_then(number_result))
        
    }

}
