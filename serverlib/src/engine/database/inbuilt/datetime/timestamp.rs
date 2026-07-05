use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{datetime_to_string, evaluate_string_arg, expect_arg_count, parse_date, parse_datetime, parse_time};

pub struct TimestampCommand;

// returns the current timestamp

impl InbuiltServerCommand for TimestampCommand {

    fn name(&self) -> &'static str {
        "TIMESTAMP"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 2, self.name())?;

        if args.is_empty() {
            return Ok(Some(super::helpers::utc_now_datetime_string().into_bytes()));
        }

        let Some(first) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        if args.len() == 1 {
            let result = parse_datetime(&first)
                .map(datetime_to_string)
                .or_else(|| parse_date(&first).map(|date| datetime_to_string(date.and_hms_opt(0, 0, 0).expect("midnight is valid"))))
                .or_else(|| parse_time(&first).map(|time| datetime_to_string(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid epoch date").and_time(time))));
            return Ok(result.map(|value| value.into_bytes()));
        }

        let Some(second) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        let date = parse_datetime(&first)
            .map(|datetime| datetime.date())
            .or_else(|| parse_date(&first))
            .or_else(|| parse_time(&first).and_then(|_| chrono::NaiveDate::from_ymd_opt(1970, 1, 1)));
        
        let time = parse_datetime(&second)
            .map(|datetime| datetime.time())
            .or_else(|| parse_time(&second));

        Ok(match (date, time) {
            (Some(date), Some(time)) => Some(datetime_to_string(date.and_time(time)).into_bytes()),
            _ => None,
        })
        
    }

}
