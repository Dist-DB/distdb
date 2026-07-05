use chrono::Timelike;

use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_string_arg, expect_arg_count, number_result, parse_datetime, parse_time};

pub struct TimeToSecCommand;

// returns the number of seconds since '00:00:00'

impl InbuiltServerCommand for TimeToSecCommand {

    fn name(&self) -> &'static str {
        "TIME_TO_SEC"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        let seconds = parse_datetime(&value)
			.map(|datetime| datetime.time().num_seconds_from_midnight() as i64)
			.or_else(|| parse_time(&value).map(|time| time.num_seconds_from_midnight() as i64));

        Ok(seconds.and_then(number_result))
        
    }

}
