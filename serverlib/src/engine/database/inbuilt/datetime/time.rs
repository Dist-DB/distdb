use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_string_arg, expect_arg_count, parse_date, parse_datetime, parse_time, time_to_string};

pub struct TimeCommand;

// returns the current time

impl InbuiltServerCommand for TimeCommand {

    fn name(&self) -> &'static str {
        "TIME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        let result = parse_datetime(&value)
			.map(|datetime| time_to_string(datetime.time()))
			.or_else(|| parse_time(&value).map(time_to_string))
			.or_else(|| parse_date(&value).map(|_| "00:00:00".to_string()));

        Ok(result.map(|result| result.into_bytes()))
        
    }

}
