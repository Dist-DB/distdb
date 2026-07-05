use chrono::Timelike;

use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{
    add_seconds_to_datetime,
    datetime_to_string,
    evaluate_string_arg,
    expect_arg_count,
    parse_datetime,
    parse_time,
    time_from_seconds,
    time_seconds_from_value,
};

pub struct SubTimeCommand;

// returns the result of subtracting a time value from another time value

impl InbuiltServerCommand for SubTimeCommand {

    fn name(&self) -> &'static str {
        "SUBTIME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(delta) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        let Some(seconds) = time_seconds_from_value(&delta) else {
            return Ok(None);
        };

        if let Some(datetime) = parse_datetime(&value) {
            return Ok(add_seconds_to_datetime(datetime, -seconds)
                .map(datetime_to_string)
                .map(|result| result.into_bytes()));
        }

        let Some(time) = parse_time(&value) else {
            return Ok(None);
        };

        let total_seconds = time.num_seconds_from_midnight() as i64 - seconds;
        
        Ok(time_from_seconds(total_seconds).map(|result| result.into_bytes()))
        
    }

}
