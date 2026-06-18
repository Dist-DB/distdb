use chrono::Timelike;

use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{add_seconds_to_datetime, datetime_to_string, evaluate_string_arg, expect_arg_count, parse_datetime, parse_time, time_from_seconds, time_seconds_from_value};

pub struct AddTimeCommand;

// adds a specified time interval to a time

impl InbuiltServerCommand for AddTimeCommand {

    fn name(&self) -> &'static str {
        "ADDTIME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(left) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        let Some(right) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        let result = if let Some(datetime) = parse_datetime(&left) {

            time_seconds_from_value(&right)
                .and_then(|seconds| add_seconds_to_datetime(datetime, seconds))
                .map(datetime_to_string)

        } else if let Some(time) = parse_time(&left) {

            time_seconds_from_value(&right).and_then(|seconds| {
                let total = time.num_seconds_from_midnight() as i64 + seconds;
                time_from_seconds(total)
            })

        } else {
            
            Some(left)
            
        };

        Ok(result.map(|value| value.into_bytes()))
        
    }

}
