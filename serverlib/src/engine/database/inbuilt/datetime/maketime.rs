use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_i64_arg, expect_arg_count, make_time_string};

pub struct MakeTimeCommand;

// returns the time for the given hour, minute, and second

impl InbuiltServerCommand for MakeTimeCommand {

    fn name(&self) -> &'static str {
        "MAKETIME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 3, 3, self.name())?;

        let Some(hours) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(minutes) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        let Some(seconds) = evaluate_i64_arg(args, 2)? else {
            return Ok(None);
        };

        Ok(make_time_string(hours, minutes, seconds).map(|time| time.into_bytes()))
        
    }

}
