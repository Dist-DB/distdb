use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, extract_minute, number_result};

pub struct MinuteCommand;

// returns the minute part of the time

impl InbuiltServerCommand for MinuteCommand {

    fn name(&self) -> &'static str {
        "MINUTE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(extract_minute(&value).and_then(number_result))
        
    }

}
