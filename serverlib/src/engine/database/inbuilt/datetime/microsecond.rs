use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, extract_microsecond, number_result};

pub struct MicrosecondCommand;

// returns the microsecond part of the time

impl InbuiltServerCommand for MicrosecondCommand {

    fn name(&self) -> &'static str {
        "MICROSECOND"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(extract_microsecond(&value).and_then(number_result))
        
    }

}
