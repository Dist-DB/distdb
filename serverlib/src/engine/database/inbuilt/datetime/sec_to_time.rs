use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_i64_arg, expect_arg_count, number_result, time_from_seconds};

pub struct SecToTimeCommand;

// returns the result of converting seconds to time

impl InbuiltServerCommand for SecToTimeCommand {

    fn name(&self) -> &'static str {
        "SEC_TO_TIME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(seconds) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(time_from_seconds(seconds).and_then(number_result))
        
    }

}
