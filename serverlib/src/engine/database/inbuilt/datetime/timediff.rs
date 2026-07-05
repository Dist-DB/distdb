use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_string_arg, expect_arg_count, number_result, time_difference_seconds};

pub struct TimeDiffCommand;

// returns the difference between two times

impl InbuiltServerCommand for TimeDiffCommand {

    fn name(&self) -> &'static str {
        "TIME_DIFF"
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

        Ok(time_difference_seconds(&left, &right).and_then(number_result))
        
    }

}
