use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{date_difference_days, evaluate_string_arg, expect_arg_count, number_result};

pub struct DateDiffCommand;

// returns the difference between two dates

impl InbuiltServerCommand for DateDiffCommand {

    fn name(&self) -> &'static str {
        "DATEDIFF"
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

        Ok(date_difference_days(&left, &right).and_then(number_result))
        
    }

}
