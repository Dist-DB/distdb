use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_string_arg, expect_arg_count, find_substring_position, number_result};

pub struct PositionCommand;

// returns the position of a substring within a string

impl InbuiltServerCommand for PositionCommand {

    fn name(&self) -> &'static str {
        "POSITION"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(needle) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(value) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(number_result(find_substring_position(&value, &needle, 1)))
        
    }

}
