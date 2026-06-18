use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_string_arg, expect_arg_count, find_substring_position, number_result};

pub struct InstrCommand;

// returns the position of the first occurrence of a substring

impl InbuiltServerCommand for InstrCommand {

    fn name(&self) -> &'static str {
        "INSTR"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(needle) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(number_result(find_substring_position(&value, &needle, 1)))
        
    }

}
