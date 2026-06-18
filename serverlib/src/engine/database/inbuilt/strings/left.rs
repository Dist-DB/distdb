use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, left_chars, string_result};

pub struct LeftCommand;

// returns the leftmost characters of a string

impl InbuiltServerCommand for LeftCommand {

    fn name(&self) -> &'static str {
        "LEFT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(length) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(string_result(left_chars(&value, length)))
        
    }

}
