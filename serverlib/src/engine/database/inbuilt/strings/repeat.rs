use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, string_result};

pub struct RepeatCommand;

// returns the repeated version of a string

impl InbuiltServerCommand for RepeatCommand {

    fn name(&self) -> &'static str {
        "REPEAT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(count) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        if count <= 0 {
            return Ok(string_result(String::new()));
        }

        let repeated = value.repeat(count as usize);

        Ok(string_result(repeated))
        
    }

}
