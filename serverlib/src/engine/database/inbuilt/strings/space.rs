use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_i64_arg, expect_arg_count, string_result};

pub struct SpaceCommand;

// returns a string consisting of a specified number of spaces

impl InbuiltServerCommand for SpaceCommand {

    fn name(&self) -> &'static str {
        "SPACE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(count) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };

        let count = count.max(0) as usize;

        Ok(string_result(" ".repeat(count)))
        
    }

}
