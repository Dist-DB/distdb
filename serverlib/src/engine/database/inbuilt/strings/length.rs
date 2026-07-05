use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_bytes_arg, expect_arg_count, number_result};

pub struct LengthCommand;

// returns the length of a string

impl InbuiltServerCommand for LengthCommand {

    fn name(&self) -> &'static str {
        "LENGTH"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_bytes_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(number_result(value.len()))
        
    }

}
