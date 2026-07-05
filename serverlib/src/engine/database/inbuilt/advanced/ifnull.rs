use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_bytes_arg, expect_arg_count};

pub struct IfNullCommand;

// returns the result of an IFNULL expression

impl InbuiltServerCommand for IfNullCommand {

    fn name(&self) -> &'static str {
        "IFNULL"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let value = evaluate_bytes_arg(args, 0)?;
        
        if value.is_some() {
            return Ok(value);
        }

        evaluate_bytes_arg(args, 1)

    }

}

