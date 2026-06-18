use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_bytes_arg, expect_arg_count, number_result};

pub struct IsNullCommand;

// returns the result of an ISNULL expression

impl InbuiltServerCommand for IsNullCommand {

    fn name(&self) -> &'static str {
        "ISNULL"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let value = evaluate_bytes_arg(args, 0)?;
        
        Ok(number_result(if value.is_none() { 1 } else { 0 }))

    }

}
