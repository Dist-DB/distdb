use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, extract_second, number_result};

pub struct SecondCommand;

// returns the second of the minute for a given time

impl InbuiltServerCommand for SecondCommand {

    fn name(&self) -> &'static str {
        "SECOND"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(extract_second(&value).and_then(number_result))
        
    }

}
