use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_string_arg, expect_arg_count, number_result};

pub struct FieldCommand;

impl InbuiltServerCommand for FieldCommand {

    fn name(&self) -> &'static str {
        "FIELD"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, usize::MAX, self.name())?;

        let Some(needle) = evaluate_string_arg(args, 0)? else {
            return Ok(number_result(0));
        };

        let normalized_needle = needle.to_lowercase();

        for index in 1..args.len() {
            if let Some(value) = evaluate_string_arg(args, index)?
                && value.to_lowercase() == normalized_needle {
                    return Ok(number_result(index));
                }
        }

        Ok(number_result(0))
        
    }

}
