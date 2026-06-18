use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_string_arg, expect_arg_count, string_result};

pub struct ConcatWCommand;

// returns the concatenated version of a string with a separator

impl InbuiltServerCommand for ConcatWCommand {

    fn name(&self) -> &'static str {
        "CONCAT_WS"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, usize::MAX, self.name())?;

        let Some(separator) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        let mut values = Vec::new();
        for index in 1..args.len() {
            if let Some(value) = evaluate_string_arg(args, index)? {
                values.push(value);
            }
        }

        Ok(string_result(values.join(&separator)))
        
    }

}
