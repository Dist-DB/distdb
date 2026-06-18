use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, string_result, substring_mysql};

pub struct SubstrCommand;

// returns a substring of a string

impl InbuiltServerCommand for SubstrCommand {

    fn name(&self) -> &'static str {
        "SUBSTR"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 3, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(position) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        let length = if args.len() == 3 {
            let Some(length) = evaluate_i64_arg(args, 2)? else {
                return Ok(None);
            };
            Some(length)
        } else {
            None
        };

        Ok(string_result(substring_mysql(&value, position, length)))
        
    }

}
