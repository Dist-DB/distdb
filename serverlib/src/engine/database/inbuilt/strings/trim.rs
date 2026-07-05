use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_string_arg, expect_arg_count, string_result, trim_exact, trim_spaces};

pub struct TrimCommand;

// returns the trimmed version of a string

impl InbuiltServerCommand for TrimCommand {

    fn name(&self) -> &'static str {
        "TRIM"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 2, self.name())?;

        let trimmed = if args.len() == 1 {
            
            let Some(value) = evaluate_string_arg(args, 0)? else {
                return Ok(None);
            };
            
            trim_spaces(&value, true, true)

        } else {
            
            let Some(pattern) = evaluate_string_arg(args, 0)? else {
                return Ok(None);
            };

            let Some(value) = evaluate_string_arg(args, 1)? else {
                return Ok(None);
            };

            trim_exact(&value, &pattern, true, true)
            
        };

        Ok(string_result(trimmed))
        
    }

}
