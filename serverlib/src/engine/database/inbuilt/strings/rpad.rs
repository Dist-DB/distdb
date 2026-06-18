use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, pad_mysql};

pub struct RpadCommand;

// returns the right-padded version of a string

impl InbuiltServerCommand for RpadCommand {

    fn name(&self) -> &'static str {
        "RPAD"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 3, 3, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(target_length) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        let Some(pad) = evaluate_string_arg(args, 2)? else {
            return Ok(None);
        };

        Ok(pad_mysql(&value, target_length, &pad, false).map(|value| value.into_bytes()))
        
    }

}
