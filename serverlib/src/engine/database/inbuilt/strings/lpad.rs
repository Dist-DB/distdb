use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, pad_mysql, string_result};

pub struct LpadCommand;

// returns the left-padded version of a string

impl InbuiltServerCommand for LpadCommand {

    fn name(&self) -> &'static str {
        "LPAD"
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

        Ok(pad_mysql(&value, target_length, &pad, true).map(|value| value.into_bytes()))
        
    }

}
