use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_string_arg, expect_arg_count, string_result, trim_spaces};

pub struct RtrimCommand;

// returns the right-trimmed version of a string

impl InbuiltServerCommand for RtrimCommand {

    fn name(&self) -> &'static str {
        "RTRIM"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(string_result(trim_spaces(&value, false, true)))
        
    }

}
