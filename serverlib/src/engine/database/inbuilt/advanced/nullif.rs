use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_bytes_arg, expect_arg_count};

pub struct NullIfCommand;

// returns the result of a NULLIF expression

impl InbuiltServerCommand for NullIfCommand {

    fn name(&self) -> &'static str {
        "NULLIF"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let left = evaluate_bytes_arg(args, 0)?;
        let right = evaluate_bytes_arg(args, 1)?;

        match left {
            None => Ok(None),
            Some(left_value) => {
                if let Some(right_value) = right {
                    if left_value == right_value {
                        return Ok(None);
                    }
                }

                Ok(Some(left_value))
            }
        }

    }

}
