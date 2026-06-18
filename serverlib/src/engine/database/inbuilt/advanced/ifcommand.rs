use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_bytes_arg, expect_arg_count, is_truthy};

pub struct IfCommand;

// returns the result of an IF expression

impl InbuiltServerCommand for IfCommand {

    fn name(&self) -> &'static str {
        "IF"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 3, 3, self.name())?;

        let Some(condition) = evaluate_bytes_arg(args, 0)? else {
            return evaluate_bytes_arg(args, 2);
        };

        if is_truthy(&condition) {
            return evaluate_bytes_arg(args, 1);
        }

        evaluate_bytes_arg(args, 2)

    }

}

