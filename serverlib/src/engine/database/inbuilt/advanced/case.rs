use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_bytes_arg, expect_arg_count, is_truthy};

pub struct CaseCommand;

// returns the result of a CASE expression

impl InbuiltServerCommand for CaseCommand {

    fn name(&self) -> &'static str {
        "CASE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, usize::MAX, self.name())?;

        let mut index = 0;

        while index + 1 < args.len() {

            let Some(condition) = evaluate_bytes_arg(args, index)? else {
                index += 2;
                continue;
            };

            if is_truthy(&condition) {
                return evaluate_bytes_arg(args, index + 1);
            }

            index += 2;
            
        }

        if args.len() % 2 == 1 {
            return evaluate_bytes_arg(args, args.len() - 1);
        }

        Ok(None)

    }

}

