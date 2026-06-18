use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_f64_arg, expect_arg_count, number_result};

pub struct DivCommand;

// returns the division of the numbers

impl InbuiltServerCommand for DivCommand {

    fn name(&self) -> &'static str {
        "DIV"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(lhs) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };

        let Some(rhs) = evaluate_f64_arg(args, 1)? else {
            return Ok(None);
        };
        
        if rhs == 0.0 {
            return Ok(None);
        }

        Ok(number_result((lhs / rhs).trunc() as i64))
        
    }

}
