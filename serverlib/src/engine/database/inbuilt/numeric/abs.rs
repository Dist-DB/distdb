use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct AbsCommand;

// returns the absolute value of the number

impl InbuiltServerCommand for AbsCommand {

    fn name(&self) -> &'static str {
        "ABS"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(float_result(value.abs()))
        
    }

}
