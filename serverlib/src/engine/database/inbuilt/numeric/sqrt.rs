use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct SqrtCommand;

// returns the square root of the given number

impl InbuiltServerCommand for SqrtCommand {

    fn name(&self) -> &'static str {
        "SQRT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        
        if value < 0.0 {
            return Ok(None);
        }

        Ok(float_result(value.sqrt()))
        
    }

}
