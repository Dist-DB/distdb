use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct Log10Command;

// returns the base-10 logarithm of the number

impl InbuiltServerCommand for Log10Command {

    fn name(&self) -> &'static str {
        "LOG10"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        
        if value <= 0.0 {
            return Ok(None);
        }

        Ok(float_result(value.log10()))
        
    }

}
