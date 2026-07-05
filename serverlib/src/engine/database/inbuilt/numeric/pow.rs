use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct PowCommand;

// returns the result of raising a number to a power

impl InbuiltServerCommand for PowCommand {

    fn name(&self) -> &'static str {
        "POW"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(base) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        let Some(exponent) = evaluate_f64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(float_result(base.powf(exponent)))
        
    }

}
