use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct Atan2Command;

// returns the arc tangent of the number

impl InbuiltServerCommand for Atan2Command {

    fn name(&self) -> &'static str {
        "ATAN2"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(y) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        let Some(x) = evaluate_f64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(float_result(y.atan2(x)))
        
    }

}
