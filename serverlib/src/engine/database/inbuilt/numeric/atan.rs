use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct AtanCommand;

// returns the arc tangent of the number

impl InbuiltServerCommand for AtanCommand {

    fn name(&self) -> &'static str {
        "ATAN"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(float_result(value.atan()))
        
    }

}
