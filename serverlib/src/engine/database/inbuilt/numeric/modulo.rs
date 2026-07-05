use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct ModuloCommand;

// returns the result of the modular operation

impl InbuiltServerCommand for ModuloCommand {

    fn name(&self) -> &'static str {
        "MOD"
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

        Ok(float_result(lhs % rhs))
        
    }

}
