use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct CotCommand;

// returns the cotangent of the number

impl InbuiltServerCommand for CotCommand {

    fn name(&self) -> &'static str {
        "COT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };

        let tan = value.tan();
        
        if tan == 0.0 {
            return Ok(None);
        }

        Ok(float_result(1.0 / tan))
        
    }

}
