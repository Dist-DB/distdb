use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_f64_arg, expect_arg_count, number_result};

pub struct SignCommand;

// returns the sign of the given number

impl InbuiltServerCommand for SignCommand {

    fn name(&self) -> &'static str {
        "SIGN"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        let sign = if value > 0.0 {
			1
		} else if value < 0.0 {
			-1
		} else {
			0
		};

        Ok(number_result(sign))
        
    }

}
