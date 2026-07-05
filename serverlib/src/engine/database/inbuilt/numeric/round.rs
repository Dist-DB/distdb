use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, evaluate_i64_arg, expect_arg_count, float_result, round_mysql};

pub struct RoundCommand;

// returns the rounded value for the given number

impl InbuiltServerCommand for RoundCommand {

    fn name(&self) -> &'static str {
        "ROUND"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 2, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        let decimals = if args.len() == 2 {
			let Some(value) = evaluate_i64_arg(args, 1)? else {
				return Ok(None);
			};
			value
		} else {
			0
		};

        Ok(float_result(round_mysql(value, decimals)))
        
    }

}
