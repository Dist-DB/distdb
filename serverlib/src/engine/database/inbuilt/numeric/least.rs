use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{collect_numeric_args, expect_arg_count, float_result};

pub struct LeastCommand;

// returns the least value among the arguments

impl InbuiltServerCommand for LeastCommand {

    fn name(&self) -> &'static str {
        "LEAST"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, usize::MAX, self.name())?;

        let values = collect_numeric_args(args)?;
		
        if values.iter().any(|value| value.is_none()) {
			return Ok(None);
		}

		let least = values
			.into_iter()
			.flatten()
			.reduce(f64::min)
			.expect("at least one argument should exist");

        Ok(float_result(least))
        
    }

}
