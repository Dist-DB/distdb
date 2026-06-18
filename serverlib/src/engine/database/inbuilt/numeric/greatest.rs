use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{collect_numeric_args, expect_arg_count, float_result};

pub struct GreatestCommand;

// returns the greatest value among the arguments

impl InbuiltServerCommand for GreatestCommand {

    fn name(&self) -> &'static str {
        "GREATEST"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, usize::MAX, self.name())?;

        let values = collect_numeric_args(args)?;
		
        if values.iter().any(|value| value.is_none()) {
			return Ok(None);
		}

		let greatest = values
			.into_iter()
			.flatten()
			.reduce(f64::max)
			.expect("at least one argument should exist");

        Ok(float_result(greatest))
        
    }

}
