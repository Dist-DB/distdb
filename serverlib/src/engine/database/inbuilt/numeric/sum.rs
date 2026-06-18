use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{collect_numeric_args, expect_arg_count, float_result};

pub struct SumCommand;

// returns the sum of the given numbers

impl InbuiltServerCommand for SumCommand {

    fn name(&self) -> &'static str {
        "SUM"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, usize::MAX, self.name())?;

        let values = collect_numeric_args(args)?
			.into_iter()
			.flatten()
			.collect::<Vec<_>>();
        
		if values.is_empty() {
			return Ok(None);
		}

        Ok(float_result(values.iter().sum::<f64>()))
        
    }

}
