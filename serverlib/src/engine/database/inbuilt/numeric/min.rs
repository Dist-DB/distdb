use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{collect_numeric_args, expect_arg_count, float_result};

pub struct MinCommand;

// returns the minimum value from the arguments

impl InbuiltServerCommand for MinCommand {

    fn name(&self) -> &'static str {
        "MIN"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, usize::MAX, self.name())?;

        let minimum = collect_numeric_args(args)?
			.into_iter()
			.flatten()
			.reduce(f64::min);

        Ok(minimum.and_then(float_result))
        
    }

}
