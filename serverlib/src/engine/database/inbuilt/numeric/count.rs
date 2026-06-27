use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{collect_numeric_args, expect_arg_count, number_result};

pub struct CountCommand;

// Counts non-NULL function arguments only.
// SELECT COUNT(*) row aggregation is handled in select execution, not here.

impl InbuiltServerCommand for CountCommand {

    fn name(&self) -> &'static str {
        "COUNT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, usize::MAX, self.name())?;

        let count = collect_numeric_args(args)?
			.into_iter()
			.filter(|value| value.is_some())
			.count();

        Ok(number_result(count))
        
    }

}
