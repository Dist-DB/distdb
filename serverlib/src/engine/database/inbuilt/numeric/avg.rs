use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{collect_numeric_args, expect_arg_count, float_result};

pub struct AvgCommand;

// returns the average of the numbers

impl InbuiltServerCommand for AvgCommand {

    fn name(&self) -> &'static str {
        "AVG"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, usize::MAX, self.name())?;

        let values = collect_numeric_args(args)?;
        let numbers = values.into_iter().flatten().collect::<Vec<_>>();
        
        if numbers.is_empty() {
            return Ok(None);
        }

        let total = numbers.iter().sum::<f64>();

        Ok(float_result(total / numbers.len() as f64))
        
    }

}
