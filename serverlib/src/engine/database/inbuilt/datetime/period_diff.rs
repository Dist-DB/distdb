use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_i64_arg, expect_arg_count, number_result, period_diff};

pub struct PeriodDiffCommand;

// returns the difference between two periods

impl InbuiltServerCommand for PeriodDiffCommand {

    fn name(&self) -> &'static str {
        "PERIOD_DIFF"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(end_period) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(start_period) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(period_diff(end_period, start_period).and_then(number_result))
        
    }

}
