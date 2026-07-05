use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_i64_arg, expect_arg_count, number_result, period_add};

pub struct PeriodAddCommand;

// returns the result of adding a period to a date or time

impl InbuiltServerCommand for PeriodAddCommand {

    fn name(&self) -> &'static str {
        "PERIOD_ADD"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(period) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(months) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(period_add(period, months).and_then(number_result))
        
    }

}
