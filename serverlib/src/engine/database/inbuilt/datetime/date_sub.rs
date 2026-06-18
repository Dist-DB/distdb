use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, evaluate_i64_arg, evaluate_string_arg, sub_days_from_value};

pub struct DateSubCommand;

// returns the result of subtracting a time interval from a date

impl InbuiltServerCommand for DateSubCommand {

    fn name(&self) -> &'static str {
        "DATE_SUB"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(days) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(sub_days_from_value(&value, days).map(|result| result.into_bytes()))
        
    }

}
