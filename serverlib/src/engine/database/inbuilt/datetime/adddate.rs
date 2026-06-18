use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{add_days_to_value, expect_arg_count, evaluate_i64_arg, evaluate_string_arg};

pub struct AddDateCommand;

// adds a specified time interval to a date

impl InbuiltServerCommand for AddDateCommand {

    fn name(&self) -> &'static str {
        "ADDDATE"
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

        Ok(add_days_to_value(&value, days).map(|result| result.into_bytes()))
        
    }

}
