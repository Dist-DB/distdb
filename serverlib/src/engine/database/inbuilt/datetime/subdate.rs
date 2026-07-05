use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, sub_days_from_value};

pub struct SubDateCommand;

// returns the result of subtracting a date or datetime value from another date or datetime value

impl InbuiltServerCommand for SubDateCommand {

    fn name(&self) -> &'static str {
        "SUBDATE"
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
