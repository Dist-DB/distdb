use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, str_to_date_with_mysql_pattern};

pub struct StrToDateCommand;

// returns the result of converting a string to a date

impl InbuiltServerCommand for StrToDateCommand {

    fn name(&self) -> &'static str {
        "STR_TO_DATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(pattern) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(str_to_date_with_mysql_pattern(&value, &pattern).map(|result| result.into_bytes()))
        
    }

}
