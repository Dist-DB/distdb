use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, evaluate_string_arg, format_time_with_mysql_pattern, parse_time};

pub struct TimeFormatCommand;

// returns the formatted time

impl InbuiltServerCommand for TimeFormatCommand {

    fn name(&self) -> &'static str {
        "TIME_FORMAT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(format) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(parse_time(&value).map(|time| format_time_with_mysql_pattern(&time, &format).into_bytes()))
        
    }

}
