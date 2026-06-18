use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, extract_day_of_week, number_result};

pub struct DayOfWeekCommand;

// returns the day of the week for a date

impl InbuiltServerCommand for DayOfWeekCommand {

    fn name(&self) -> &'static str {
        "DAYOFWEEK"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(extract_day_of_week(&value).and_then(number_result))
        
    }

}
