use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, last_day_of_month, string_result};

pub struct LastDayCommand;

// returns the last day of the month for a given date

impl InbuiltServerCommand for LastDayCommand {

    fn name(&self) -> &'static str {
        "LAST_DAY"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(last_day_of_month(&value).and_then(string_result))
        
    }

}
