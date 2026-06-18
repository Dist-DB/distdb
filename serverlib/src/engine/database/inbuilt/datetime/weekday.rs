use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, extract_weekday, number_result};

pub struct WeekdayCommand;

// returns the weekday for a given date

impl InbuiltServerCommand for WeekdayCommand {

    fn name(&self) -> &'static str {
        "WEEKDAY"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(extract_weekday(&value).and_then(number_result))
        
    }

}
