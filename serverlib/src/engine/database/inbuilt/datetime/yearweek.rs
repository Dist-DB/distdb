use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_string_arg, expect_arg_count, extract_yearweek, number_result};

pub struct YearWeekCommand;

// returns the year and week for a given date

impl InbuiltServerCommand for YearWeekCommand {

    fn name(&self) -> &'static str {
        "YEARWEEK"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 2, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(extract_yearweek(&value).and_then(number_result))
        
    }

}
