use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{date_from_year_and_day, evaluate_i64_arg, expect_arg_count};

pub struct MakeDateCommand;

// returns the date for the given year and day of year

impl InbuiltServerCommand for MakeDateCommand {

    fn name(&self) -> &'static str {
        "MAKEDATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(year) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(day_of_year) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(date_from_year_and_day(year, day_of_year).map(|date| date.into_bytes()))
        
    }

}
