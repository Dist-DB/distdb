use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, evaluate_string_arg, parse_datetime, parse_date, to_date_string};

pub struct DateCommand;

// returns the current date

impl InbuiltServerCommand for DateCommand {

    fn name(&self) -> &'static str {
        "DATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(parse_datetime(&value)
            .map(|datetime| datetime.date().format("%Y-%m-%d").to_string())
            .or_else(|| parse_date(&value).map(|date| date.format("%Y-%m-%d").to_string()))
            .map(|result| result.into_bytes()))
        
    }

}
