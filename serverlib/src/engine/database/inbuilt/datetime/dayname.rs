use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{day_name, evaluate_string_arg, expect_arg_count, string_result};

pub struct DayNameCommand;

// returns the name of the day for a date

impl InbuiltServerCommand for DayNameCommand {

    fn name(&self) -> &'static str {
        "DAYNAME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(day_name(&value).and_then(string_result))
        
    }

}
