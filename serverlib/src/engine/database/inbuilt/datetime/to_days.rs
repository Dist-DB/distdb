use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;

use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{days_from_mysql_origin, evaluate_string_arg, expect_arg_count, parse_date, parse_datetime, number_result};

pub struct ToDaysCommand;

// returns the number of days since year 0

impl InbuiltServerCommand for ToDaysCommand {

    fn name(&self) -> &'static str {
        "TO_DAYS"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        let days = parse_datetime(&value)
			.map(|datetime| days_from_mysql_origin(datetime.date()))
			.or_else(|| parse_date(&value).map(days_from_mysql_origin));

        Ok(days.and_then(number_result))
        
    }

}
