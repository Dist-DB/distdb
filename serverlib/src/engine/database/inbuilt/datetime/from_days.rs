use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{date_from_mysql_days, evaluate_i64_arg, expect_arg_count};

pub struct FromDaysCommand;

// returns the date from the number of days since year 0

impl InbuiltServerCommand for FromDaysCommand {

    fn name(&self) -> &'static str {
        "FROM_DAYS"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(days) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(date_from_mysql_days(days).map(|date| date.to_string().into_bytes()))
        
    }

}
