use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{expect_arg_count, evaluate_string_arg, format_date_with_mysql_pattern, parse_date};

pub struct DateFormatCommand;

// returns the formatted date

impl InbuiltServerCommand for DateFormatCommand {

    fn name(&self) -> &'static str {
        "DATE_FORMAT"
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

        Ok(parse_date(&value).map(|date| format_date_with_mysql_pattern(&date, &format).into_bytes()))
        
    }

}
