use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_f64_arg, evaluate_i64_arg, evaluate_string_arg, expect_arg_count, format_mysql_number, string_result};

pub struct FormatCommand;

impl InbuiltServerCommand for FormatCommand {

    fn name(&self) -> &'static str {
        "FORMAT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 3, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };

        let Some(decimals) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };
        
        let locale = if args.len() == 3 {
            let Some(locale) = evaluate_string_arg(args, 2)? else {
                return Ok(None);
            };
            Some(locale)
        } else {
            None
        };

        Ok(string_result(format_mysql_number(value, decimals, locale.as_deref())))
        
    }

}
