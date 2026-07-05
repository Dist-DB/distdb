use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, find_substring_position, number_result};

pub struct LocateCommand;

// returns the position of the first occurrence of a substring

impl InbuiltServerCommand for LocateCommand {

    fn name(&self) -> &'static str {
        "LOCATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 3, self.name())?;

        let Some(needle) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(value) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        let start = if args.len() == 3 {
            let Some(start) = evaluate_i64_arg(args, 2)? else {
                return Ok(None);
            };
            start
        } else {
            1
        };

        Ok(number_result(find_substring_position(&value, &needle, start)))
        
    }

}
