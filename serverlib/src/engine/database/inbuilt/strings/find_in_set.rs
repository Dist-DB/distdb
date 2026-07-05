use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_string_arg, expect_arg_count, number_result};

pub struct FindInSetCommand;

impl InbuiltServerCommand for FindInSetCommand {

    fn name(&self) -> &'static str {
        "FIND_IN_SET"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(needle) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        let Some(haystack) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        if needle.contains(',') || haystack.is_empty() {
            return Ok(number_result(0));
        }

        let normalized_needle = needle.to_lowercase();
        
        let index = haystack
            .split(',')
            .position(|value| value.to_lowercase() == normalized_needle)
            .map(|value| value + 1)
            .unwrap_or(0);

        Ok(number_result(index))
        
    }

}
