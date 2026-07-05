use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, string_result, substring_index_mysql};

pub struct SubstringIndexCommand;

// returns the substring index of a string

impl InbuiltServerCommand for SubstringIndexCommand {

    fn name(&self) -> &'static str {
        "SUBSTRING_INDEX"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 3, 3, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(delimiter) = evaluate_string_arg(args, 1)? else {
            return Ok(None);
        };

        let Some(count) = evaluate_i64_arg(args, 2)? else {
            return Ok(None);
        };

        Ok(string_result(substring_index_mysql(&value, &delimiter, count)))
        
    }

}
