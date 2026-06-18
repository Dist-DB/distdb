use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{char_count, evaluate_string_arg, expect_arg_count, number_result};

pub struct CharLengthCommand;

// returns the character length for the string

impl InbuiltServerCommand for CharLengthCommand {

    fn name(&self) -> &'static str {
        "CHAR_LENGTH"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(number_result(char_count(&value)))
        
    }

}
