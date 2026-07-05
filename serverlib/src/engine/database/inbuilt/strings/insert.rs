use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_i64_arg, evaluate_string_arg, expect_arg_count, insert_mysql, string_result};

pub struct InsertCommand;

impl InbuiltServerCommand for InsertCommand {

    fn name(&self) -> &'static str {
        "INSERT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 4, 4, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(position) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };
        
        let Some(length) = evaluate_i64_arg(args, 2)? else {
            return Ok(None);
        };

        let Some(new_value) = evaluate_string_arg(args, 3)? else {
            return Ok(None);
        };

        Ok(string_result(insert_mysql(&value, position, length, &new_value)))
        
    }

}
