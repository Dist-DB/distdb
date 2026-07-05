use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_i64_arg, expect_arg_count, string_result};

pub struct BinCommand;

// returns the binary representation of the number

impl InbuiltServerCommand for BinCommand {

    fn name(&self) -> &'static str {
        "BIN"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        let Some(value) = evaluate_i64_arg(args, 0)? else {
            return Ok(None);
        };

        Ok(string_result(format!("{:b}", value as u64)))

    }

}
