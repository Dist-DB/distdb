use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_bytes_arg, expect_arg_count};

pub struct BinaryCommand;

// returns the binary representation of the number

impl InbuiltServerCommand for BinaryCommand {

    fn name(&self) -> &'static str {
        "BINARY"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 1, self.name())?;

        evaluate_bytes_arg(args, 0)

    }

}
