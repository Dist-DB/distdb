use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_bytes_arg, expect_arg_count};

pub struct ConvertCommand;

// returns the result of a CONVERT expression

impl InbuiltServerCommand for ConvertCommand {

    fn name(&self) -> &'static str {
        "CONVERT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        // CONVERT is usually parsed as dedicated AST variants; keep function-style fallback equivalent to passthrough.
        expect_arg_count(args, 1, 2, self.name())?;

        evaluate_bytes_arg(args, 0)

    }

}

