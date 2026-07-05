use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{evaluate_bytes_arg, expect_arg_count};

pub struct CastCommand;

// returns the result of a CAST expression

impl InbuiltServerCommand for CastCommand {

    fn name(&self) -> &'static str {
        "CAST"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        // CAST is normally represented as Expr::Cast by sqlparser, but this keeps function-style fallback working.
        expect_arg_count(args, 1, 2, self.name())?;

        evaluate_bytes_arg(args, 0)

    }

}

