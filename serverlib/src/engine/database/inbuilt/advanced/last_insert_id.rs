use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{evaluate_bytes_arg, expect_arg_count, number_result, runtime_last_insert_id};

pub struct LastInsertIdCommand;

// returns the last inserted ID

impl InbuiltServerCommand for LastInsertIdCommand {

    fn name(&self) -> &'static str {
        "LAST_INSERT_ID"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 1, self.name())?;

        if args.is_empty() {
            return Ok(number_result(runtime_last_insert_id()));
        }

        evaluate_bytes_arg(args, 0)

    }

}
