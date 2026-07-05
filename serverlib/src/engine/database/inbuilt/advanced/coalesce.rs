use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::evaluate_bytes_arg;

pub struct CoalesceCommand;

// returns the result of a COALESCE expression

impl InbuiltServerCommand for CoalesceCommand {

    fn name(&self) -> &'static str {
        "COALESCE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        for index in 0..args.len() {
            if let Some(value) = evaluate_bytes_arg(args, index)? {
                return Ok(Some(value));
            }
        }

        Ok(None)

    }

}

