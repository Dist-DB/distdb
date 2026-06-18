use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, number_result, runtime_connection_id};

pub struct ConnectionIdCommand;

// returns the connection ID

impl InbuiltServerCommand for ConnectionIdCommand {

    fn name(&self) -> &'static str {
        "CONNECTION_ID"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 0, self.name())?;

        Ok(number_result(runtime_connection_id()))

    }

}

