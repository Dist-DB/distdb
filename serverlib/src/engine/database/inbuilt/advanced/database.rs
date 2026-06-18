use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, runtime_database, string_result};

pub struct DatabaseCommand;

// returns the current database

impl InbuiltServerCommand for DatabaseCommand {

    fn name(&self) -> &'static str {
        "DATABASE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 0, self.name())?;

        Ok(runtime_database().and_then(string_result))

    }

}

