use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, runtime_system_user, string_result};

pub struct SystemUserCommand;

// returns the current system user

impl InbuiltServerCommand for SystemUserCommand {

    fn name(&self) -> &'static str {
        "SYSTEM_USER"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 0, self.name())?;

        Ok(string_result(runtime_system_user()))

    }

}

