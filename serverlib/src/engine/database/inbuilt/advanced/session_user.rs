use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{expect_arg_count, runtime_session_user, string_result};

pub struct SessionUserCommand;

// returns the current session user

impl InbuiltServerCommand for SessionUserCommand {

    fn name(&self) -> &'static str {
        "SESSION_USER"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 0, self.name())?;

        Ok(string_result(runtime_session_user()))

    }

}

