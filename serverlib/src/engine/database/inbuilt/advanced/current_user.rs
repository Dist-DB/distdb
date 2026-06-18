use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_arg_count, runtime_current_user, string_result};

pub struct CurrentUserCommand;

// returns the current user

impl InbuiltServerCommand for CurrentUserCommand {

    fn name(&self) -> &'static str {
        "CURRENT_USER"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 0, self.name())?;

        Ok(string_result(runtime_current_user()))

    }

}

