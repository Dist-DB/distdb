use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_zero_args, utc_now_time_string};

pub struct CurrentTimeCommand;

// returns the current time

impl InbuiltServerCommand for CurrentTimeCommand {

    fn name(&self) -> &'static str {
        "CURRENT_TIME"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_zero_args(self.name(), args)?;

        Ok(Some(utc_now_time_string().into_bytes()))
        
    }

}
