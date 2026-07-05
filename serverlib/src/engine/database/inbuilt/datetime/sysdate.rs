use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{expect_zero_args, utc_now_datetime_string};

pub struct SysDateCommand;

// returns the current date and time

impl InbuiltServerCommand for SysDateCommand {

    fn name(&self) -> &'static str {
        "SYSDATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_zero_args(self.name(), args)?;

        Ok(Some(utc_now_datetime_string().into_bytes()))
        
    }

}
