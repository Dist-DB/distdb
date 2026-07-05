use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::helpers::{expect_zero_args, utc_now_date_string};

pub struct CurrentDateCommand;

// returns the current date

impl InbuiltServerCommand for CurrentDateCommand {

    fn name(&self) -> &'static str {
        "CURRENT_DATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_zero_args(self.name(), args)?;

        Ok(Some(utc_now_date_string().into_bytes()))
        
    }

}
