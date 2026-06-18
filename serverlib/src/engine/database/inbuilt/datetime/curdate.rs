use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{expect_zero_args, utc_now_date_string};

pub struct CurDateCommand;

// returns the current date

impl InbuiltServerCommand for CurDateCommand {

    fn name(&self) -> &'static str {
        "CURDATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_zero_args(self.name(), args)?;

        Ok(Some(utc_now_date_string().into_bytes()))
        
    }

}
