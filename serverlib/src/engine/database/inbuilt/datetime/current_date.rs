use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::{evaluate_argument_expression, function_argument_expr, function_args};

pub struct CurrentDateCommand;

// returns the current date

impl InbuiltServerCommand for CurrentDateCommand {

    fn name(&self) -> &'static str {
        "CURRENT_DATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;
        
        let mut merged = Vec::new();

        Ok(Some(merged))
        
    }

}
