use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::{evaluate_argument_expression, function_argument_expr, function_args};

pub struct ConcatCommand;

impl InbuiltServerCommand for ConcatCommand {

    fn name(&self) -> &'static str {
        "CONCAT"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        if args.is_empty() {
            return Err(format!("{} requires at least one argument", self.name()));
        }

        let mut merged = Vec::new();

        for argument in args {
            let expr = function_argument_expr(argument)?;
            let Some(value) = evaluate_argument_expression(expr)? else {
                // Match MySQL CONCAT behavior: any NULL input yields NULL.
                return Ok(None);
            };
            merged.extend_from_slice(&value);
        }

        Ok(Some(merged))
        
    }

}
