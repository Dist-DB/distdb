use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_f64_arg, expect_arg_count, float_result};

pub struct LogCommand;

// returns the logarithm of the number

impl InbuiltServerCommand for LogCommand {

    fn name(&self) -> &'static str {
        "LOG"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 1, 2, self.name())?;

        let result = if args.len() == 1 {
            
            let Some(value) = evaluate_f64_arg(args, 0)? else {
                return Ok(None);
            };
            
            if value <= 0.0 {
                return Ok(None);
            }
            
            value.ln()

        } else {
            
            let Some(base) = evaluate_f64_arg(args, 0)? else {
                return Ok(None);
            };
            
            let Some(value) = evaluate_f64_arg(args, 1)? else {
                return Ok(None);
            };
            
            if base <= 0.0 || base == 1.0 || value <= 0.0 {
                return Ok(None);
            }
            
            value.log(base)
            
        };

        Ok(float_result(result))
        
    }

}
