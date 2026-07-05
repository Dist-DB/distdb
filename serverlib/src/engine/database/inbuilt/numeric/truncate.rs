use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{evaluate_f64_arg, evaluate_i64_arg, expect_arg_count, float_result, truncate_mysql};

pub struct TruncateCommand;

// returns the truncated value of the given number

impl InbuiltServerCommand for TruncateCommand {

    fn name(&self) -> &'static str {
        "TRUNCATE"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 2, 2, self.name())?;

        let Some(value) = evaluate_f64_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(decimals) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        Ok(float_result(truncate_mysql(value, decimals)))
        
    }

}
