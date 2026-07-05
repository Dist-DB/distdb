use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args
};

use super::{expect_arg_count, float_result};

pub struct PiCommand;

// returns the value of pi

impl InbuiltServerCommand for PiCommand {

    fn name(&self) -> &'static str {
        "PI"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 0, self.name())?;

        Ok(float_result(std::f64::consts::PI))
        
    }

}
