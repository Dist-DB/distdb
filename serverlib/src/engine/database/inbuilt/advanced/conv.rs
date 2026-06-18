use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::helpers::{
    evaluate_i64_arg,
    evaluate_string_arg,
    expect_arg_count,
    format_signed_in_base,
    parse_base,
    parse_signed_in_base,
    string_result,
};

pub struct ConvCommand;

// returns the result of a CONV expression

impl InbuiltServerCommand for ConvCommand {

    fn name(&self) -> &'static str {
        "CONV"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 3, 3, self.name())?;

        let Some(value) = evaluate_string_arg(args, 0)? else {
            return Ok(None);
        };
        
        let Some(from_base) = evaluate_i64_arg(args, 1)? else {
            return Ok(None);
        };

        let Some(to_base) = evaluate_i64_arg(args, 2)? else {
            return Ok(None);
        };

        let Some(from_base) = parse_base(from_base) else {
            return Ok(None);
        };

        let Some(to_base) = parse_base(to_base) else {
            return Ok(None);
        };

        let Some(number) = parse_signed_in_base(&value, from_base) else {
            return Ok(None);
        };

        Ok(string_result(format_signed_in_base(number, to_base)))

    }

}

