use sqlparser::ast::Function;

use crate::engine::database::inbuilt::command::InbuiltServerCommand;
use crate::engine::database::inbuilt::indexer::function_args;

use super::{evaluate_i64_arg, expect_arg_count, float_result, random_seed_now, seeded_random};

pub struct RandCommand;

// returns a random number

impl InbuiltServerCommand for RandCommand {

    fn name(&self) -> &'static str {
        "RAND"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        expect_arg_count(args, 0, 1, self.name())?;

        let value = if args.is_empty() {
			seeded_random(random_seed_now())
		} else {
			let Some(seed) = evaluate_i64_arg(args, 0)? else {
				return Ok(None);
			};
			seeded_random(seed)
		};

        Ok(float_result(value))
        
    }

}
