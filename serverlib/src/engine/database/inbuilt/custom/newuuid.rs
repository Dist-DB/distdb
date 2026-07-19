use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{
    command::InbuiltServerCommand,
    indexer::function_args,
};

pub struct NewUuidCommand;

impl InbuiltServerCommand for NewUuidCommand {

    fn name(&self) -> &'static str {
        "NEWUUID"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        let args = function_args(function)?;

        if !args.is_empty() {
            return Err(format!("{} requires 0 arguments", self.name()));
        }

        Ok(Some(common::Uuid::now_v7().to_string().into_bytes()))

    }

}
