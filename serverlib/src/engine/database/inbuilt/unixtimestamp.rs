use std::time::{SystemTime, UNIX_EPOCH};

use sqlparser::ast::Function;

use super::command::InbuiltServerCommand;
use super::indexer::function_args;

pub struct UnixTimestampCommand;

impl InbuiltServerCommand for UnixTimestampCommand {

    fn name(&self) -> &'static str {
        "UNIXTIMESTAMP"
    }

    fn evaluate(&self, function: &Function) -> Result<Option<Vec<u8>>, String> {

        if !function_args(function)?.is_empty() {
            return Err(format!(
                "{} currently supports only zero arguments",
                self.name()
            ));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|err| format!("{} evaluation failed: {err}", self.name()))?
            .as_secs();

        Ok(Some(now.to_string().into_bytes()))
        
    }

}
