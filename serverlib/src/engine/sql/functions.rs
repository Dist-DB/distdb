use sqlparser::ast::Function;

use crate::engine::database::inbuilt::{evaluate_inbuilt_sql_function, is_inbuilt_function};

use super::SqlParseError;

pub fn is_supported_sql_function(function_name: &str) -> bool {
    is_inbuilt_function(function_name)
}

pub fn evaluate_sql_function(function: &Function) -> Result<Option<Vec<u8>>, SqlParseError> {
    evaluate_inbuilt_sql_function(function).map_err(|err| {
        SqlParseError::UnsupportedStatement(format!("SQL function evaluation failed: {err}"))
    })
}