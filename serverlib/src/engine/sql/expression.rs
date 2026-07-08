
use sqlparser::ast::Function;

use super::sql_function_references_column;

#[expect(clippy::large_enum_variant, reason="the enum variants are large but necessary for the expression representation")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectExpression {
    Null,
    Literal(Vec<u8>),
    Column { field_name: String },
    InbuiltFunction { function: Function },
}

pub fn expression_references_column(expression: &SelectExpression) -> bool {

    match expression {
        
        SelectExpression::Column { .. } => true,
        
        SelectExpression::InbuiltFunction { function } => sql_function_references_column(function),

        _ => false,
    
    }

}

#[cfg(test)]
#[path = "expression_test.rs"]
mod tests;
