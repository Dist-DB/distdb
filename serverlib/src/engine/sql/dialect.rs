use super::SqlCompatibilityTarget;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlDialectCapabilities {
    pub supports_searched_case_expressions: bool,
    pub supports_simple_case_expressions: bool,
    pub supports_if_expression_function: bool,
    pub supports_if_else_end_statements: bool,
    pub supports_stored_procedures: bool,
    pub supports_user_defined_functions: bool,
}

pub fn dialect_capabilities_for_target(
    target: SqlCompatibilityTarget,
) -> SqlDialectCapabilities {

    match target {
        
        SqlCompatibilityTarget::Mysql80 => SqlDialectCapabilities {
            supports_searched_case_expressions: true,
            supports_simple_case_expressions: true,
            supports_if_expression_function: true,
            supports_if_else_end_statements: true,
            supports_stored_procedures: true,
            supports_user_defined_functions: true,
        },
    
    }

}
