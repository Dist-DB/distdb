use sqlparser::ast::Statement;

mod types;
mod expression;
mod dialect;
mod classify;
mod literals;
mod parser;
mod requests;
mod schema_plan;
mod functions;
mod predicates;
mod select_plan;
mod routine_plan;
mod trigger_plan;
mod insert_plan;
mod update_plan;
mod delete_plan;

pub use insert_plan::{parse_insert_rows_from_parsed_statement, parse_insert_rows_from_statement};
pub use update_plan::parse_update_rows_from_statement;
pub use delete_plan::parse_delete_rows_from_statement;

pub use requests::{
    parse_mysql8_sql_requests, parse_sql_requests, sql_directive_for_statement,
    sql_statement_metadata,
};

pub use schema_plan::{
    create_table_plan_from_statement, create_table_schema_from_statement,
    parse_alter_table_change_plan_from_statement,
};

pub use select_plan::{
    parse_select_projection_from_statement, parse_select_read_plan_from_statement,
    parse_union_select_read_plans_from_statement,
    parse_select_condition_from_expr, parse_relation_bindings_from_table_with_joins,
    parse_joins_from_table_with_joins, derive_relation_pushdown_conditions,
    parse_create_view_dependencies_from_statement,
    parse_create_view_dependencies_from_sql,
};
pub use routine_plan::{
    bind_call_procedure_arguments,
    extract_create_function_action_sql,
    extract_create_function_return_expression,
    parse_create_procedure_parameter_names_from_statement,
    parse_create_function_parameter_names_from_statement,
    parse_create_function_return_type_from_statement,
    parse_if_else_end_plan_from_create_procedure_statement,
    parse_if_else_end_plan_from_statement,
};
pub use trigger_plan::parse_trigger_invocation_binding_from_create_trigger_statement;

pub use functions::{
    evaluate_expression_sql_to_bytes,
    evaluate_inbuilt_sql_function_with_lookup,
    evaluate_sql_function, evaluate_sql_function_with_lookup,
    function_argument_values,
    is_supported_sql_function, sql_function_references_column,
    SqlFunctionEvaluationStrategy, with_lookup_sql_function_evaluator,
};
pub use dialect::{dialect_capabilities_for_target, SqlDialectCapabilities};
pub use expression::{expression_references_column, SelectExpression};
pub use predicates::{
    compare_like_value, compare_regex_value, compare_row_value, validate_regex_pattern,
};

pub use types::{
    AlterTableChangeOp, AlterTableChangePlan, DeleteRowsPlan, IfElseEndBranchPlan,
    IfElseEndPlan, InsertRowsPlan, InsertRowsSource,
    SelectCaseWhen,
    SelectComparisonOp, SelectCondition,
    SelectCtePlan, SelectJoin, SelectJoinKind, SelectOrderByItem, SelectSetBoundaryOp, SelectSetQueryStep, SelectPredicate,
    SelectProjectionItem, SelectReadPlan, SelectRelation,
    TriggerEventKind, TriggerInvocationBinding, TriggerTiming,
    SqlCompatibilityTarget, SqlDirective, SqlOperation,
    SqlParseError, SqlRequest, DEFAULT_SQL_COMPATIBILITY_TARGET,
    UpdateAssignment, UpdateRowsPlan,
};

pub(super) fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {
    parser::parse_mysql_statements(sql)
}