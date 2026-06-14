use sqlparser::ast::Statement;

mod types;
mod classify;
mod literals;
mod parser;
mod requests;
mod schema_plan;
mod select_plan;
mod insert_plan;
mod update_plan;
mod delete_plan;

pub use insert_plan::parse_insert_rows_from_statement;
pub use update_plan::parse_update_rows_from_statement;
pub use delete_plan::parse_delete_rows_from_statement;

pub use requests::{
    parse_mysql8_sql_requests, parse_sql_requests, sql_directive_for_statement,
    sql_statement_metadata,
};

pub use schema_plan::{
    create_table_schema_from_statement, parse_alter_table_change_plan_from_statement,
};

pub use select_plan::{
    parse_select_projection_from_statement, parse_select_read_plan_from_statement,
    parse_select_condition_from_expr, parse_relation_bindings_from_table_with_joins,
    parse_joins_from_table_with_joins, derive_relation_pushdown_conditions,
};

pub use types::{
    AlterTableChangeOp, AlterTableChangePlan, DeleteRowsPlan, InsertRowsPlan, InsertRowsSource,
    SelectComparisonOp, SelectCondition,
    SelectJoin, SelectJoinKind, SelectPredicate, SelectProjectionItem, SelectReadPlan, SelectRelation,
    SqlCompatibilityTarget, SqlDirective, SqlOperation,
    SqlParseError, SqlRequest, DEFAULT_SQL_COMPATIBILITY_TARGET,
    UpdateAssignment, UpdateRowsPlan,
};

pub(super) fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {
    parser::parse_mysql_statements(sql)
}