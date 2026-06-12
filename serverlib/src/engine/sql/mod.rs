use sqlparser::ast::Statement;

mod types;
mod classify;
mod literals;
mod parser;
mod requests;
mod schema_plan;
mod select_plan;
mod insert_plan;

pub use insert_plan::parse_insert_rows_from_statement;

pub use requests::{
    parse_mysql8_sql_requests, parse_sql_requests, sql_directive_for_statement,
    sql_statement_metadata,
};

pub use schema_plan::{
    create_table_schema_from_statement, parse_alter_table_change_plan_from_statement,
};

pub use select_plan::{
    parse_select_projection_from_statement, parse_select_read_plan_from_statement,
};

pub use types::{
    AlterTableChangeOp, AlterTableChangePlan, InsertRowsPlan, SelectComparisonOp, SelectCondition,
    SelectPredicate, SelectProjectionItem, SelectReadPlan, SqlCompatibilityTarget, SqlDirective, SqlOperation,
    SqlParseError, SqlRequest, DEFAULT_SQL_COMPATIBILITY_TARGET,
};

pub(super) fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {
    parser::parse_mysql_statements(sql)
}