use sqlparser::ast::{
    Delete, FromTable, GrantObjects, ObjectName, ObjectType, Query, SchemaName, SetExpr,
    SetOperator, Statement, TableFactor, TableWithJoins, Use,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCompatibilityTarget {
    Mysql80,
}

pub const DEFAULT_SQL_COMPATIBILITY_TARGET: SqlCompatibilityTarget = SqlCompatibilityTarget::Mysql80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDirective {
    Create,
    Retrieve,
    Union,
    Update,
    Delete,
    AlterSchema,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOperation {
    Select,
    UnionQuery,
    Insert,
    Update,
    Delete,
    CreateDatabase,
    CreateTable,
    CreateView,
    CreateOther,
    DropDatabase,
    DropTable,
    DropView,
    DropOther,
    AlterTable,
    AlterView,
    AlterOther,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlRequest {
    pub database_id: String,
    pub sql: String,
    pub directive: SqlDirective,
    pub operation: SqlOperation,
    pub object_name: Option<String>,
    pub compatibility_target: SqlCompatibilityTarget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlParseError {
    EmptyStatement,
    MissingIdentifier { keyword: &'static str, statement: String },
    UnsupportedStatement(String),
}

impl std::fmt::Display for SqlParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyStatement => write!(f, "sql statement is empty"),
            Self::MissingIdentifier { keyword, statement } => {
                write!(f, "sql statement '{statement}' is missing an identifier after '{keyword}'")
            }
            Self::UnsupportedStatement(statement) => {
                write!(f, "unsupported sql statement '{statement}'")
            }
        }
    }
}

impl std::error::Error for SqlParseError {}

pub fn parse_mysql8_sql_requests(
    sql: &str,
    database_id: impl Into<String>,
) -> Result<Vec<SqlRequest>, SqlParseError> {
    parse_sql_requests(sql, database_id, DEFAULT_SQL_COMPATIBILITY_TARGET)
}

pub fn parse_sql_requests(
    sql: &str,
    database_id: impl Into<String>,
    compatibility_target: SqlCompatibilityTarget,
) -> Result<Vec<SqlRequest>, SqlParseError> {
    let database_id = database_id.into();
    let statements = parse_mysql_statements(sql)?;

    if statements.is_empty() {
        return Err(SqlParseError::EmptyStatement);
    }

    statements
        .into_iter()
        .map(|statement| {
            let statement_sql = statement.to_string();
            let (directive, operation, object_name) = classify_statement(&statement, &statement_sql)?;

            Ok(SqlRequest {
                database_id: database_id.clone(),
                sql: statement_sql,
                directive,
                operation,
                object_name,
                compatibility_target,
            })
        })
        .collect()
}

pub fn sql_statement_metadata(
    statement: &str,
) -> Result<(SqlDirective, SqlOperation, Option<String>), SqlParseError> {
    let parsed = parse_mysql_statements(statement)?;

    let single = parsed
        .first()
        .ok_or(SqlParseError::EmptyStatement)?;

    if parsed.len() > 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "expected a single statement for metadata extraction".to_string(),
        ));
    }

    let statement_sql = single.to_string();
    classify_statement(single, &statement_sql)
}

pub fn sql_directive_for_statement(statement: &str) -> Result<SqlDirective, SqlParseError> {
    let (directive, _, _) = sql_statement_metadata(statement)?;
    Ok(directive)
}

fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {
    let dialect = MySqlDialect {};
    Parser::parse_sql(&dialect, sql).map_err(|e| SqlParseError::UnsupportedStatement(e.to_string()))
}

fn classify_statement(
    statement: &Statement,
    statement_sql: &str,
) -> Result<(SqlDirective, SqlOperation, Option<String>), SqlParseError> {
    let normalized = statement_sql.trim();

    let (directive, operation, object_name) = match statement {
        Statement::Query(query) => {
            let has_union = query_contains_operator(query, SetOperator::Union);
            let object_name = first_object_name_in_set_expr(&query.body);

            if has_union {
                (SqlDirective::Union, SqlOperation::UnionQuery, object_name)
            } else {
                (SqlDirective::Retrieve, SqlOperation::Select, object_name)
            }
        }
        Statement::Insert(insert) => (
            SqlDirective::Create,
            SqlOperation::Insert,
            Some(insert.table_name.to_string()),
        ),
        Statement::Update { table, .. } => (
            SqlDirective::Update,
            SqlOperation::Update,
            first_object_name_in_table_with_joins(table),
        ),
        Statement::Delete(delete) => (
            SqlDirective::Delete,
            SqlOperation::Delete,
            first_object_name_in_delete(delete),
        ),
        Statement::CreateDatabase { db_name, .. } => (
            SqlDirective::Create,
            SqlOperation::CreateDatabase,
            Some(db_name.to_string()),
        ),
        Statement::CreateSchema { schema_name, .. } => (
            SqlDirective::Create,
            SqlOperation::CreateDatabase,
            schema_name_to_string(schema_name),
        ),
        Statement::CreateTable(create_table) => (
            SqlDirective::Create,
            SqlOperation::CreateTable,
            Some(create_table.name.to_string()),
        ),
        Statement::CreateView { name, .. } => (
            SqlDirective::Create,
            SqlOperation::CreateView,
            Some(name.to_string()),
        ),
        Statement::CreateIndex(create_index) => (
            SqlDirective::Create,
            SqlOperation::CreateOther,
            create_index
                .name
                .as_ref()
                .map(ObjectName::to_string)
                .or_else(|| Some(create_index.table_name.to_string())),
        ),
        Statement::Drop {
            object_type, names, ..
        } => {
            let object_name = names.first().map(ObjectName::to_string);
            let operation = match object_type {
                ObjectType::Schema => SqlOperation::DropDatabase,
                ObjectType::Table => SqlOperation::DropTable,
                ObjectType::View => SqlOperation::DropView,
                _ => SqlOperation::DropOther,
            };
            (SqlDirective::AlterSchema, operation, object_name)
        }
        Statement::AlterTable { name, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterTable,
            Some(name.to_string()),
        ),
        Statement::AlterView { name, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterView,
            Some(name.to_string()),
        ),
        Statement::Truncate { table_names, .. } => (
            SqlDirective::Delete,
            SqlOperation::Delete,
            table_names.first().map(|target| target.name.to_string()),
        ),
        Statement::ShowCreate { obj_name, .. } => (
            SqlDirective::Retrieve,
            SqlOperation::Select,
            Some(obj_name.to_string()),
        ),
        Statement::ShowColumns { table_name, .. } => (
            SqlDirective::Retrieve,
            SqlOperation::Select,
            Some(table_name.to_string()),
        ),
        Statement::ShowTables { db_name, .. } => (
            SqlDirective::Retrieve,
            SqlOperation::Select,
            db_name.as_ref().map(|name| name.to_string()),
        ),
        Statement::ShowFunctions { .. }
        | Statement::ShowStatus { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowCollation { .. } => (SqlDirective::Retrieve, SqlOperation::Select, None),
        Statement::ShowVariable { variable } => (
            SqlDirective::Retrieve,
            SqlOperation::Select,
            variable.first().map(|name| name.to_string()),
        ),
        Statement::SetVariable { variables, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterOther,
            variables.first().map(ObjectName::to_string),
        ),
        Statement::SetNames { charset_name, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterOther,
            Some(charset_name.clone()),
        ),
        Statement::SetNamesDefault {}
        | Statement::SetRole { .. }
        | Statement::SetTimeZone { .. }
        | Statement::SetTransaction { .. } => {
            (SqlDirective::AlterSchema, SqlOperation::AlterOther, None)
        }
        Statement::StartTransaction { .. } | Statement::Commit { .. } => {
            (SqlDirective::AlterSchema, SqlOperation::AlterOther, None)
        }
        Statement::Rollback { savepoint, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterOther,
            savepoint.as_ref().map(|name| name.to_string()),
        ),
        Statement::Grant { objects, .. } | Statement::Revoke { objects, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterOther,
            first_object_name_in_grant_objects(objects),
        ),
        Statement::Use(use_target) => (
            SqlDirective::AlterSchema,
            SqlOperation::AlterOther,
            use_target_to_object_name(use_target),
        ),
        _ => return Err(SqlParseError::UnsupportedStatement(normalized.to_string())),
    };

    if matches!(
        operation,
        SqlOperation::CreateDatabase
            | SqlOperation::CreateTable
            | SqlOperation::CreateView
            | SqlOperation::DropDatabase
            | SqlOperation::DropTable
            | SqlOperation::DropView
            | SqlOperation::AlterTable
            | SqlOperation::AlterView
    ) && object_name.is_none()
    {
        return Err(SqlParseError::MissingIdentifier {
            keyword: "object",
            statement: normalized.to_string(),
        });
    }

    Ok((directive, operation, object_name))

}

fn set_expr_contains_operator(set_expr: &SetExpr, needle: SetOperator) -> bool {
    match set_expr {
        SetExpr::SetOperation {
            op,
            left,
            right,
            ..
        } => {
            *op == needle
                || set_expr_contains_operator(left, needle)
                || set_expr_contains_operator(right, needle)
        }
        SetExpr::Query(query) => query_contains_operator(query, needle),
        _ => false,
    }
}

fn query_contains_operator(query: &Query, needle: SetOperator) -> bool {
    set_expr_contains_operator(&query.body, needle)
        || query
            .with
            .as_ref()
            .map(|with| {
                with.cte_tables
                    .iter()
                    .any(|cte| query_contains_operator(&cte.query, needle))
            })
            .unwrap_or(false)
}

fn first_object_name_in_set_expr(set_expr: &SetExpr) -> Option<String> {
    match set_expr {
        SetExpr::Select(select) => select
            .from
            .first()
            .and_then(first_object_name_in_table_with_joins),
        SetExpr::Query(query) => first_object_name_in_set_expr(&query.body),
        SetExpr::SetOperation { left, right, .. } => {
            first_object_name_in_set_expr(left).or_else(|| first_object_name_in_set_expr(right))
        }
        _ => None,
    }
}

fn first_object_name_in_delete(delete: &Delete) -> Option<String> {
    match &delete.from {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            tables.first().and_then(first_object_name_in_table_with_joins)
        }
    }
}

fn first_object_name_in_table_with_joins(table: &TableWithJoins) -> Option<String> {
    match &table.relation {
        TableFactor::Table { name, .. } => Some(name.to_string()),
        TableFactor::Derived { subquery, .. } => first_object_name_in_set_expr(&subquery.body),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => first_object_name_in_table_with_joins(table_with_joins),
        _ => None,
    }
}

fn schema_name_to_string(schema_name: &SchemaName) -> Option<String> {
    match schema_name {
        SchemaName::Simple(name) => Some(name.to_string()),
        SchemaName::UnnamedAuthorization(name) => Some(name.to_string()),
        SchemaName::NamedAuthorization(name, _) => Some(name.to_string()),
    }
}

fn use_target_to_object_name(use_target: &Use) -> Option<String> {
    match use_target {
        Use::Catalog(name)
        | Use::Schema(name)
        | Use::Database(name)
        | Use::Warehouse(name)
        | Use::Object(name) => Some(name.to_string()),
        Use::Default => None,
    }
}

fn first_object_name_in_grant_objects(objects: &GrantObjects) -> Option<String> {
    match objects {
        GrantObjects::AllSequencesInSchema { schemas }
        | GrantObjects::AllTablesInSchema { schemas }
        | GrantObjects::Schemas(schemas)
        | GrantObjects::Sequences(schemas)
        | GrantObjects::Tables(schemas) => schemas.first().map(|name| name.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_returns_directives_for_multiple_statements() {
        let requests = parse_mysql8_sql_requests(
            "select * from users; update users set active=1 where id=1",
            "main",
        )
        .expect("requests should parse");

        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].directive, SqlDirective::Retrieve);
        assert_eq!(requests[0].operation, SqlOperation::Select);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
        assert_eq!(requests[1].directive, SqlDirective::Update);
        assert_eq!(requests[1].operation, SqlOperation::Update);
        assert_eq!(requests[1].object_name.as_deref(), Some("users"));
        assert_eq!(requests[0].compatibility_target, SqlCompatibilityTarget::Mysql80);
    }

    #[test]
    fn parser_rejects_unsupported_statement() {
        let error = parse_mysql8_sql_requests("explain select * from users", "main")
            .expect_err("unsupported statement should fail");

        assert!(matches!(error, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn drop_statement_maps_to_alter_schema_directive() {
        let requests = parse_mysql8_sql_requests("drop table users", "main")
            .expect("drop statement should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::DropTable);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn create_database_operation_parses_object_name() {
        let requests = parse_mysql8_sql_requests("create database analytics", "main")
            .expect("create database should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].operation, SqlOperation::CreateDatabase);
        assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
    }

    #[test]
    fn create_schema_operation_maps_to_create_database() {
        let requests = parse_mysql8_sql_requests("create schema analytics", "main")
            .expect("create schema should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::CreateDatabase);
        assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
    }

    #[test]
    fn select_from_qualified_table_keeps_database_and_table_name() {
        let requests = parse_mysql8_sql_requests("select * from main.users", "main")
            .expect("qualified select should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Retrieve);
        assert_eq!(requests[0].operation, SqlOperation::Select);
        assert_eq!(requests[0].object_name.as_deref(), Some("main.users"));
    }

    #[test]
    fn union_select_maps_to_union_directive() {
        let requests = parse_mysql8_sql_requests(
            "select id from users union select id from archived_users",
            "main",
        )
        .expect("union select should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Union);
        assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn parenthesized_union_select_maps_to_union_directive() {
        let requests = parse_mysql8_sql_requests(
            "(select id as a from users) union (select id as b from archived_users)",
            "main",
        )
        .expect("parenthesized union select should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Union);
        assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn create_view_operation_parses_object_name() {
        let requests = parse_mysql8_sql_requests(
            "create view active_users as select * from users",
            "main",
        )
        .expect("create view should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::CreateView);
        assert_eq!(requests[0].object_name.as_deref(), Some("active_users"));
    }

    #[test]
    fn drop_view_operation_maps_to_alter_schema() {
        let requests = parse_mysql8_sql_requests("drop view archived_users", "main")
            .expect("drop view should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::DropView);
        assert_eq!(requests[0].object_name.as_deref(), Some("archived_users"));
    }

    #[test]
    fn drop_schema_operation_maps_to_drop_database() {
        let requests = parse_mysql8_sql_requests("drop schema analytics", "main")
            .expect("drop schema should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::DropDatabase);
        assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
    }

    #[test]
    fn alter_view_operation_maps_to_alter_schema() {
        let requests = parse_mysql8_sql_requests(
            "alter view active_users as select id from users",
            "main",
        )
        .expect("alter view should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterView);
        assert_eq!(requests[0].object_name.as_deref(), Some("active_users"));
    }

    #[test]
    fn insert_select_union_maps_to_insert_operation() {
        let requests = parse_mysql8_sql_requests(
            "insert into users (id) (select id from staged_users) union (select id from backup_users)",
            "main",
        )
        .expect("insert-select union should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::Insert);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn cte_union_select_maps_to_union_directive() {
        let requests = parse_mysql8_sql_requests(
            "with combined as (select id from users union select id from archived_users) select * from combined",
            "main",
        )
        .expect("cte union select should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Union);
        assert_eq!(requests[0].operation, SqlOperation::UnionQuery);
        assert_eq!(requests[0].object_name.as_deref(), Some("combined"));
    }

    #[test]
    fn truncate_table_maps_to_delete_operation() {
        let requests = parse_mysql8_sql_requests("truncate table users", "main")
            .expect("truncate table should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Delete);
        assert_eq!(requests[0].operation, SqlOperation::Delete);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn show_columns_maps_to_retrieve_operation() {
        let requests = parse_mysql8_sql_requests("show columns from users", "main")
            .expect("show columns should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Retrieve);
        assert_eq!(requests[0].operation, SqlOperation::Select);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn use_database_maps_to_alter_schema_other_operation() {
        let requests = parse_mysql8_sql_requests("use analytics", "main")
            .expect("use database should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
    }

    #[test]
    fn create_index_maps_to_create_other_operation() {
        let requests = parse_mysql8_sql_requests("create index idx_users_id on users(id)", "main")
            .expect("create index should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::CreateOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("idx_users_id"));
    }

    #[test]
    fn set_names_maps_to_alter_schema_other_operation() {
        let requests = parse_mysql8_sql_requests("set names utf8mb4", "main")
            .expect("set names should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("utf8mb4"));
    }

    #[test]
    fn set_variable_maps_to_alter_schema_other_operation() {
        let requests = parse_mysql8_sql_requests("set autocommit = 0", "main")
            .expect("set variable should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("autocommit"));
    }

    #[test]
    fn show_create_table_maps_to_retrieve_operation() {
        let requests = parse_mysql8_sql_requests("show create table users", "main")
            .expect("show create table should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Retrieve);
        assert_eq!(requests[0].operation, SqlOperation::Select);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn show_tables_from_database_maps_to_retrieve_operation() {
        let requests = parse_mysql8_sql_requests("show tables from analytics", "main")
            .expect("show tables from db should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Retrieve);
        assert_eq!(requests[0].operation, SqlOperation::Select);
        assert_eq!(requests[0].object_name.as_deref(), Some("analytics"));
    }

    #[test]
    fn start_transaction_maps_to_alter_schema_other_operation() {
        let requests = parse_mysql8_sql_requests("start transaction", "main")
            .expect("start transaction should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert!(requests[0].object_name.is_none());
    }

    #[test]
    fn rollback_to_savepoint_maps_savepoint_name() {
        let requests = parse_mysql8_sql_requests("rollback to savepoint sp1", "main")
            .expect("rollback to savepoint should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("sp1"));
    }

    #[test]
    fn grant_statement_maps_to_alter_schema_other_operation() {
        let requests = parse_mysql8_sql_requests("grant select on users to app_user", "main")
            .expect("grant should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn revoke_statement_maps_to_alter_schema_other_operation() {
        let requests = parse_mysql8_sql_requests("revoke select on users from app_user", "main")
            .expect("revoke should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::AlterOther);
        assert_eq!(requests[0].object_name.as_deref(), Some("users"));
    }

    #[test]
    fn create_function_is_unsupported() {
        let error = parse_mysql8_sql_requests("create function f_add() returns int", "main")
            .expect_err("create function should be unsupported");

        assert!(matches!(error, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn drop_function_is_unsupported() {
        let error = parse_mysql8_sql_requests("drop function f_add", "main")
            .expect_err("drop function should be unsupported");

        assert!(matches!(error, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn create_procedure_is_unsupported() {
        let error = parse_mysql8_sql_requests("create procedure p_sync() begin end", "main")
            .expect_err("create procedure should be unsupported");

        assert!(matches!(error, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn drop_procedure_is_unsupported() {
        let error = parse_mysql8_sql_requests("drop procedure p_sync", "main")
            .expect_err("drop procedure should be unsupported");

        assert!(matches!(error, SqlParseError::UnsupportedStatement(_)));
    }

    #[test]
    fn create_trigger_is_unsupported() {
        let error = parse_mysql8_sql_requests(
            "create trigger trg_users_bi before insert on users for each row begin end",
            "main",
        )
        .expect_err("create trigger should be unsupported");

        assert!(matches!(error, SqlParseError::UnsupportedStatement(_)));
    }
}
