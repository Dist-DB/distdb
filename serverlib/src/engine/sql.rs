use sqlparser::ast::{
    ColumnOption, DataType, Delete, FromTable, GrantObjects, ObjectName, ObjectType, Query,
    SchemaName, SetExpr, SetOperator, Statement, TableFactor, TableWithJoins, Use,
};
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;

use crate::{FieldDef, FieldIndex, FieldType, TableSchema};

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
    TruncateTable,
    CreateDatabase,
    CreateTable,
    CreateView,
    CreateTrigger,
    CreateStoredProcedure,
    CreateOther,
    DropDatabase,
    DropTable,
    DropView,
    DropTrigger,
    DropStoredProcedure,
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
    let statements = match parse_mysql_statements(sql) {
        Ok(statements) => statements,
        Err(parse_error) => {
            let trimmed = sql.trim();
            if let Some((directive, operation, object_name)) = classify_text_fallback(trimmed) {
                return Ok(vec![SqlRequest {
                    database_id,
                    sql: trimmed.to_string(),
                    directive,
                    operation,
                    object_name,
                    compatibility_target,
                }]);
            }
            return Err(parse_error);
        }
    };

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

    let parsed = match parse_mysql_statements(statement) {
        Ok(parsed) => parsed,
        Err(parse_error) => {
            if let Some(metadata) = classify_text_fallback(statement.trim()) {
                return Ok(metadata);
            }
            return Err(parse_error);
        }
    };

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

pub fn create_table_schema_from_statement(
    statement: &str,
) -> Result<(String, TableSchema), SqlParseError> {

    let parsed = parse_mysql_statements(statement)?;
    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    let Statement::CreateTable(create_table) = single else {
        return Err(SqlParseError::UnsupportedStatement(
            "statement is not CREATE TABLE".to_string(),
        ));
    };

    let table_id = create_table.name.to_string();
    let mut fields = Vec::with_capacity(create_table.columns.len());

    for (idx, column) in create_table.columns.iter().enumerate() {
        
        let nullable = !column
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::NotNull));

        let indexed = if column.options.iter().any(|opt| {
            matches!(
                opt.option,
                ColumnOption::Unique {
                    is_primary: true,
                    ..
                }
            )
        }) {
            FieldIndex::PrimaryKey
        } else if column
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::Unique { .. }))
        {
            FieldIndex::Indexed
        } else {
            FieldIndex::None
        };

        fields.push(FieldDef {
            seqno: (idx + 1) as u32,
            field_name: column.name.value.clone(),
            field_type: map_sql_data_type(&column.data_type),
            nullable,
            indexed,
            default_value: None,
        });

    }

    Ok((table_id, TableSchema::new(fields)))
    
}

fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {
    let mysql = MySqlDialect {};
    match Parser::parse_sql(&mysql, sql) {
        Ok(statements) => Ok(statements),
        Err(mysql_error) => {
            let generic = GenericDialect {};
            Parser::parse_sql(&generic, sql)
                .map_err(|_| SqlParseError::UnsupportedStatement(mysql_error.to_string()))
        }
    }
}

fn map_sql_data_type(data_type: &DataType) -> FieldType {

    let lowered = data_type.to_string().to_ascii_lowercase();

    if lowered.contains("unsigned") {

        if lowered.contains("bigint") {
            return FieldType::UInt(64);
        }

        if lowered.contains("smallint") {
            return FieldType::UInt(16);
        }

        if lowered.contains("tinyint") {
            return FieldType::UInt(8);
        }
        
        return FieldType::UInt(32);
    }

    if lowered.contains("bigint") {
        return FieldType::Int(64);
    }
    
    if lowered.contains("smallint") {
        return FieldType::Int(16);
    }
    
    if lowered.contains("tinyint") {
        return FieldType::Int(8);
    }
    
    if lowered.contains("int") {
        return FieldType::Int(32);
    }
    
    if lowered.contains("double") {
        return FieldType::Float(64);
    }
    
    if lowered.contains("float") || lowered.contains("real") {
        return FieldType::Float(32);
    }
    
    if lowered.contains("blob") || lowered.contains("binary") {
        return FieldType::Blob;
    }
    
    if lowered.contains("char(") {
        let start = lowered.find("char(").unwrap_or(0) + 5;
        let end = lowered[start..]
            .find(')')
            .map(|i| start + i)
            .unwrap_or(start);
        if let Ok(len) = lowered[start..end].trim().parse::<usize>() {
            return FieldType::StringFixed(len.max(1));
        }
        return FieldType::StringFixed(32);
    }
    
    if lowered.contains("text") || lowered.contains("varchar") || lowered.contains("string") {
        return FieldType::Text;
    }

    FieldType::Text

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
        Statement::CreateTrigger { name, .. } => (
            SqlDirective::Create,
            SqlOperation::CreateTrigger,
            Some(name.to_string()),
        ),
        Statement::CreateProcedure { name, .. } => (
            SqlDirective::Create,
            SqlOperation::CreateStoredProcedure,
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
        Statement::DropTrigger { trigger_name, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::DropTrigger,
            Some(trigger_name.to_string()),
        ),
        Statement::DropProcedure { proc_desc, .. } => (
            SqlDirective::AlterSchema,
            SqlOperation::DropStoredProcedure,
            proc_desc.first().map(|desc| desc.name.to_string()),
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
            SqlOperation::TruncateTable,
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
            | SqlOperation::CreateTrigger
            | SqlOperation::CreateStoredProcedure
            | SqlOperation::DropDatabase
            | SqlOperation::DropTable
            | SqlOperation::DropView
            | SqlOperation::DropTrigger
            | SqlOperation::DropStoredProcedure
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

fn classify_text_fallback(statement: &str) -> Option<(SqlDirective, SqlOperation, Option<String>)> {

    let tokens = statement
        .split_whitespace()
        .map(|token| token.trim_matches(';'))
        .collect::<Vec<_>>();

    let Some(first) = tokens.first() else {
        return None;
    };

    let verb = first.to_ascii_lowercase();
    if verb != "create" && verb != "drop" {
        return None;
    }

    let mut object_idx = 1usize;
    if verb == "create" && tokens.get(1).is_some_and(|tok| tok.eq_ignore_ascii_case("or")) {
        let modifier = tokens.get(2)?.to_ascii_lowercase();
        if modifier != "replace" && modifier != "alter" {
            return None;
        }
        object_idx = 3;
    }

    let object_kind = tokens.get(object_idx)?.to_ascii_lowercase();
    let object_name = fallback_object_name_after_tokens(&tokens, &verb, object_idx);

    match (verb.as_str(), object_kind.as_str()) {
        ("create", "trigger") => Some((SqlDirective::Create, SqlOperation::CreateTrigger, object_name)),
        ("drop", "trigger") => Some((SqlDirective::AlterSchema, SqlOperation::DropTrigger, object_name)),
        ("create", "procedure") => Some((SqlDirective::Create, SqlOperation::CreateStoredProcedure, object_name)),
        ("drop", "procedure") => Some((SqlDirective::AlterSchema, SqlOperation::DropStoredProcedure, object_name)),
        ("drop", "database") => Some((SqlDirective::AlterSchema, SqlOperation::DropDatabase, object_name)),
        // Intentionally unsupported for now.
        ("create", "function") | ("drop", "function") => None,
        _ => None,
    }

}

fn fallback_object_name_after_tokens(
    tokens: &[&str],
    verb: &str,
    object_idx: usize,
) -> Option<String> {

    let mut name_idx = object_idx + 1;
    
    if verb == "drop" && tokens.get(name_idx).is_some_and(|tok| tok.eq_ignore_ascii_case("if")) {
        if !tokens
            .get(name_idx + 1)
            .is_some_and(|tok| tok.eq_ignore_ascii_case("exists"))
        {
            return None;
        }
        name_idx += 2;
    }

    let name = tokens.get(name_idx)?;

    Some(
        name.trim_matches(|c| c == ';' || c == '(' || c == ')')
            .to_string(),
    )

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
    fn create_table_schema_helper_maps_fields() {
        let (table_id, schema) = create_table_schema_from_statement(
            "create table users (id bigint not null primary key, email varchar(255) not null, age int)",
        )
        .expect("create table schema should parse");

        assert_eq!(table_id, "users");
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.fields[0].field_name, "id");
        assert_eq!(schema.fields[0].field_type, FieldType::Int(64));
        assert_eq!(schema.fields[0].indexed, FieldIndex::PrimaryKey);
        assert!(!schema.fields[0].nullable);

        assert_eq!(schema.fields[1].field_name, "email");
        assert_eq!(schema.fields[1].field_type, FieldType::StringFixed(255));
        assert!(!schema.fields[1].nullable);

        assert_eq!(schema.fields[2].field_name, "age");
        assert_eq!(schema.fields[2].field_type, FieldType::Int(32));
        assert!(schema.fields[2].nullable);
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
    fn drop_database_operation_maps_to_drop_database() {
        let requests = parse_mysql8_sql_requests("drop database analytics", "main")
            .expect("drop database should parse");

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
    fn truncate_table_maps_to_truncate_operation() {
        let requests = parse_mysql8_sql_requests("truncate table users", "main")
            .expect("truncate table should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Delete);
        assert_eq!(requests[0].operation, SqlOperation::TruncateTable);
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
    fn create_procedure_maps_to_create_stored_procedure_operation() {
        let requests = parse_mysql8_sql_requests(
            "create procedure p_sync() as begin end;",
            "main",
        )
        .expect("create procedure should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::CreateStoredProcedure);
        assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
    }

    #[test]
    fn drop_procedure_maps_to_drop_stored_procedure_operation() {
        let requests = parse_mysql8_sql_requests("drop procedure p_sync", "main")
            .expect("drop procedure should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::DropStoredProcedure);
        assert_eq!(requests[0].object_name.as_deref(), Some("p_sync"));
    }

    #[test]
    fn create_trigger_maps_to_create_trigger_operation() {
        let requests = parse_mysql8_sql_requests(
            "create trigger trg_users_bi before insert on users for each row execute function audit_users()",
            "main",
        )
        .expect("create trigger should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::CreateTrigger);
        assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
    }

    #[test]
    fn drop_trigger_maps_to_drop_trigger_operation() {
        let requests = parse_mysql8_sql_requests("drop trigger trg_users_bi on users", "main")
            .expect("drop trigger should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::DropTrigger);
        assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
    }

    #[test]
    fn create_or_replace_trigger_maps_to_create_trigger_operation() {
        let requests = parse_mysql8_sql_requests(
            "create or replace trigger trg_users_bi before insert on users for each row set @x = 1",
            "main",
        )
        .expect("create or replace trigger should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::Create);
        assert_eq!(requests[0].operation, SqlOperation::CreateTrigger);
        assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
    }

    #[test]
    fn drop_trigger_if_exists_maps_to_drop_trigger_operation() {
        let requests = parse_mysql8_sql_requests("drop trigger if exists trg_users_bi", "main")
            .expect("drop trigger if exists should parse");

        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].directive, SqlDirective::AlterSchema);
        assert_eq!(requests[0].operation, SqlOperation::DropTrigger);
        assert_eq!(requests[0].object_name.as_deref(), Some("trg_users_bi"));
    }
}
