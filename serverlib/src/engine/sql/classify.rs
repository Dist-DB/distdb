use sqlparser::ast::{
    Delete, FromTable, GrantObjects, ObjectName, ObjectType, Query, SchemaName, SetExpr,
    SetOperator, Statement, TableFactor, TableWithJoins, Use,
};

use super::{parse_mysql_statements, SqlDirective, SqlOperation, SqlParseError};

pub(super) fn classify_statement(
    statement: &Statement,
    statement_sql: &str,
) -> Result<(SqlDirective, SqlOperation, Option<String>), SqlParseError> {

    let normalized = statement_sql.trim();
    let normalized_lower = normalized.to_ascii_lowercase();

    if normalized_lower.starts_with("explain ") {
        let inner_statement = normalized["explain".len()..].trim();
        let parsed_inner = parse_mysql_statements(inner_statement)?;

        let Some(single_inner) = parsed_inner.first() else {
            return Err(SqlParseError::EmptyStatement);
        };

        let inner_sql = single_inner.to_string();
        let (_, inner_operation, inner_object_name) =
            classify_statement(single_inner, &inner_sql)?;

        if !matches!(
            inner_operation,
            SqlOperation::Select | SqlOperation::Insert | SqlOperation::Update | SqlOperation::Delete
        ) {
            return Err(SqlParseError::UnsupportedStatement(normalized.to_string()));
        }

        return Ok((SqlDirective::Retrieve, inner_operation, inner_object_name));
    }

    let (directive, operation, object_name) = match statement {

        Statement::Query(query) => {
            let has_set_operation = query_contains_operator(query, SetOperator::Union)
                || query_contains_operator(query, SetOperator::Except)
                || query_contains_operator(query, SetOperator::Intersect);
            let object_name = first_object_name_in_set_expr(&query.body);

            if has_set_operation {
                (SqlDirective::Union, SqlOperation::UnionQuery, object_name)
            } else {
                (SqlDirective::Retrieve, SqlOperation::Select, object_name)
            }
        },

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

        Statement::Call(function) => (
            SqlDirective::Retrieve,
            SqlOperation::CallStoredProcedure,
            Some(function.name.to_string()),
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

        Statement::ExplainTable { table_name, .. } => (
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
            | SqlOperation::CallStoredProcedure
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
        },

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

        Use::Catalog(name) |
        Use::Schema(name) |
        Use::Database(name) |
        Use::Warehouse(name) |
        Use::Object(name) => Some(name.to_string()),

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

pub(super) fn classify_text_fallback(
    statement: &str,
) -> Option<(SqlDirective, SqlOperation, Option<String>)> {
    
    let tokens = statement
        .split_whitespace()
        .map(|token| token.trim_matches(';'))
        .collect::<Vec<_>>();

    let first = tokens.first()?;
    let verb = first.to_ascii_lowercase();

    match verb.as_str() {

        "create" | "drop" => {},

        "call" => {
            let object_name = tokens
                .get(1)
                .and_then(|name| normalize_fallback_object_name(name));
            return Some((
                SqlDirective::Retrieve,
                SqlOperation::CallStoredProcedure,
                object_name,
            ));
        },

        _ => return None,

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

        ("create", "trigger")   => Some((SqlDirective::Create, SqlOperation::CreateTrigger, object_name)),

        ("drop", "trigger")     => Some((SqlDirective::AlterSchema, SqlOperation::DropTrigger, object_name)),

        ("create", "procedure") => Some((SqlDirective::Create, SqlOperation::CreateStoredProcedure, object_name)),

        ("drop", "procedure")   => Some((SqlDirective::AlterSchema, SqlOperation::DropStoredProcedure, object_name)),

        ("drop", "database")    => Some((SqlDirective::AlterSchema, SqlOperation::DropDatabase, object_name)),

        ("create", "database") | 
        ("create", "schema")    => Some((SqlDirective::Create, SqlOperation::CreateDatabase, object_name)),

        // Intentionally unsupported for now.
        ("create", "function") | 
        ("drop", "function")    => None,
        
        _                       => None,

    }
    
}

fn fallback_object_name_after_tokens(tokens: &[&str], verb: &str, object_idx: usize) -> Option<String> {

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

    normalize_fallback_object_name(name)

}

fn normalize_fallback_object_name(token: &str) -> Option<String> {

    let trimmed = token.trim_matches(';');
    let head = trimmed.split_once('(').map_or(trimmed, |(name, _)| name);
    let normalized = head.trim_matches(|c| c == ';' || c == '`' || c == '"');

    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fallback_extracts_parameterized_procedure_name_for_create() {
        let classified = classify_text_fallback(
            "create procedure p_arg_route(p_mode uint64) begin if p_mode = 1 then select 1; end if; end;",
        )
        .expect("create procedure should classify");

        assert_eq!(classified.1, SqlOperation::CreateStoredProcedure);
        assert_eq!(classified.2.as_deref(), Some("p_arg_route"));
    }

    #[test]
    fn fallback_extracts_parameterized_procedure_name_for_call() {
        let classified = classify_text_fallback("call p_arg_route(1);")
            .expect("call procedure should classify");

        assert_eq!(classified.1, SqlOperation::CallStoredProcedure);
        assert_eq!(classified.2.as_deref(), Some("p_arg_route"));
    }
    
}
