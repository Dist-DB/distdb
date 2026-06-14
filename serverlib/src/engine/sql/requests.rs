
use super::{
    classify, parser, SqlCompatibilityTarget, SqlDirective, SqlOperation, SqlParseError,
    SqlRequest, DEFAULT_SQL_COMPATIBILITY_TARGET,
};

use super::types::ParsedOrFallback;

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

    let statements = match parse_or_fallback(sql)? {
        ParsedOrFallback::Parsed(statements) => statements,
        ParsedOrFallback::Fallback {
            trimmed_sql,
            metadata: (directive, operation, object_name),
        } => {
            return Ok(vec![SqlRequest {
                database_id,
                sql: trimmed_sql,
                directive,
                operation,
                object_name,
                compatibility_target,
            }]);
        }
    };

    if statements.is_empty() {
        return Err(SqlParseError::EmptyStatement);
    }

    statements
        .into_iter()
        .map(|statement| {
            let statement_sql = statement.to_string();
            let (directive, operation, object_name) =
                classify::classify_statement(&statement, &statement_sql)?;

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

    let parsed = match parse_or_fallback(statement)? {
        ParsedOrFallback::Parsed(parsed) => parsed,
        ParsedOrFallback::Fallback { metadata, .. } => return Ok(metadata),
    };

    let single = parsed.first().ok_or(SqlParseError::EmptyStatement)?;

    if parsed.len() > 1 {
        return Err(SqlParseError::UnsupportedStatement(
            "expected a single statement for metadata extraction".to_string(),
        ));
    }

    let statement_sql = single.to_string();
    classify::classify_statement(single, &statement_sql)
}

pub fn sql_directive_for_statement(statement: &str) -> Result<SqlDirective, SqlParseError> {
    let (directive, _, _) = sql_statement_metadata(statement)?;
    Ok(directive)
}

fn parse_or_fallback(sql: &str) -> Result<ParsedOrFallback, SqlParseError> {
    parser::parse_or_fallback(sql)
}


#[cfg(test)]
#[path = "requests_test.rs"]
mod tests;
