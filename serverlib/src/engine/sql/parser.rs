use sqlparser::ast::Statement;
use sqlparser::dialect::{GenericDialect, MySqlDialect};
use sqlparser::parser::Parser;

use super::classify;
use super::types::ParsedOrFallback;
use super::SqlParseError;

pub(super) fn parse_mysql_statements(sql: &str) -> Result<Vec<Statement>, SqlParseError> {

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

pub(super) fn parse_or_fallback(sql: &str) -> Result<ParsedOrFallback, SqlParseError> {
    
    match parse_mysql_statements(sql) {

        Ok(statements) => Ok(ParsedOrFallback::Parsed(statements)),
        
        Err(parse_error) => {
            let trimmed = sql.trim();
            if let Some(metadata) = classify::classify_text_fallback(trimmed) {
                Ok(ParsedOrFallback::Fallback {
                    trimmed_sql: trimmed.to_string(),
                    metadata,
                })
            } else {
                Err(parse_error)
            }
        }

    }

}
