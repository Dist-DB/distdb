use crate::core::{ConnectorCommand, DataMutation, DataQuery, SchemaCommand};
use crate::schema::{FieldSpec, SchemaChangeRequest};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCompatibilityTarget {
    Mysql80,
}

pub const DEFAULT_SQL_COMPATIBILITY_TARGET: SqlCompatibilityTarget = SqlCompatibilityTarget::Mysql80;

pub const MYSQL_8_COMPATIBILITY_NOTE: &str = "mysql 8.0.x compatibility target";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlAction {
    UseDatabase(String),
    Execute(ConnectorCommand),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlStatementPlan {
    pub actions: Vec<SqlAction>,
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

pub fn parse_mysql8_sql_statement_plan(
    sql: &str,
    default_database: impl Into<String>,
) -> Result<SqlStatementPlan, SqlParseError> {
    parse_sql_statement_plan(sql, default_database, DEFAULT_SQL_COMPATIBILITY_TARGET)
}

pub fn parse_sql_statement_plan(
    sql: &str,
    default_database: impl Into<String>,
    compatibility_target: SqlCompatibilityTarget,
) -> Result<SqlStatementPlan, SqlParseError> {
    let mut active_database = default_database.into();
    let mut actions = Vec::new();

    for statement in sql.split(';') {
        let statement = statement.trim();
        if statement.is_empty() {
            continue;
        }

        let lowered = statement.to_lowercase();

        if lowered == "help" || lowered == ".help" {
            continue;
        }

        if lowered == "\\q" || lowered == "quit" || lowered == "exit" {
            continue;
        }

        if let Some(database) = statement.strip_prefix("use ") {
            let database = database.trim();
            if database.is_empty() {
                return Err(SqlParseError::MissingIdentifier {
                    keyword: "use",
                    statement: statement.to_string(),
                });
            }

            active_database = database.to_string();
            actions.push(SqlAction::UseDatabase(active_database.clone()));
            continue;
        }

        if let Some(database_name) = statement.strip_prefix("create database ") {
            let database_name = database_name.trim();
            if database_name.is_empty() {
                return Err(SqlParseError::MissingIdentifier {
                    keyword: "create database",
                    statement: statement.to_string(),
                });
            }

            actions.push(SqlAction::Execute(ConnectorCommand::CreateDatabase {
                database_name: database_name.trim_end_matches(';').trim().to_string(),
            }));
            continue;
        }

        if lowered.starts_with("select ") {
            actions.push(SqlAction::Execute(ConnectorCommand::Query {
                query: DataQuery {
                    database_id: active_database.clone(),
                    sql: statement.to_string(),
                },
            }));
            continue;
        }

        if lowered.starts_with("insert ") {
            actions.push(SqlAction::Execute(ConnectorCommand::Mutation {
                database_id: active_database.clone(),
                mutation: DataMutation::Insert {
                    table_id: extract_table_name_after_keyword(statement, "into")
                        .unwrap_or_else(|| "unknown".to_string()),
                    values: Vec::new(),
                },
            }));
            continue;
        }

        if lowered.starts_with("update ") {
            actions.push(SqlAction::Execute(ConnectorCommand::Mutation {
                database_id: active_database.clone(),
                mutation: DataMutation::Update {
                    table_id: extract_table_name_after_keyword(statement, "update")
                        .unwrap_or_else(|| "unknown".to_string()),
                    values: Vec::new(),
                    predicate_sql: extract_predicate(statement),
                },
            }));
            continue;
        }

        if lowered.starts_with("delete ") {
            actions.push(SqlAction::Execute(ConnectorCommand::Mutation {
                database_id: active_database.clone(),
                mutation: DataMutation::Delete {
                    table_id: extract_table_name_after_keyword(statement, "from")
                        .unwrap_or_else(|| "unknown".to_string()),
                    predicate_sql: extract_predicate(statement),
                },
            }));
            continue;
        }

        if lowered.starts_with("create table ") {
            let table_id = extract_table_name_after_keyword(statement, "table").ok_or_else(|| {
                SqlParseError::MissingIdentifier {
                    keyword: "create table",
                    statement: statement.to_string(),
                }
            })?;

            actions.push(SqlAction::Execute(ConnectorCommand::Schema {
                database_id: active_database.clone(),
                command: SchemaCommand::CreateTable {
                    table_id,
                    fields: Vec::<FieldSpec>::new(),
                },
            }));
            continue;
        }

        if lowered.starts_with("alter table ") {
            let table_id = extract_table_name_after_keyword(statement, "table").ok_or_else(|| {
                SqlParseError::MissingIdentifier {
                    keyword: "alter table",
                    statement: statement.to_string(),
                }
            })?;

            actions.push(SqlAction::Execute(ConnectorCommand::Schema {
                database_id: active_database.clone(),
                command: SchemaCommand::AlterTable {
                    change: SchemaChangeRequest::new(table_id),
                },
            }));
            continue;
        }

        if lowered.starts_with("drop table ") {
            let table_id = extract_table_name_after_keyword(statement, "table").ok_or_else(|| {
                SqlParseError::MissingIdentifier {
                    keyword: "drop table",
                    statement: statement.to_string(),
                }
            })?;

            actions.push(SqlAction::Execute(ConnectorCommand::Schema {
                database_id: active_database.clone(),
                command: SchemaCommand::DropTable { table_id },
            }));
            continue;
        }

        return Err(SqlParseError::UnsupportedStatement(statement.to_string()));
    }

    if actions.is_empty() {
        return Err(SqlParseError::EmptyStatement);
    }

    Ok(SqlStatementPlan {
        actions,
        compatibility_target,
    })
}

fn extract_table_name_after_keyword(sql: &str, keyword: &str) -> Option<String> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    for (idx, part) in parts.iter().enumerate() {
        if part.eq_ignore_ascii_case(keyword) {
            return parts
                .get(idx + 1)
                .map(|value| value.trim_matches(|c: char| c == ';' || c == ',' || c == '(' || c == ')'))
                .filter(|value| !value.is_empty())
                .map(ToString::to_string);
        }
    }
    None
}

fn extract_predicate(sql: &str) -> Option<String> {
    let lowered = sql.to_lowercase();
    lowered.find(" where ").map(|idx| sql[idx + 7..].trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_multiple_statements_into_actions() {
        let plan = parse_mysql8_sql_statement_plan(
            "use analytics; select * from events; create database archive",
            "main",
        )
        .expect("plan should parse");

        assert_eq!(plan.actions.len(), 3);
        assert_eq!(plan.compatibility_target, SqlCompatibilityTarget::Mysql80);
        assert!(matches!(plan.actions[0], SqlAction::UseDatabase(ref db) if db == "analytics"));
        assert!(matches!(plan.actions[1], SqlAction::Execute(ConnectorCommand::Query { .. })));
        assert!(matches!(plan.actions[2], SqlAction::Execute(ConnectorCommand::CreateDatabase { .. })));
    }
}