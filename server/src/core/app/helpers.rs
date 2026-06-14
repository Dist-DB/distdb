use connector::{ConnectorCommand, DataMutation, SchemaCommand};

#[derive(Debug, Clone, Copy)]
pub(super) enum SessionTxMarkerType {
    Begin,
    Commit,
    Rollback,
    DisconnectRollback,
    CommitFailed,
}

impl SessionTxMarkerType {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            SessionTxMarkerType::Begin => "begin",
            SessionTxMarkerType::Commit => "commit",
            SessionTxMarkerType::Rollback => "rollback",
            SessionTxMarkerType::DisconnectRollback => "disconnect_rollback",
            SessionTxMarkerType::CommitFailed => "commit_failed",
        }
    }
}

pub(super) fn is_staged_dml_query(query: &connector::DataQuery) -> bool {
    
    let Ok(parsed) = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) else {
        return false;
    };

    if parsed.len() != 1 {
        return false;
    }

    matches!(
        parsed[0].operation,
        serverlib::SqlOperation::Insert
            | serverlib::SqlOperation::Update
            | serverlib::SqlOperation::Delete
    )

}

pub(super) fn is_transactional_read_query(query: &connector::DataQuery) -> bool {

    let Ok(parsed) = serverlib::parse_mysql8_sql_requests(&query.sql, &query.database_id) else {
        return false;
    };

    if parsed.len() != 1 {
        return false;
    }

    matches!(parsed[0].operation, serverlib::SqlOperation::Select)

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CommandKind {
    CreateDatabase,
    Query,
    Schema,
    Mutation,
}

#[derive(Debug, Clone)]
pub(super) struct CommandInfo {
    pub(super) kind: CommandKind,
    pub(super) path: String,
}

pub(super) fn command_info(command: &ConnectorCommand) -> CommandInfo {

    match command {

        ConnectorCommand::CreateDatabase { database_name } => CommandInfo {
            kind: CommandKind::CreateDatabase,
            path: format!("create_database:{database_name}"),
        },

        ConnectorCommand::Query { query } => CommandInfo {
            kind: CommandKind::Query,
            path: format!("query:{}", query.database_id),
        },

        ConnectorCommand::Schema {
            database_id,
            command,
        } => {
            let path = match command {
                SchemaCommand::CreateTable { table_id, .. } => {
                    format!("schema:create_table:{database_id}:{table_id}")
                }
                SchemaCommand::AlterTable { change } => {
                    format!("schema:alter_table:{database_id}:{}", change.table_id)
                }
                SchemaCommand::DropTable { table_id } => {
                    format!("schema:drop_table:{database_id}:{table_id}")
                }
            };

            CommandInfo {
                kind: CommandKind::Schema,
                path,
            }
        },

        ConnectorCommand::Mutation {
            database_id,
            mutation,
        } => {
            let path = match mutation {
                DataMutation::Insert { table_id, .. } => {
                    format!("mutation:insert:{database_id}:{table_id}")
                }
                DataMutation::Update { table_id, .. } => {
                    format!("mutation:update:{database_id}:{table_id}")
                }
                DataMutation::Delete { table_id, .. } => {
                    format!("mutation:delete:{database_id}:{table_id}")
                }
            };

            CommandInfo {
                kind: CommandKind::Mutation,
                path,
            }
        },

    }
    
}

