use crate::schema::{FieldSpec, SchemaChangeRequest};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConnectorRequest {
    pub request_id: String,
    pub command: ConnectorCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConnectorCommand {
    CreateDatabase {
        database_name: String,
    },
    Schema {
        database_id: String,
        command: SchemaCommand,
    },
    Mutation {
        database_id: String,
        mutation: DataMutation,
    },
    Query {
        query: DataQuery,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SchemaCommand {
    CreateTable {
        table_id: String,
        fields: Vec<FieldSpec>,
    },
    AlterTable {
        change: SchemaChangeRequest,
    },
    DropTable {
        table_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataMutation {
    Insert {
        table_id: String,
        values: Vec<FieldValue>,
    },
    Update {
        table_id: String,
        values: Vec<FieldValue>,
        predicate_sql: Option<String>,
    },
    Delete {
        table_id: String,
        predicate_sql: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldValue {
    pub name: String,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DataQuery {
    pub database_id: String,
    pub sql: String,
}

impl ConnectorRequest {
    pub fn new(request_id: impl Into<String>, command: ConnectorCommand) -> Self {
        Self {
            request_id: request_id.into(),
            command,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_serializes_roundtrip() {
        let req = ConnectorRequest::new(
            "req-1",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let bytes = bincode::serialize(&req).expect("request should serialize");
        let decoded: ConnectorRequest =
            bincode::deserialize(&bytes).expect("request should deserialize");

        assert_eq!(decoded, req);
    }
}
