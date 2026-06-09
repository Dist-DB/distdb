#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResponseStatus {
    Accepted,
    Applied,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ConnectorResponse {
    pub request_id: String,
    pub status: ResponseStatus,
    pub result: ConnectorResult,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConnectorResult {
    Mutation(MutationResult),
    Query(QueryResult),
    Schema(SchemaResult),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MutationResult {
    pub affected_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Vec<u8>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaResult {
    pub table_id: String,
    pub schema_revision: u64,
}

impl ConnectorResponse {
    pub fn applied(request_id: impl Into<String>, result: ConnectorResult) -> Self {
        Self {
            request_id: request_id.into(),
            status: ResponseStatus::Applied,
            result,
        }
    }

    pub fn rejected(request_id: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            request_id: request_id.into(),
            status: ResponseStatus::Rejected,
            result: ConnectorResult::Error(message.into()),
        }
    }
}
