use common::schema::{FieldIndex as CommonFieldIndex, FieldKind};

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

pub type FieldType = FieldKind;
pub type FieldIndex = CommonFieldIndex;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FieldDef {
    pub seqno: u32,
    pub field_name: String,
    pub field_type: FieldType,
    pub nullable: bool,
    pub indexed: FieldIndex,
    pub default_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryResult {
    pub columns: Vec<FieldDef>,
    pub rows: Vec<Vec<Vec<u8>>>,
    pub timings: QueryTimings,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueryTimings {
    pub server_parse_ms: u64,
    pub server_execute_ms: u64,
    pub server_total_ms: u64,
    pub network_round_trip_ms: Option<u64>,
    #[serde(default)]
    pub cache: Option<QueryCacheObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryCacheObservation {
    Hit {
        lookup_ms: u64,
        materialize_ms: u64,
        snapshot_revision: Option<u64>,
    },
    Miss {
        lookup_ms: u64,
        reason: Option<String>,
    },
    Bypassed {
        reason: QueryCacheBypassReason,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum QueryCacheBypassReason {
    NonTableSource,
    ViewSource,
    CacheDisabled,
    UnsupportedShape,
    NotApplicable,
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
