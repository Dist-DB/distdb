use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TlsMode {
    Off,
    #[default]
    Optional,
    Required,
}

impl TlsMode {

    pub(crate) fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" => Some(Self::Off),
            "optional" => Some(Self::Optional),
            "required" => Some(Self::Required),
            _ => None,
        }
    }

    pub(crate) fn as_common(self) -> common::TlsMode {
        match self {
            Self::Off => common::TlsMode::Off,
            Self::Optional => common::TlsMode::Optional,
            Self::Required => common::TlsMode::Required,
        }
    }
    
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientOptions {
    pub servers: Vec<String>,
    pub tls_mode: TlsMode,
    pub tls_ca_path: Option<PathBuf>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub peer_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub active_peer_id: String,
    pub session_id: Option<String>,
    pub user: Option<String>,
    pub database: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryRow {
    pub values: Vec<QueryValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum QueryValue {
    Null,
    Int(i64),
    UInt(u64),
    Float(String),
    Text(String),
    Bytes(Vec<u8>),
}

impl QueryValue {
    pub fn render_display(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Int(value) => value.to_string(),
            Self::UInt(value) => value.to_string(),
            Self::Float(value) => value.clone(),
            Self::Text(value) => value.clone(),
            Self::Bytes(value) => {
                let hex = value
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                format!("0x{hex}")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryColumnDef {
    pub ordinal: usize,
    pub name: String,
    pub sql_type: String,
    pub nullable: bool,
    pub indexed: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryTimings {
    pub server_parse_ms: u64,
    pub server_execute_ms: u64,
    pub server_total_ms: u64,
    pub network_round_trip_ms: Option<u64>,
    pub cache: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryResponse {
    pub request_id: String,
    pub status: String,
    pub columns: Vec<QueryColumnDef>,
    pub rows: Vec<QueryRow>,
    pub row_count: usize,
    pub timings: QueryTimings,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecuteResponse {
    Mutation {
        request_id: String,
        status: String,
        affected_rows: u64,
    },
    Schema {
        request_id: String,
        status: String,
        table_id: String,
        schema_revision: u64,
    },
    Query(QueryResponse),
}
