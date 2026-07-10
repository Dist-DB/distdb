use common::schema::{FieldIndex as CommonFieldIndex, FieldKind, FieldMetadata};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResponseStatus {
    Accepted,
    Applied,
    Rejected,
}

impl std::fmt::Display for ResponseStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accepted => write!(f, "accepted"),
            Self::Applied => write!(f, "applied"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
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
    #[serde(default)]
    pub metadata: Option<FieldMetadata>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connector_response_applied_constructor_sets_applied_status() {
        let response = ConnectorResponse::applied(
            "req-1",
            ConnectorResult::Mutation(MutationResult { affected_rows: 9 }),
        );

        assert_eq!(response.request_id, "req-1");
        assert_eq!(response.status, ResponseStatus::Applied);
        assert!(matches!(
            response.result,
            ConnectorResult::Mutation(MutationResult { affected_rows: 9 })
        ));
    }

    #[test]
    fn connector_response_rejected_constructor_sets_error_result() {
        let response = ConnectorResponse::rejected("req-2", "not allowed");

        assert_eq!(response.request_id, "req-2");
        assert_eq!(response.status, ResponseStatus::Rejected);
        assert_eq!(
            response.result,
            ConnectorResult::Error("not allowed".to_string())
        );
    }

    #[test]
    fn query_result_and_cache_observation_serialize_roundtrip() {
        let result = QueryResult {
            columns: vec![FieldDef {
                seqno: 1,
                field_name: "id".to_string(),
                field_type: FieldType::UInt(64),
                nullable: false,
                indexed: FieldIndex::PrimaryKey,
                default_value: None,
                metadata: None,
            }],
            rows: vec![vec![b"1".to_vec()]],
            timings: QueryTimings {
                server_parse_ms: 1,
                server_execute_ms: 2,
                server_total_ms: 3,
                network_round_trip_ms: Some(4),
                cache: Some(QueryCacheObservation::Hit {
                    lookup_ms: 5,
                    materialize_ms: 6,
                    snapshot_revision: Some(7),
                }),
            },
        };

        let encoded = bincode::serialize(&result).expect("query result should serialize");
        let decoded: QueryResult = bincode::deserialize(&encoded).expect("query result should deserialize");

        assert_eq!(decoded, result);
    }

    #[test]
    fn query_cache_variants_serialize_roundtrip() {
        let miss = QueryCacheObservation::Miss {
            lookup_ms: 10,
            reason: Some("no snapshot".to_string()),
        };
        let bypassed = QueryCacheObservation::Bypassed {
            reason: QueryCacheBypassReason::UnsupportedShape,
        };

        let miss_encoded = bincode::serialize(&miss).expect("cache miss should serialize");
        let miss_decoded: QueryCacheObservation =
            bincode::deserialize(&miss_encoded).expect("cache miss should deserialize");
        assert_eq!(miss_decoded, miss);

        let bypassed_encoded = bincode::serialize(&bypassed).expect("cache bypass should serialize");
        let bypassed_decoded: QueryCacheObservation =
            bincode::deserialize(&bypassed_encoded).expect("cache bypass should deserialize");
        assert_eq!(bypassed_decoded, bypassed);
    }
}
