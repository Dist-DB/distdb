use crate::core::cluster::NodeDescriptor;
use crate::engine::affinity::AffinityDocument;
use crate::engine::database::transaction::TransactionId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AffinityReplicationAction {
    JoinRequest,
    JoinResponse,
    SchemaCatalogRequest,
    SchemaCatalogResponse,
    DataSnapshotRequest,
    DataSnapshotResponse,
    TransactionsSinceRequest,
    TransactionsSinceResponse,
}

impl AffinityReplicationAction {

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::JoinRequest => "affinity.join.request",
            Self::JoinResponse => "affinity.join.response",
            Self::SchemaCatalogRequest => "affinity.schema.request",
            Self::SchemaCatalogResponse => "affinity.schema.response",
            Self::DataSnapshotRequest => "affinity.snapshot.request",
            Self::DataSnapshotResponse => "affinity.snapshot.response",
            Self::TransactionsSinceRequest => "affinity.wal.request",
            Self::TransactionsSinceResponse => "affinity.wal.response",
        }
    }
    
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EventType {
    DataChanged,
    SchemaChanged,
    SecurityChanged,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublicationEvent {
    pub timestamp_epoch_ms: u64,
    pub service_id: String,
    pub transaction_id: TransactionId,
    pub event_type: EventType,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AffinityJoinRequest {
    pub request_id: String,
    pub affinity_id: String,
    pub requester_node_id: String,
    #[serde(default)]
    pub requester_addrs: Vec<String>,
    pub affinity_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AffinityJoinResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub document: Option<AffinityDocument>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaCatalogRequest {
    pub request_id: String,
    pub affinity_id: String,
    pub database_id: String,
    pub expected_schema_identifier: u64,
    pub expected_schema_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaCatalogResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub schema_identifier: u64,
    pub schema_hash: Option<String>,
    pub schema_definitions: Vec<String>, // SQL CREATE TABLE statements
    #[serde(default)]
    pub database_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DataSnapshotRequest {
    pub request_id: String,
    pub affinity_id: String,
    pub database_id: String,
    pub table_names: Vec<String>, // Tables to snapshot
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DataSnapshotResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub snapshot_data: Vec<(String, Vec<String>)>, // (table_name, [INSERT statements for rows])
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionsSinceRequest {
    pub request_id: String,
    pub affinity_id: String,
    pub database_id: String,
    pub from_transaction_id: Option<TransactionId>,
    pub from_stream_transaction_ids: Vec<(String, TransactionId)>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionsSinceResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub transactions: Vec<String>, // SQL statements (UPDATE, DELETE, INSERT, etc.)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ServiceMessage {
    NodeAnnounce(NodeDescriptor),
    Publication {
        subscription_key: String,
        event: PublicationEvent,
    },
    TransactionsSince {
        database_id: String,
        from: Option<TransactionId>,
    },
    AffinityJoinRequest(AffinityJoinRequest),
    AffinityJoinResponse(AffinityJoinResponse),
    SchemaCatalogRequest(SchemaCatalogRequest),
    SchemaCatalogResponse(SchemaCatalogResponse),
    DataSnapshotRequest(DataSnapshotRequest),
    DataSnapshotResponse(DataSnapshotResponse),
    TransactionsSinceRequest(TransactionsSinceRequest),
    TransactionsSinceResponse(TransactionsSinceResponse),
}

impl ServiceMessage {
    pub fn affinity_replication_action(&self) -> Option<AffinityReplicationAction> {
        match self {
            Self::AffinityJoinRequest(_) => Some(AffinityReplicationAction::JoinRequest),
            Self::AffinityJoinResponse(_) => Some(AffinityReplicationAction::JoinResponse),
            Self::SchemaCatalogRequest(_) => Some(AffinityReplicationAction::SchemaCatalogRequest),
            Self::SchemaCatalogResponse(_) => Some(AffinityReplicationAction::SchemaCatalogResponse),
            Self::DataSnapshotRequest(_) => Some(AffinityReplicationAction::DataSnapshotRequest),
            Self::DataSnapshotResponse(_) => Some(AffinityReplicationAction::DataSnapshotResponse),
            Self::TransactionsSinceRequest(_) => Some(AffinityReplicationAction::TransactionsSinceRequest),
            Self::TransactionsSinceResponse(_) => Some(AffinityReplicationAction::TransactionsSinceResponse),
            _ => None,
        }
    }
}