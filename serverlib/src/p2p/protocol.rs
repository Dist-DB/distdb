use crate::core::cluster::NodeDescriptor;
use crate::engine::affinity::AffinityDocument;
use crate::engine::database::transaction::TransactionId;

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
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SchemaCatalogResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub schema_definitions: Vec<String>, // SQL CREATE TABLE statements
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