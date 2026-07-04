use crate::p2p::types::{PeerNode, WireAffinityDocument, WireTransactionId};

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
    pub transaction_id: WireTransactionId,
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
    pub document: Option<WireAffinityDocument>,
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
    pub from_transaction_id: Option<WireTransactionId>,
    pub from_stream_transaction_ids: Vec<(String, WireTransactionId)>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TransactionsSinceResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub transactions: Vec<String>, // SQL statements (UPDATE, DELETE, INSERT, etc.)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TlsCaDistribution {
    pub issuer_node_id: String,
    pub ca_cert_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TlsCertEnrollRequest {
    pub request_id: String,
    pub requester_node_id: String,
    pub csr_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TlsCertEnrollResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub node_cert_pem: Option<String>,
    pub ca_cert_pem: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServiceAnnounce {
    pub node_id: String,
    #[serde(default)]
    pub addrs: Vec<String>,
    #[serde(default)]
    pub services: Vec<String>,
    pub timestamp_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableLockState {
    pub owner_node_id: String,
    pub owner_session_id: String,
    pub database_id: String,
    #[serde(default)]
    pub table_ids: Vec<String>,
    pub locked: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ServiceMessage {
    NodeAnnounce(PeerNode),
    Publication {
        subscription_key: String,
        event: PublicationEvent,
    },
    TransactionsSince {
        database_id: String,
        from: Option<WireTransactionId>,
    },
    AffinityJoinRequest(AffinityJoinRequest),
    AffinityJoinResponse(AffinityJoinResponse),
    SchemaCatalogRequest(SchemaCatalogRequest),
    SchemaCatalogResponse(SchemaCatalogResponse),
    DataSnapshotRequest(DataSnapshotRequest),
    DataSnapshotResponse(DataSnapshotResponse),
    TransactionsSinceRequest(TransactionsSinceRequest),
    TransactionsSinceResponse(TransactionsSinceResponse),
    TlsCaDistribution(TlsCaDistribution),
    TlsCertEnrollRequest(TlsCertEnrollRequest),
    TlsCertEnrollResponse(TlsCertEnrollResponse),
    ServiceAnnounce(ServiceAnnounce),
    TableLockState(TableLockState),
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