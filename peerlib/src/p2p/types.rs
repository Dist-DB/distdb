#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PeerNode {
    pub id: String,
    pub addrs: Vec<String>,
    pub is_local: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WireTransactionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum WireAffinityMemberStatus {
    Online,
    Offline,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireAffinityMember {
    pub node_id: String,
    pub addrs: Vec<String>,
    pub status: WireAffinityMemberStatus,
    pub last_seen_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireDatabaseSchemaSummary {
    pub database_id: String,
    #[serde(default)]
    pub database_name: String,
    pub schema_identifier: u64,
    pub schema_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireReplicationSecuritySummary {
    pub policy_revision: u64,
    pub key_id: Option<String>,
    pub updated_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WireAffinityDocument {
    pub affinity_id: String,
    pub affinity_revision: u64,
    pub members: Vec<WireAffinityMember>,
    pub databases: Vec<WireDatabaseSchemaSummary>,
    pub replication_security: WireReplicationSecuritySummary,
}
