use crate::core::identity::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AffinityMemberStatus {
    Online,
    Offline,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AffinityMember {
    pub node_id: NodeId,
    pub addrs: Vec<String>,
    pub status: AffinityMemberStatus,
    pub last_seen_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DatabaseSchemaSummary {
    pub database_id: String,
    pub schema_identifier: u64,
    pub schema_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReplicationSecuritySummary {
    pub policy_revision: u64,
    pub key_id: Option<String>,
    pub updated_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AffinityDocument {
    pub affinity_id: String,
    pub affinity_revision: u64,
    pub members: Vec<AffinityMember>,
    pub databases: Vec<DatabaseSchemaSummary>,
    pub replication_security: ReplicationSecuritySummary,
}

impl AffinityDocument {

    pub fn upsert_member(&mut self, member: AffinityMember) {

        if let Some(existing) = self
            .members
            .iter_mut()
            .find(|existing| existing.node_id == member.node_id)
        {
            *existing = member;
            return;
        }

        self.members.push(member);

    }

    pub fn upsert_database_schema(&mut self, incoming: DatabaseSchemaSummary) {

        if let Some(existing) = self
            .databases
            .iter_mut()
            .find(|existing| existing.database_id == incoming.database_id)
        {
            if incoming.schema_identifier >= existing.schema_identifier {
                *existing = incoming;
            }
            return;
        }

        self.databases.push(incoming);

    }

    pub fn database_schema_identifier(&self, database_id: &str) -> Option<u64> {

        self.databases
            .iter()
            .find(|entry| entry.database_id == database_id)
            .map(|entry| entry.schema_identifier)

    }
    
}