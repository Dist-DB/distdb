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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AffinitySyncPhase {
    ControlPlane,
    SchemaCatalog,
    DataSnapshot,
    WalCatchup,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinitySyncStep {
    pub phase: AffinitySyncPhase,
    pub database_id: Option<String>,
    pub schema_identifier: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AffinityProcessorState {
    Unconfigured,
    JoinRequested,
    Syncing(AffinitySyncPhase),
    Ready,
    Degraded(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AffinityProcessorError {
    MissingAffinityDocument,
    SchemaValidationPartnerRequired,
}

impl std::fmt::Display for AffinityProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingAffinityDocument => {
                write!(f, "missing affinity document for replication planning")
            }
            Self::SchemaValidationPartnerRequired => write!(
                f,
                "schema change requires at least one reachable partner in same affinity"
            ),
        }
    }
}

impl std::error::Error for AffinityProcessorError {}

/// Tracks replication progress for an affinity, enabling resumable sync after crashes
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckpointMetadata {
    pub affinity_id: String,
    pub revision: u64,
    pub current_phase: AffinitySyncPhase,
    pub completed_step_indices: Vec<usize>,
    pub last_update_epoch_ms: u64,
}

impl CheckpointMetadata {
    
    pub fn new(affinity_id: String, current_phase: AffinitySyncPhase) -> Self {
        Self {
            affinity_id,
            revision: 1,
            current_phase,
            completed_step_indices: Vec::new(),
            last_update_epoch_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
        }
    }

    pub fn mark_step_completed(&mut self, step_index: usize) {
        if !self.completed_step_indices.contains(&step_index) {
            self.completed_step_indices.push(step_index);
            self.completed_step_indices.sort();
            self.last_update_epoch_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
        }
    }

    pub fn is_step_completed(&self, step_index: usize) -> bool {
        self.completed_step_indices.contains(&step_index)
    }

    pub fn next_incomplete_step(&self, total_steps: usize) -> Option<usize> {
        (0..total_steps).find(|idx| !self.is_step_completed(*idx))
    }

    pub fn progress_percentage(&self, total_steps: usize) -> u64 {
        if total_steps == 0 {
            return 100;
        }
        ((self.completed_step_indices.len() as u64 * 100) / total_steps as u64).min(100)
    }

}

#[derive(Debug, Clone)]
pub struct AffinityProcessor {
    local_node_id: NodeId,
    state: AffinityProcessorState,
    document: Option<AffinityDocument>,
    checkpoint: Option<CheckpointMetadata>,
}

impl AffinityProcessor {
    pub fn new(local_node_id: NodeId) -> Self {
        Self {
            local_node_id,
            state: AffinityProcessorState::Unconfigured,
            document: None,
            checkpoint: None,
        }
    }

    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    pub fn state(&self) -> &AffinityProcessorState {
        &self.state
    }

    pub fn document(&self) -> Option<&AffinityDocument> {
        self.document.as_ref()
    }

    pub fn begin_join(&mut self) {
        self.state = AffinityProcessorState::JoinRequested;
    }

    pub fn apply_affinity_document(&mut self, document: AffinityDocument) {
        self.document = Some(document);
        self.state = AffinityProcessorState::Syncing(AffinitySyncPhase::ControlPlane);
    }

    pub fn build_sync_plan(&self) -> Result<Vec<AffinitySyncStep>, AffinityProcessorError> {

        let document = self
            .document
            .as_ref()
            .ok_or(AffinityProcessorError::MissingAffinityDocument)?;

        let mut plan = Vec::new();
        plan.push(AffinitySyncStep {
            phase: AffinitySyncPhase::ControlPlane,
            database_id: None,
            schema_identifier: None,
        });

        let mut databases = document.databases.clone();
        databases.sort_by_key(|db| std::cmp::Reverse(db.schema_identifier));

        for database in databases {

            plan.push(AffinitySyncStep {
                phase: AffinitySyncPhase::SchemaCatalog,
                database_id: Some(database.database_id.clone()),
                schema_identifier: Some(database.schema_identifier),
            });
            
            plan.push(AffinitySyncStep {
                phase: AffinitySyncPhase::DataSnapshot,
                database_id: Some(database.database_id.clone()),
                schema_identifier: Some(database.schema_identifier),
            });
            
            plan.push(AffinitySyncStep {
                phase: AffinitySyncPhase::WalCatchup,
                database_id: Some(database.database_id),
                schema_identifier: Some(database.schema_identifier),
            });

        }

        Ok(plan)

    }

    pub fn set_ready(&mut self) {
        self.state = AffinityProcessorState::Ready;
    }

    pub fn set_degraded(&mut self, reason: impl Into<String>) {
        self.state = AffinityProcessorState::Degraded(reason.into());
    }

    pub fn validate_schema_change_partner_count(
        &self,
        reachable_partner_count: usize,
    ) -> Result<(), AffinityProcessorError> {
        if reachable_partner_count == 0 {
            return Err(AffinityProcessorError::SchemaValidationPartnerRequired);
        }
        Ok(())
    }

    /// Initialize checkpoint for tracking replication progress
    pub fn initialize_checkpoint(&mut self, phase: AffinitySyncPhase) {
        if let Some(doc) = &self.document {
            let checkpoint = CheckpointMetadata::new(doc.affinity_id.clone(), phase);
            self.checkpoint = Some(checkpoint);
            log::debug!(
                "initialized checkpoint affinity_id={} phase={:?}",
                doc.affinity_id,
                phase
            );
        }
    }

    /// Load existing checkpoint, useful for resuming interrupted replication
    pub fn restore_checkpoint(&mut self, checkpoint: CheckpointMetadata) {
        log::info!(
            "restoring checkpoint affinity_id={} phase={:?} completed_steps={}",
            checkpoint.affinity_id,
            checkpoint.current_phase,
            checkpoint.completed_step_indices.len()
        );
        self.checkpoint = Some(checkpoint);
    }

    /// Get current checkpoint
    pub fn checkpoint(&self) -> Option<&CheckpointMetadata> {
        self.checkpoint.as_ref()
    }

    /// Mark a sync step as completed
    pub fn mark_sync_step_completed(&mut self, step_index: usize) {
        if let Some(ref mut checkpoint) = self.checkpoint {
            checkpoint.mark_step_completed(step_index);
            log::debug!(
                "marked step completed affinity_id={} step_index={} progress={}%",
                checkpoint.affinity_id,
                step_index,
                checkpoint.progress_percentage(10) // placeholder total
            );
        }
    }

    /// Get next incomplete sync step, useful for resuming
    pub fn next_incomplete_sync_step(&self, total_steps: usize) -> Option<usize> {
        self.checkpoint
            .as_ref()
            .and_then(|cp| cp.next_incomplete_step(total_steps))
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_document() -> AffinityDocument {
        AffinityDocument {
            affinity_id: "finance-eu-01".to_string(),
            affinity_revision: 7,
            members: vec![AffinityMember {
                node_id: NodeId("sam01".to_string()),
                addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
                status: AffinityMemberStatus::Online,
                last_seen_epoch_ms: 10,
            }],
            databases: vec![
                DatabaseSchemaSummary {
                    database_id: "orders".to_string(),
                    schema_identifier: 200,
                    schema_hash: Some("abc".to_string()),
                },
                DatabaseSchemaSummary {
                    database_id: "billing".to_string(),
                    schema_identifier: 100,
                    schema_hash: Some("def".to_string()),
                },
            ],
            replication_security: ReplicationSecuritySummary {
                policy_revision: 1,
                key_id: Some("k-2026-06".to_string()),
                updated_epoch_ms: 11,
            },
        }
    }

    #[test]
    fn processor_builds_sync_plan_sorted_by_schema_identifier() {
        let mut processor = AffinityProcessor::new(NodeId("sam03".to_string()));
        processor.begin_join();
        processor.apply_affinity_document(sample_document());

        let plan = processor.build_sync_plan().expect("plan should build");
        assert_eq!(plan[0].phase, AffinitySyncPhase::ControlPlane);
        assert_eq!(plan[1].database_id.as_deref(), Some("orders"));
        assert_eq!(plan[4].database_id.as_deref(), Some("billing"));
    }

    #[test]
    fn schema_change_requires_at_least_one_partner() {
        let processor = AffinityProcessor::new(NodeId("sam01".to_string()));
        let result = processor.validate_schema_change_partner_count(0);
        assert!(matches!(
            result,
            Err(AffinityProcessorError::SchemaValidationPartnerRequired)
        ));
        assert!(processor.validate_schema_change_partner_count(1).is_ok());
    }

    #[test]
    fn upsert_database_schema_keeps_highest_identifier() {
        let mut document = sample_document();
        document.upsert_database_schema(DatabaseSchemaSummary {
            database_id: "orders".to_string(),
            schema_identifier: 150,
            schema_hash: Some("older".to_string()),
        });

        assert_eq!(document.database_schema_identifier("orders"), Some(200));

        document.upsert_database_schema(DatabaseSchemaSummary {
            database_id: "orders".to_string(),
            schema_identifier: 300,
            schema_hash: Some("newer".to_string()),
        });

        assert_eq!(document.database_schema_identifier("orders"), Some(300));
    }
}