use crate::core::identity::NodeId;

use super::{
    AffinityDocument, AffinityProcessorError, AffinityProcessorState, AffinitySyncPhase,
    AffinitySyncStep, CheckpointMetadata,
};

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
                checkpoint.progress_percentage(10)
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