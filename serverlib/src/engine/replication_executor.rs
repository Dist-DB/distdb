use crate::engine::affinity::{AffinityProcessor, AffinitySyncPhase};
use crate::helpers::error::{Result, ServerLibError};

/// Handles execution of replication phases for affinity synchronization
#[derive(Debug, Clone)]
pub struct ReplicationPhaseExecutor {
    current_sync_index: usize,
}


impl Default for ReplicationPhaseExecutor {
    fn default() -> Self {
        Self::new()
    }
}


impl ReplicationPhaseExecutor {

    pub fn new() -> Self {
        Self {
            current_sync_index: 0,
        }
    }

    pub fn current_sync_index(&self) -> usize {
        self.current_sync_index
    }

    /// Reset the executor so a new sync plan can be walked from the beginning.
    pub fn reset(&mut self) {
        self.current_sync_index = 0;
    }

    /// Execute the next replication phase based on processor state
    pub fn execute_next_phase(
        &mut self,
        processor: &mut AffinityProcessor,
        sync_plan: &[crate::engine::affinity::AffinitySyncStep],
    ) -> Result<bool> {
        
        if self.current_sync_index >= sync_plan.len() {
            log::info!("all replication phases completed");
            return Ok(true);
        }

        let step = &sync_plan[self.current_sync_index];

        match step.phase {
            
            AffinitySyncPhase::ControlPlane => {
                self.execute_control_plane_phase(processor, step)?;
            },
            
            AffinitySyncPhase::SchemaCatalog => {
                self.execute_schema_catalog_phase(processor, step)?;
            },
            
            AffinitySyncPhase::DataSnapshot => {
                self.execute_data_snapshot_phase(processor, step)?;
            },
            
            AffinitySyncPhase::WalCatchup => {
                self.execute_wal_catchup_phase(processor, step)?;
            },

        }

        processor.mark_sync_step_completed(self.current_sync_index);
        self.current_sync_index += 1;

        Ok(self.current_sync_index >= sync_plan.len())

    }

    /// Control Plane: Initialize membership and document agreement
    /// This is mostly done by the join sequence, but we mark it complete here
    fn execute_control_plane_phase(
        &self,
        processor: &AffinityProcessor,
        _step: &crate::engine::affinity::AffinitySyncStep,
    ) -> Result<()> {

        log::info!(
            "executing control plane replication phase local_node={:?}",
            processor.local_node_id()
        );

        // Control plane work is done during join negotiation.
        // Here we just verify the processor has a valid document.
        if processor.document().is_none() {
            return Err(ServerLibError::InvalidState(
                "control plane phase requires valid affinity document".to_string(),
            ));
        }

        log::debug!("control plane phase completed - affinity document is established");
        Ok(())

    }

    /// Schema Catalog: Sync schema metadata for each database in the affinity
    fn execute_schema_catalog_phase(
        &self,
        processor: &AffinityProcessor,
        step: &crate::engine::affinity::AffinitySyncStep,
    ) -> Result<()> {

        let Some(database_id) = &step.database_id else {
            return Err(ServerLibError::InvalidState(
                "schema catalog phase requires database_id".to_string(),
            ));
        };

        let Some(schema_identifier) = step.schema_identifier else {
            return Err(ServerLibError::InvalidState(
                "schema catalog phase requires schema_identifier".to_string(),
            ));
        };

        log::info!(
            "executing schema catalog sync affinity={:?} database={} schema_id={}",
            processor.document().map(|d| &d.affinity_id),
            database_id,
            schema_identifier
        );

        let document = processor.document().ok_or_else(|| {
            ServerLibError::InvalidState(
                "schema catalog phase requires valid affinity document".to_string(),
            )
        })?;

        let document_schema_identifier = document
            .database_schema_identifier(database_id)
            .ok_or_else(|| {
                ServerLibError::InvalidState(format!(
                    "schema catalog phase requires database '{}' in affinity document",
                    database_id
                ))
            })?;

        if document_schema_identifier != schema_identifier {
            return Err(ServerLibError::InvalidState(format!(
                "schema catalog phase schema mismatch for database '{}': plan={} document={}",
                database_id,
                schema_identifier,
                document_schema_identifier
            )));
        }

        log::debug!(
            "schema catalog phase completed database={} schema_id={}",
            database_id,
            schema_identifier
        );

        Ok(())

    }

    /// Data Snapshot: Replicate initial data snapshot from peers
    fn execute_data_snapshot_phase(
        &self,
        processor: &AffinityProcessor,
        step: &crate::engine::affinity::AffinitySyncStep,
    ) -> Result<()> {

        let Some(database_id) = &step.database_id else {
            return Err(ServerLibError::InvalidState(
                "data snapshot phase requires database_id".to_string(),
            ));
        };

        let Some(schema_identifier) = step.schema_identifier else {
            return Err(ServerLibError::InvalidState(
                "data snapshot phase requires schema_identifier".to_string(),
            ));
        };

        log::info!(
            "executing data snapshot sync affinity={:?} database={} schema_id={}",
            processor.document().map(|d| &d.affinity_id),
            database_id,
            schema_identifier
        );

        // For MVP single-node: Phase completes when data is applied locally
        // For multi-node: Would query peers via DataSnapshotRequest and apply received rows
        // 
        // Phase Steps:
        // 1. Query peers for data snapshot of database at schema_identifier
        // 2. Receive snapshot responses containing INSERT statements for all rows
        // 3. Execute INSERT statements in local database
        // 4. Verify record counts match peers
        //
        // For now (single-node), just mark as complete since there's no peer to sync from

        log::debug!(
            "data snapshot phase completed database={} schema_id={}",
            database_id,
            schema_identifier
        );

        Ok(())

    }

    /// WAL Catchup: Apply transaction log to reach consistency with peers
    fn execute_wal_catchup_phase(
        &self,
        processor: &AffinityProcessor,
        step: &crate::engine::affinity::AffinitySyncStep,
    ) -> Result<()> {

        let Some(database_id) = &step.database_id else {
            return Err(ServerLibError::InvalidState(
                "wal catchup phase requires database_id".to_string(),
            ));
        };

        let Some(schema_identifier) = step.schema_identifier else {
            return Err(ServerLibError::InvalidState(
                "wal catchup phase requires schema_identifier".to_string(),
            ));
        };

        log::info!(
            "executing wal catchup sync affinity={:?} database={} schema_id={}",
            processor.document().map(|d| &d.affinity_id),
            database_id,
            schema_identifier
        );

        // For MVP single-node: Phase completes when transaction log is caught up
        // For multi-node: Would implement the following steps
        // 
        // WAL Catchup Steps:
        // 1. Query peers for transactions since startup LSN (or last known checkpoint)
        // 2. Receive transaction responses (INSERT, UPDATE, DELETE statements)
        // 3. Execute transactions in order through app to maintain consistency
        // 4. Verify final state matches peers (e.g., via checksums or counts)
        // 5. Update checkpoint with current LSN
        //
        // For now (single-node), just mark as complete since there's no peer to catch up from

        log::debug!(
            "wal catchup phase completed database={} schema_id={}",
            database_id,
            schema_identifier
        );

        Ok(())

    }
    
}

#[cfg(test)]
#[path = "replication_executor_test.rs"]
mod tests;
