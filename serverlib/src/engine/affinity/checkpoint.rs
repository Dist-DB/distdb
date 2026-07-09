use super::AffinitySyncPhase;
use common::{epoch_ms};

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
            last_update_epoch_ms: epoch_ms!(),
        }
    }

    pub fn mark_step_completed(&mut self, step_index: usize) {

        if !self.completed_step_indices.contains(&step_index) {
            self.completed_step_indices.push(step_index);
            self.completed_step_indices.sort();
            self.last_update_epoch_ms = epoch_ms!();
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

