#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SchemaChangePhase {
    Locked,
    Rewriting,
    Reindexing,
    Syncing,
    Cutover,
}

impl SchemaChangePhase {

    pub fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Locked, Self::Rewriting)
                | (Self::Rewriting, Self::Reindexing)
                | (Self::Reindexing, Self::Syncing)
                | (Self::Syncing, Self::Cutover)
                | (Self::Cutover, Self::Cutover)
                | (Self::Locked, Self::Locked)
                | (Self::Rewriting, Self::Rewriting)
                | (Self::Reindexing, Self::Reindexing)
                | (Self::Syncing, Self::Syncing)
        )
    }
    
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ActiveSchemaChange {
    pub job_id: String,
    pub table_id: String,
    pub target_revision: u64,
    pub schema_epoch: u64,
    pub phase: SchemaChangePhase,
    #[serde(default)]
    pub rows_total: Option<u64>,
    #[serde(default)]
    pub rows_rewritten: u64,
    #[serde(default)]
    pub checkpoint_epoch_ms: u64,
    #[serde(default)]
    pub resume_token: Option<String>,
}

impl ActiveSchemaChange {

    pub fn begin(table_id: String, target_revision: u64, schema_epoch: u64) -> Self {
        Self {
            job_id: common::helpers::utils::unique_id(),
            table_id,
            target_revision,
            schema_epoch,
            phase: SchemaChangePhase::Locked,
            rows_total: None,
            rows_rewritten: 0,
            checkpoint_epoch_ms: common::epochabs!() as u64,
            resume_token: None,
        }
    }

    pub fn update_progress(
        &mut self,
        rows_rewritten: u64,
        rows_total: Option<u64>,
        resume_token: Option<String>,
    ) {
        self.rows_rewritten = rows_rewritten;
        if rows_total.is_some() {
            self.rows_total = rows_total;
        }
        if resume_token.is_some() {
            self.resume_token = resume_token;
        }
        self.checkpoint_epoch_ms = common::epochabs!() as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::{ActiveSchemaChange, SchemaChangePhase};

    #[test]
    fn schema_change_phase_enforces_ordered_transitions() {
        assert!(SchemaChangePhase::Locked.can_transition_to(SchemaChangePhase::Rewriting));
        assert!(SchemaChangePhase::Rewriting.can_transition_to(SchemaChangePhase::Reindexing));
        assert!(SchemaChangePhase::Reindexing.can_transition_to(SchemaChangePhase::Syncing));
        assert!(SchemaChangePhase::Syncing.can_transition_to(SchemaChangePhase::Cutover));

        assert!(!SchemaChangePhase::Locked.can_transition_to(SchemaChangePhase::Syncing));
        assert!(!SchemaChangePhase::Rewriting.can_transition_to(SchemaChangePhase::Cutover));
    }

    #[test]
    fn active_schema_change_progress_checkpoint_updates_fields() {
        let mut active = ActiveSchemaChange::begin("users".to_string(), 3, 10);
        let first_checkpoint = active.checkpoint_epoch_ms;

        active.update_progress(42, Some(128), Some("pk:users:42".to_string()));

        assert_eq!(active.rows_rewritten, 42);
        assert_eq!(active.rows_total, Some(128));
        assert_eq!(active.resume_token.as_deref(), Some("pk:users:42"));
        assert!(active.checkpoint_epoch_ms >= first_checkpoint);
    }

    #[test]
    fn active_schema_change_begin_assigns_job_id() {
        let active = ActiveSchemaChange::begin("users".to_string(), 1, 1);
        assert!(!active.job_id.is_empty());
    }

}
