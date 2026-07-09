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
            (Self::Locked, Self::Rewriting) |
            (Self::Rewriting, Self::Reindexing) |
            (Self::Reindexing, Self::Syncing) |
            (Self::Syncing, Self::Cutover) |
            (Self::Cutover, Self::Cutover) |
            (Self::Locked, Self::Locked) |
            (Self::Rewriting, Self::Rewriting) |
            (Self::Reindexing, Self::Reindexing) |
            (Self::Syncing, Self::Syncing)
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
            checkpoint_epoch_ms: common::epoch_nanos!(),
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
        
        self.checkpoint_epoch_ms = common::epoch_nanos!();

    }

}


#[cfg(test)]
#[path = "change_state_test.rs"]
mod change_state_test;
