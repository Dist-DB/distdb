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
