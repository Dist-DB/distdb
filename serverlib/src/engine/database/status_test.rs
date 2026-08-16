
use super::ObjectStatus;

#[test]
fn object_status_lock_to_ready_is_valid_for_abort_path() {
    assert!(ObjectStatus::Lock.can_transition_to(ObjectStatus::Ready));
}

#[test]
fn object_status_supports_indexing_transitions() {
    assert!(ObjectStatus::Load.can_transition_to(ObjectStatus::Indexing));
    assert!(ObjectStatus::Sync.can_transition_to(ObjectStatus::Indexing));
    assert!(ObjectStatus::Ready.can_transition_to(ObjectStatus::Indexing));
    assert!(ObjectStatus::Indexing.can_transition_to(ObjectStatus::Ready));
}

#[test]
fn object_status_allows_reads_only_once_materialized() {
    assert!(ObjectStatus::Ready.allows_reads());
    // Committed data is readable while replication acks settle or a writer holds the lock.
    assert!(ObjectStatus::Sync.allows_reads());
    assert!(ObjectStatus::Lock.allows_reads());

    // Not yet materialized: queries must be refused.
    assert!(!ObjectStatus::Load.allows_reads());
    assert!(!ObjectStatus::Indexing.allows_reads());
}
