
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
