
use super::ObjectStatus;

#[test]
fn object_status_lock_to_ready_is_valid_for_abort_path() {
    assert!(ObjectStatus::Lock.can_transition_to(ObjectStatus::Ready));
}
