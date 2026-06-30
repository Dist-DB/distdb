
use super::*;

#[test]
fn entity_kind_equality() {
    assert_eq!(DatabaseEntityKind::Table, DatabaseEntityKind::Table);
    assert_ne!(DatabaseEntityKind::Table, DatabaseEntityKind::View);
}

#[test]
fn entity_kind_is_copy() {
    let kind = DatabaseEntityKind::Trigger;
    let copy = kind;
    assert_eq!(kind, copy);
}
