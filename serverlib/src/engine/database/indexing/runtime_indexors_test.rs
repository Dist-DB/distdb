use super::runtime_index_key_codec::RuntimeIndexNumericKind;
use super::runtime_index_storage::RuntimeIndexStorage;
use super::runtime_indexors::{
    numeric_kind_for_field_kind, CompositeIndexor, DatatypeIndexor, DateTimeIndexor,
    FloatIndexor, SignedIntegerIndexor, StringIndexor, UnsignedIntegerIndexor,
};
use crate::{DatabaseIndex, DatabaseIndexKind, FieldKind};

fn index(field_name: &str) -> DatabaseIndex {
    DatabaseIndex::from_table_fields(
        "test",
        DatabaseIndexKind::Indexed,
        vec![field_name.to_string()],
    )
}

fn assert_basic_indexor_behavior(mut indexor: impl RuntimeIndexStorage) {
    indexor.insert(vec![b"value".to_vec()], Some(7));
    assert!(indexor.contains(&[b"value".to_vec()]));
    assert_eq!(indexor.row_refs_for_key(&[b"value".to_vec()], None), vec![7]);
    assert_eq!(indexor.cardinality(), 1);
    indexor.remove(&[b"value".to_vec()], Some(7));
    assert_eq!(indexor.cardinality(), 0);
}

#[test]
fn string_indexor_stores_and_removes_postings() {
    let mut indexor = StringIndexor::new(index("name"));
    indexor.insert(vec![b"Cologne".to_vec()], Some(7));
    assert!(indexor.contains(&[b"cologne".to_vec()]));
    assert_eq!(indexor.row_refs_for_key(&[b"COLOGNE".to_vec()], None), vec![7]);
    indexor.remove(&[b"cologne".to_vec()], Some(7));
    assert_eq!(indexor.cardinality(), 0);
}

#[test]
fn signed_integer_indexor_stores_and_removes_postings() {
    assert_basic_indexor_behavior(SignedIntegerIndexor::new(index("id")));
}

#[test]
fn unsigned_integer_indexor_stores_and_removes_postings() {
    assert_basic_indexor_behavior(UnsignedIntegerIndexor::new(index("id")));
}

#[test]
fn float_indexor_stores_and_removes_postings() {
    assert_basic_indexor_behavior(FloatIndexor::new(index("score")));
}

#[test]
fn datetime_indexor_stores_and_removes_postings() {
    assert_basic_indexor_behavior(DateTimeIndexor::new(index("created_at")));
}

#[test]
fn composite_indexor_stores_and_removes_postings() {
    assert_basic_indexor_behavior(CompositeIndexor::new(index("key")));
}

#[test]
fn datatype_factory_selects_each_indexor_family() {
    assert!(matches!(
        DatatypeIndexor::for_field_kind(index("name"), &FieldKind::Text),
        DatatypeIndexor::String(_)
    ));
    assert!(matches!(
        DatatypeIndexor::for_field_kind(index("id"), &FieldKind::Int(32)),
        DatatypeIndexor::SignedInteger(_)
    ));
    assert!(matches!(
        DatatypeIndexor::for_field_kind(index("id"), &FieldKind::UInt(32)),
        DatatypeIndexor::UnsignedInteger(_)
    ));
    assert!(matches!(
        DatatypeIndexor::for_field_kind(index("score"), &FieldKind::Float(64)),
        DatatypeIndexor::Float(_)
    ));
    assert!(matches!(
        DatatypeIndexor::for_field_kind(index("created_at"), &FieldKind::DateTime),
        DatatypeIndexor::DateTime(_)
    ));
}

#[test]
fn numeric_kind_factory_matches_signedness() {
    assert_eq!(numeric_kind_for_field_kind(&FieldKind::Int(8)), Some(RuntimeIndexNumericKind::Signed));
    assert_eq!(numeric_kind_for_field_kind(&FieldKind::UInt(8)), Some(RuntimeIndexNumericKind::Unsigned));
    assert_eq!(numeric_kind_for_field_kind(&FieldKind::Text), None);
}
