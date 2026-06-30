
use super::*;
use crate::engine::database::field_types::{FieldIndex, FieldType};

fn text_field(seqno: u32, name: &str) -> FieldDef {
    FieldDef {
        seqno,
        field_name: name.to_string(),
        field_type: FieldType::Text,
        nullable: false,
        indexed: FieldIndex::None,
        default_value: None,
        metadata: None,
    }
}

#[test]
fn add_field_normalizes_name() {
    let mut schema = TableSchema::new(Vec::new());
    schema.add_field(text_field(1, "Email")).unwrap();
    assert!(schema.field("email").is_some());
}

#[test]
fn add_field_rejects_duplicate_name() {
    let mut schema = TableSchema::new(vec![text_field(1, "email")]);
    let err = schema.add_field(text_field(2, "Email")).unwrap_err();
    assert!(matches!(err, SchemaError::DuplicateField));
}

#[test]
fn add_field_rejects_duplicate_seqno() {
    let mut schema = TableSchema::new(vec![text_field(1, "email")]);
    let err = schema.add_field(text_field(1, "name")).unwrap_err();
    assert!(matches!(err, SchemaError::SeqnoConflict));
}

#[test]
fn remove_field_removes_by_normalized_name() {
    let mut schema = TableSchema::new(vec![text_field(1, "email"), text_field(2, "name")]);
    schema.remove_field("Email").unwrap();
    assert!(schema.field("email").is_none());
    assert_eq!(schema.fields.len(), 1);
}

#[test]
fn remove_field_returns_error_when_not_found() {
    let mut schema = TableSchema::new(Vec::new());
    let err = schema.remove_field("missing").unwrap_err();
    assert!(matches!(err, SchemaError::FieldNotFound));
}

#[test]
fn update_field_replaces_existing_definition() {

    let mut schema = TableSchema::new(vec![text_field(1, "email")]);

    let updated = FieldDef {
        seqno: 1,
        field_name: "email".to_string(),
        field_type: FieldType::Text,
        nullable: true,
        indexed: FieldIndex::Indexed,
        default_value: None,
        metadata: None,
    };

    schema.update_field(updated.clone()).unwrap();

    assert_eq!(schema.field("email"), Some(&updated));
    
}

#[test]
fn update_field_returns_error_when_not_found() {
    let mut schema = TableSchema::new(Vec::new());
    let err = schema.update_field(text_field(1, "ghost")).unwrap_err();
    assert!(matches!(err, SchemaError::FieldNotFound));
}

#[test]
fn update_field_rejects_seqno_conflict_with_other_field() {
    let mut schema = TableSchema::new(vec![text_field(1, "email"), text_field(2, "name")]);

    let err = schema
        .update_field(FieldDef {
            seqno: 2,
            field_name: "email".to_string(),
            field_type: FieldType::Text,
            nullable: false,
            indexed: FieldIndex::None,
            default_value: None,
            metadata: None,
        })
        .unwrap_err();

    assert!(matches!(err, SchemaError::SeqnoConflict));
}

#[test]
fn validate_rejects_duplicate_seqno_from_raw_schema() {
    let schema = TableSchema::new(vec![text_field(1, "email"), text_field(1, "name")]);
    let err = schema.validate().unwrap_err();
    assert!(matches!(err, SchemaError::SeqnoConflict));
}
