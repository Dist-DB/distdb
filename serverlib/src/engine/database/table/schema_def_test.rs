
use std::hint::black_box;
use std::time::Instant;

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
fn field_lookup_cache_is_rebuilt_after_mutation() {
    let mut schema = TableSchema::new(vec![text_field(1, "email"), text_field(2, "name")]);

    assert_eq!(schema.field_indexes_by_name.get("email"), Some(&0));
    assert_eq!(schema.field_indexes_by_name.get("name"), Some(&1));

    schema.add_field(text_field(3, "phone")).unwrap();
    assert_eq!(schema.field_indexes_by_name.get("phone"), Some(&2));

    schema.remove_field("name").unwrap();
    assert!(!schema.field_indexes_by_name.contains_key("name"));
    assert_eq!(schema.field("email"), Some(&text_field(1, "email")));
}

#[test]
fn table_schema_field_lookup_benchmark_style() {
    let schema = TableSchema::new((0..2_000).map(|index| text_field(index + 1, &format!("field_{index}"))).collect::<Vec<_>>());

    let baseline_start = Instant::now();
    let baseline = black_box((0..5_000).fold(0usize, |acc, _| {
        acc + usize::from(schema.fields.iter().any(|field| field.field_name == "field_1999"))
    }));
    let baseline_elapsed = baseline_start.elapsed();

    let optimized_start = Instant::now();
    let optimized = black_box((0..5_000).fold(0usize, |acc, _| {
        acc + usize::from(schema.field("field_1999").is_some())
    }));
    let optimized_elapsed = optimized_start.elapsed();

    assert!(baseline > 0);
    assert!(optimized > 0);

    println!(
        "table_schema_field_lookup_benchmark_style baseline_elapsed_ns={} optimized_elapsed_ns={}",
        baseline_elapsed.as_nanos(),
        optimized_elapsed.as_nanos(),
    );
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
