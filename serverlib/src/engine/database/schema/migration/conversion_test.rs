
use super::*;

#[test]
fn convert_numeric_string_to_int() {
    let result =
        convert_value_to_field_type(b"42", &FieldType::Int(64), TypeConversionPolicy::Safe);
    let stored = result.expect("int conversion should succeed");
    assert_ne!(stored, b"42".to_vec());
    assert_eq!(render_stored_field_value(&stored), b"42".to_vec());
}

#[test]
fn convert_invalid_to_int_safe_mode_fails() {
    let result = convert_value_to_field_type(
        b"not-a-number",
        &FieldType::Int(32),
        TypeConversionPolicy::Safe,
    );
    assert_eq!(result, Err(()));
}

#[test]
fn convert_invalid_to_int_force_mode_coerces() {
    let result = convert_value_to_field_type(
        b"not-a-number",
        &FieldType::Int(32),
        TypeConversionPolicy::Force,
    );
    let stored = result.expect("forced int conversion should succeed");
    assert_eq!(render_stored_field_value(&stored), b"0".to_vec());
}

#[test]
fn convert_text_preserves_valid_utf8() {
    let result =
        convert_value_to_field_type(b"hello", &FieldType::Text, TypeConversionPolicy::Safe);
    assert_eq!(result, Ok(b"hello".to_vec()));
}

#[test]
fn convert_uuid_text_to_binary_tagged_storage() {
    let uuid_text = b"550e8400-e29b-41d4-a716-446655440000";
    let stored = convert_value_to_field_type(uuid_text, &FieldType::Uuid, TypeConversionPolicy::Safe)
        .expect("uuid conversion should succeed");

    assert_eq!(stored.len(), 17);
    assert_eq!(stored[0], 0x31);
    assert_ne!(&stored[1..], uuid_text);
    assert_eq!(
        render_stored_field_value(&stored),
        b"550e8400-e29b-41d4-a716-446655440000".to_vec()
    );
}

#[test]
fn convert_invalid_uuid_safe_mode_fails() {
    let result = convert_value_to_field_type(
        b"not-a-uuid",
        &FieldType::Uuid,
        TypeConversionPolicy::Safe,
    );
    assert_eq!(result, Err(()));
}

#[test]
fn convert_invalid_uuid_force_mode_defaults_to_nil_uuid() {
    let stored = convert_value_to_field_type(
        b"not-a-uuid",
        &FieldType::Uuid,
        TypeConversionPolicy::Force,
    )
    .expect("forced uuid conversion should succeed");

    assert_eq!(stored.len(), 17);
    assert_eq!(stored[0], 0x31);
    assert_eq!(
        render_stored_field_value(&stored),
        b"00000000-0000-0000-0000-000000000000".to_vec()
    );
}
