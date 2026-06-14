
use super::*;

#[test]
fn convert_numeric_string_to_int() {
    let result =
        convert_value_to_field_type(b"42", &FieldType::Int(64), TypeConversionPolicy::Safe);
    assert_eq!(result, Ok(b"42".to_vec()));
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
    assert_eq!(result, Ok(b"0".to_vec()));
}

#[test]
fn convert_text_preserves_valid_utf8() {
    let result =
        convert_value_to_field_type(b"hello", &FieldType::Text, TypeConversionPolicy::Safe);
    assert_eq!(result, Ok(b"hello".to_vec()));
}
