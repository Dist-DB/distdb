use super::{compare_like_value, compare_row_value};
use crate::{FieldType, TypeConversionPolicy};
use crate::engine::database::schema::migration::convert_value_to_field_type;
use crate::SelectComparisonOp;

#[test]
fn compare_like_value_supports_escape_character() {
    assert!(compare_like_value(
        b"foo_1",
        b"foo\\_1",
        false,
        Some('\\'),
    ));
}

#[test]
fn compare_like_value_escape_can_be_custom_character() {
    assert!(compare_like_value(
        b"100%",
        b"100!%",
        false,
        Some('!'),
    ));
}

#[test]
fn compare_like_value_supports_simple_prefix_pattern() {
    assert!(compare_like_value(
        b"Amsterdam",
        b"Ams%",
        false,
        None,
    ));
    assert!(!compare_like_value(
        b"Oslo",
        b"Ams%",
        false,
        None,
    ));
}

#[test]
fn compare_like_value_supports_case_insensitive_simple_prefix() {
    assert!(compare_like_value(
        b"amsterdam",
        b"Ams%",
        true,
        None,
    ));
}

#[test]
fn compare_like_value_supports_ordered_multi_segment_patterns() {
    assert!(compare_like_value(
        b"amsterdam",
        b"%ter%am",
        false,
        None,
    ));
    assert!(!compare_like_value(
        b"amsterdam",
        b"%am%ter",
        false,
        None,
    ));
}

#[test]
fn compare_row_value_supports_native_numeric_storage() {
    let actual = convert_value_to_field_type(
        b"42",
        &FieldType::UInt(64),
        TypeConversionPolicy::Safe,
    )
    .expect("numeric field should encode");

    assert!(compare_row_value(&actual, b"42", &SelectComparisonOp::Eq));
    assert!(compare_row_value(&actual, b"7", &SelectComparisonOp::Gt));
}
