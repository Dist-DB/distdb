use common::schema::FieldKind;

use super::{
    encode_sortable_numeric, numeric_gate_depth,
    normalize_runtime_index_string_key, runtime_index_string_page_head,
    runtime_index_string_probe_variants, RuntimeIndexKeyStrategy,
    RuntimeIndexNumericKind,
};

#[test]
fn runtime_index_string_key_normalization_is_ascii_case_insensitive() {
    let value = b"Alpha-42";
    let normalized = normalize_runtime_index_string_key(value, true);
    assert_eq!(normalized, b"alpha-42");
}

#[test]
fn runtime_index_string_page_head_uses_normalized_prefix_only() {
    let head = runtime_index_string_page_head(b"Alpha-42", 5, true);
    assert_eq!(head, b"alpha");
}

#[test]
fn runtime_index_key_strategy_matches_field_kind_family() {
    assert_eq!(
        RuntimeIndexKeyStrategy::for_field_kind(&FieldKind::Text, true),
        RuntimeIndexKeyStrategy::String { case_insensitive: true }
    );
    assert_eq!(
        RuntimeIndexKeyStrategy::for_field_kind(&FieldKind::Int(64), false),
        RuntimeIndexKeyStrategy::Numeric
    );
    assert_eq!(
        RuntimeIndexKeyStrategy::for_field_kind(&FieldKind::DateTime, false),
        RuntimeIndexKeyStrategy::DateTime
    );
}

#[test]
fn runtime_index_numeric_and_datetime_page_heads_are_stable() {
    let numeric = RuntimeIndexKeyStrategy::Numeric.page_head(b"123456", 3);
    let datetime = RuntimeIndexKeyStrategy::DateTime.page_head(b"2026-08-12 15:31:00", 4);
    assert_eq!(numeric, b"123");
    assert_eq!(datetime, b"2026");
}

#[test]
fn runtime_index_string_probe_variants_include_normalized_prefix_heads() {
    let variants = runtime_index_string_probe_variants(b"Alpha-42", true);
    assert!(variants.iter().any(|value| value == b"alpha-42"));
    assert!(variants.iter().any(|value| value == b"alpha"));
}

#[test]
fn sortable_signed_numeric_encoding_orders_negative_zero_positive() {
    let negative = encode_sortable_numeric(b"-1", RuntimeIndexNumericKind::Signed).unwrap();
    let zero = encode_sortable_numeric(b"0", RuntimeIndexNumericKind::Signed).unwrap();
    let positive = encode_sortable_numeric(b"1", RuntimeIndexNumericKind::Signed).unwrap();
    assert!(negative < zero);
    assert!(zero < positive);
}

#[test]
fn sortable_unsigned_numeric_encoding_orders_values_by_magnitude() {
    let low = encode_sortable_numeric(b"2", RuntimeIndexNumericKind::Unsigned).unwrap();
    let high = encode_sortable_numeric(b"256", RuntimeIndexNumericKind::Unsigned).unwrap();
    assert!(low < high);
}

#[test]
fn narrow_numeric_types_stop_at_their_domain_leaf_width() {
    assert_eq!(numeric_gate_depth(8), 0);
    assert_eq!(numeric_gate_depth(16), 1);
    assert_eq!(numeric_gate_depth(32), 3);
    assert_eq!(numeric_gate_depth(64), 7);
}
