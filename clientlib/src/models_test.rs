use super::*;

#[test]
fn tls_mode_default_is_required() {
    assert_eq!(TlsMode::default(), TlsMode::Required);
}

#[test]
fn query_value_render_display_handles_all_variants() {
    assert_eq!(QueryValue::Null.render_display(), "NULL");
    assert_eq!(QueryValue::Int(-42).render_display(), "-42");
    assert_eq!(QueryValue::UInt(42).render_display(), "42");
    assert_eq!(QueryValue::Float("1.25".to_string()).render_display(), "1.25");
    assert_eq!(
        QueryValue::Text("hello".to_string()).render_display(),
        "hello"
    );
    assert_eq!(QueryValue::Bytes(vec![0, 15, 255]).render_display(), "0x000fff");
}

#[test]
fn query_value_serde_roundtrip_preserves_tagged_shape() {
    let value = QueryValue::UInt(9);
    let json = serde_json::to_value(&value).expect("query value should serialize");

    assert_eq!(json["kind"], "u_int");
    assert_eq!(json["value"], 9);

    let decoded: QueryValue =
        serde_json::from_value(json).expect("query value should deserialize");
    assert_eq!(decoded, value);
}
