use super::*;

#[test]
fn tls_mode_parse_and_as_common_cover_all_variants() {
    assert_eq!(TlsMode::parse("off"), Some(TlsMode::Off));
    assert_eq!(TlsMode::parse("optional"), Some(TlsMode::Optional));
    assert_eq!(TlsMode::parse("required"), Some(TlsMode::Required));
    assert_eq!(TlsMode::parse("unknown"), None);

    assert_eq!(TlsMode::Off.as_common(), common::TlsMode::Off);
    assert_eq!(TlsMode::Optional.as_common(), common::TlsMode::Optional);
    assert_eq!(TlsMode::Required.as_common(), common::TlsMode::Required);
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
