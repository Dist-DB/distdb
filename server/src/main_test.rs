use super::validate_startup_tls_requirements;

#[test]
fn startup_tls_requires_san() {
    let err = validate_startup_tls_requirements(&[])
        .expect_err("startup tls without SAN should be rejected");

    assert!(err.contains("tls_san"));
}

#[test]
fn startup_tls_allows_explicit_san() {
    validate_startup_tls_requirements(&["wss.example.com".to_string()])
        .expect("startup tls should allow explicit SANs");
}