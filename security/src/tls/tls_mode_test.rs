use crate::parse_tls_mode_from_args;

#[test]
fn default_tls_mode_is_required() {
    let args: Vec<String> = vec![];
    let mode = parse_tls_mode_from_args(&args).expect("should parse");
    assert_eq!(mode, common::TlsMode::Required, "TLS must default to required");
}

#[test]
fn explicit_tls_arg_is_rejected() {
    let args = vec!["tls=off".to_string()];
    assert!(parse_tls_mode_from_args(&args).is_err());
}

#[test]
fn explicit_tls_required_arg_is_rejected() {
    let args = vec!["tls=required".to_string()];
    assert!(parse_tls_mode_from_args(&args).is_err());
}

#[test]
fn invalid_tls_mode_is_rejected() {
    let args = vec!["tls=unsafe".to_string()];
    assert!(parse_tls_mode_from_args(&args).is_err());
}
