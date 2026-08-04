use std::path::PathBuf;

use crate::parse_tls_config_from_args;

#[test]
fn parse_tls_config_from_args_maps_all_paths() {
    let args = vec![
        "tls_cert=/tmp/cert.pem".to_string(),
        "tls_key=/tmp/key.pem".to_string(),
        "tls_ca=/tmp/ca.pem".to_string(),
        "tls_server=tls-issuer.example:8443".to_string(),
    ];

    let parsed = parse_tls_config_from_args(&args);

    assert_eq!(parsed.cert_path, Some(PathBuf::from("/tmp/cert.pem")));
    assert_eq!(parsed.key_path, Some(PathBuf::from("/tmp/key.pem")));
    assert_eq!(parsed.ca_path, Some(PathBuf::from("/tmp/ca.pem")));
    assert_eq!(parsed.issuer_addr, Some("tls-issuer.example:8443".to_string()));
}

#[test]
fn parse_tls_config_from_args_defaults_to_none() {
    let args = vec!["unrelated=value".to_string()];

    let parsed = parse_tls_config_from_args(&args);

    assert_eq!(parsed.cert_path, None);
    assert_eq!(parsed.key_path, None);
    assert_eq!(parsed.ca_path, None);
    assert_eq!(parsed.issuer_addr, None);
}
