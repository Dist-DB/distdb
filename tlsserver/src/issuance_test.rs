use crate::issuance::build_tls_certificate_response;
use crate::protocol::TlsCertificateRequest;
use security::{build_tls_enrollment_request, ensure_or_generate_tls_cert};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn unique_temp_dir(test_name: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("distdb-{test_name}-{suffix}"))
}

#[test]
fn build_tls_certificate_response_rejects_when_ca_root_disabled() {
    let request = TlsCertificateRequest {
        request_id: "req-1".to_string(),
        requester_id: "node-1".to_string(),
        requested_for: "wss".to_string(),
        csr_pem: "invalid".to_string(),
    };

    let response = build_tls_certificate_response(Path::new("/tmp/unused"), false, &request);
    assert!(!response.ok);
    assert_eq!(response.request_id, request.request_id);
    assert!(response.cert_pem.is_none());
    assert!(response.ca_cert_pem.is_none());
}

#[test]
fn build_tls_certificate_response_signs_with_existing_ca() {
    
    let node_data_dir = unique_temp_dir("tls-issuer-response");
    std::fs::create_dir_all(&node_data_dir).expect("create node data dir");

    ensure_or_generate_tls_cert(&node_data_dir, "issuer-node", "127.0.0.1:4001", &[])
        .expect("create issuer tls material");

    let enrollment = build_tls_enrollment_request(
        "requester-node",
        "requester.example:4001",
        &["api.requester.example".to_string(), "10.0.0.5".to_string()],
    )
    .expect("build csr");

    let request = TlsCertificateRequest {
        request_id: "req-2".to_string(),
        requester_id: "requester-node".to_string(),
        requested_for: "server+wss".to_string(),
        csr_pem: enrollment.csr_pem,
    };

    let response = build_tls_certificate_response(&node_data_dir, true, &request);
    assert!(response.ok);
    assert_eq!(response.request_id, request.request_id);
    assert!(response.cert_pem.is_some());
    assert!(response.ca_cert_pem.is_some());

    let _ = std::fs::remove_dir_all(&node_data_dir);
}