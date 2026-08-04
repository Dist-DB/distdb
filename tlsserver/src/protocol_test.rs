use crate::protocol::{
    TlsCertificateRequest, TlsCertificateResponse, decode_tls_certificate_request,
    decode_tls_certificate_response, encode_tls_certificate_request,
    encode_tls_certificate_response,
};

#[test]
fn tls_certificate_request_round_trips() {
    let request = TlsCertificateRequest {
        request_id: "req-1".to_string(),
        requester_id: "node-1".to_string(),
        requested_for: "server+wss".to_string(),
        csr_pem: "csr".to_string(),
    };

    let encoded = encode_tls_certificate_request(&request).expect("request should encode");
    let decoded = decode_tls_certificate_request(&encoded).expect("request should decode");

    assert_eq!(decoded, request);
}

#[test]
fn tls_certificate_response_round_trips() {
    let response = TlsCertificateResponse {
        request_id: "req-2".to_string(),
        ok: true,
        error: None,
        cert_pem: Some("cert".to_string()),
        ca_cert_pem: Some("ca".to_string()),
    };

    let encoded = encode_tls_certificate_response(&response).expect("response should encode");
    let decoded = decode_tls_certificate_response(&encoded).expect("response should decode");

    assert_eq!(decoded, response);
}