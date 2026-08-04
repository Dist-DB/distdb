use crate::client::{decode_response_frame_for_test, encode_request_frame_for_test};
use crate::protocol::{
    TlsCertificateRequest, TlsCertificateResponse, encode_tls_certificate_response,
};

#[test]
fn request_certificate_client_helpers_use_expected_framing() {

    let request = TlsCertificateRequest {
        request_id: "req-1".to_string(),
        requester_id: "server-node-01".to_string(),
        requested_for: "server+wss".to_string(),
        csr_pem: "csr".to_string(),
    };

    let request_frame = encode_request_frame_for_test(&request);
    let request_len = u32::from_le_bytes(request_frame[..4].try_into().expect("frame header"));
    assert_eq!(request_len as usize, request_frame.len() - 4);

    let expected_response = TlsCertificateResponse {
        request_id: request.request_id.clone(),
        ok: true,
        error: None,
        cert_pem: Some("cert".to_string()),
        ca_cert_pem: Some("ca".to_string()),
    };
    
    let encoded_response = encode_tls_certificate_response(&expected_response)
        .expect("encode response");
    
    let mut response_frame = Vec::new();
    response_frame.extend_from_slice(&(encoded_response.len() as u32).to_le_bytes());
    response_frame.extend_from_slice(&encoded_response);

    let response = decode_response_frame_for_test(response_frame)
        .expect("client helpers should decode response");

    assert!(response.ok);
    assert_eq!(response.request_id, request.request_id);
    assert_eq!(response.cert_pem.as_deref(), Some("cert"));
    assert_eq!(response.ca_cert_pem.as_deref(), Some("ca"));

}