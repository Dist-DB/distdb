pub const TLS_CERTIFICATE_MAGIC: &[u8; 4] = b"TLSS";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TlsCertificateRequest {
    pub request_id: String,
    pub requester_id: String,
    pub requested_for: String,
    pub csr_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TlsCertificateResponse {
    pub request_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub cert_pem: Option<String>,
    pub ca_cert_pem: Option<String>,
}

pub fn encode_tls_certificate_request(request: &TlsCertificateRequest) -> Option<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(request, bincode::config::legacy()).ok()?;
    let mut out = TLS_CERTIFICATE_MAGIC.to_vec();
    out.extend_from_slice(&payload);
    Some(out)
}

pub fn decode_tls_certificate_request(payload: &[u8]) -> Option<TlsCertificateRequest> {
    
    if payload.len() < TLS_CERTIFICATE_MAGIC.len() {
        return None;
    }
    if &payload[..TLS_CERTIFICATE_MAGIC.len()] != TLS_CERTIFICATE_MAGIC {
        return None;
    }

    bincode::serde::decode_from_slice(
        &payload[TLS_CERTIFICATE_MAGIC.len()..],
        bincode::config::legacy(),
    )
    .ok()
    .map(|(value, _)| value)
    
}

pub fn encode_tls_certificate_response(response: &TlsCertificateResponse) -> Option<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(response, bincode::config::legacy()).ok()?;
    let mut out = TLS_CERTIFICATE_MAGIC.to_vec();
    out.extend_from_slice(&payload);
    Some(out)
}

pub fn decode_tls_certificate_response(payload: &[u8]) -> Option<TlsCertificateResponse> {
    
    if payload.len() < TLS_CERTIFICATE_MAGIC.len() {
        return None;
    }
    if &payload[..TLS_CERTIFICATE_MAGIC.len()] != TLS_CERTIFICATE_MAGIC {
        return None;
    }

    bincode::serde::decode_from_slice(
        &payload[TLS_CERTIFICATE_MAGIC.len()..],
        bincode::config::legacy(),
    )
    .ok()
    .map(|(value, _)| value)

}