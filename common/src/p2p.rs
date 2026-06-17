
/// Lightweight CA certificate bootstrap protocol.
///
/// Before a client establishes a TLS connection it may not have the cluster CA cert.
/// This module defines a tiny pre-TLS request/response wire format (magic `CACB`)
/// that lets a client fetch the CA cert over plain TCP and then reconnect with TLS.
///
/// Wire format (both directions): 4-byte magic + bincode-encoded struct.

pub const CA_BOOTSTRAP_MAGIC: &[u8; 4] = b"CACB";

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CaBootstrapRequest {
    pub node_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CaBootstrapResponse {
    pub ok: bool,
    pub ca_cert_pem: Option<String>,
    pub error: Option<String>,
}

pub fn encode_ca_bootstrap_request(request: &CaBootstrapRequest) -> Option<Vec<u8>> {
    let payload = bincode::serialize(request).ok()?;
    let mut out = CA_BOOTSTRAP_MAGIC.to_vec();
    out.extend_from_slice(&payload);
    Some(out)
}

pub fn decode_ca_bootstrap_request(payload: &[u8]) -> Option<CaBootstrapRequest> {

    if payload.len() < CA_BOOTSTRAP_MAGIC.len() {
        return None;
    }
    if &payload[..CA_BOOTSTRAP_MAGIC.len()] != CA_BOOTSTRAP_MAGIC {
        return None;
    }
    
    bincode::deserialize(&payload[CA_BOOTSTRAP_MAGIC.len()..]).ok()

}

pub fn encode_ca_bootstrap_response(response: &CaBootstrapResponse) -> Option<Vec<u8>> {

    let payload = bincode::serialize(response).ok()?;
    let mut out = CA_BOOTSTRAP_MAGIC.to_vec();
    
    out.extend_from_slice(&payload);
    Some(out)

}

pub fn decode_ca_bootstrap_response(payload: &[u8]) -> Option<CaBootstrapResponse> {

    if payload.len() < CA_BOOTSTRAP_MAGIC.len() {
        return None;
    }
    if &payload[..CA_BOOTSTRAP_MAGIC.len()] != CA_BOOTSTRAP_MAGIC {
        return None;
    }
    
    bincode::deserialize(&payload[CA_BOOTSTRAP_MAGIC.len()..]).ok()

}

pub fn is_ca_bootstrap_frame(payload: &[u8]) -> bool {

    payload.len() >= CA_BOOTSTRAP_MAGIC.len()
        && &payload[..CA_BOOTSTRAP_MAGIC.len()] == CA_BOOTSTRAP_MAGIC
        
}
