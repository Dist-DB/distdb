use std::path::Path;

use security::sign_tls_enrollment_csr;
use crate::protocol::{TlsCertificateRequest, TlsCertificateResponse};

pub fn build_tls_certificate_response(
    node_data_dir: &Path,
    ca_root_enabled: bool,
    request: &TlsCertificateRequest,
) -> TlsCertificateResponse {
    
    if !ca_root_enabled {
        
        return TlsCertificateResponse {
            request_id: request.request_id.clone(),
            ok: false,
            error: Some("tls enrollment disabled on this node; ca_root is not enabled".to_string()),
            cert_pem: None,
            ca_cert_pem: None,
        };

    }

    match sign_tls_enrollment_csr(node_data_dir, &request.csr_pem) {
        
        Ok((cert_pem, ca_cert_pem)) => TlsCertificateResponse {
            request_id: request.request_id.clone(),
            ok: true,
            error: None,
            cert_pem: Some(cert_pem),
            ca_cert_pem: Some(ca_cert_pem),
        },

        Err(err) => TlsCertificateResponse {
            request_id: request.request_id.clone(),
            ok: false,
            error: Some(err),
            cert_pem: None,
            ca_cert_pem: None,
        },
    
    }

}