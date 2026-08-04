
pub const PLATFORM_TLS_ROOT_CERT_PEM: &str = include_str!("./platform_ca_root_cert.pem");
pub const PLATFORM_TLS_ROOT_FINGERPRINT_SHA256: &str = "c2f083325f24102d28c0dbd50c431284a825f8552e7a53e8a9ba66bfb7bcb144";
pub const PLATFORM_TLS_ISSUING_CA_CERT_PEM: &str = include_str!("./platform_issuing_ca_cert.pem");
pub const PLATFORM_TLS_ISSUING_CA_KEY_PEM: &str = include_str!("./platform_issuing_ca_key.pem");
pub const PLATFORM_TLS_ISSUING_CA_FINGERPRINT_SHA256: &str = "b3b2a99c98fc68ad8a063360fd72385b3b63037cee856ac24f4f59c40feafa1b";

pub fn platform_tls_root_cert_pem() -> &'static str {
    PLATFORM_TLS_ROOT_CERT_PEM
}

pub fn platform_tls_root_fingerprint_sha256() -> &'static str {
    PLATFORM_TLS_ROOT_FINGERPRINT_SHA256
}

pub fn platform_tls_issuing_ca_cert_pem() -> &'static str {
    PLATFORM_TLS_ISSUING_CA_CERT_PEM
}

pub fn platform_tls_issuing_ca_key_pem() -> &'static str {
    PLATFORM_TLS_ISSUING_CA_KEY_PEM
}

pub fn platform_tls_issuing_ca_fingerprint_sha256() -> &'static str {
    PLATFORM_TLS_ISSUING_CA_FINGERPRINT_SHA256
}

pub fn platform_tls_leaf_chain_pem(leaf_cert_pem: &str) -> String {
    let mut pem = String::with_capacity(
        leaf_cert_pem.len()
            + PLATFORM_TLS_ISSUING_CA_CERT_PEM.len()
            + PLATFORM_TLS_ROOT_CERT_PEM.len()
            + 2,
    );
    pem.push_str(leaf_cert_pem.trim_end());
    pem.push('\n');
    pem.push_str(PLATFORM_TLS_ISSUING_CA_CERT_PEM.trim());
    pem.push('\n');
    pem.push_str(PLATFORM_TLS_ROOT_CERT_PEM.trim());
    pem.push('\n');
    pem
}