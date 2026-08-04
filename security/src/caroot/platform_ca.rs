
pub const PLATFORM_TLS_ROOT_CERT_PEM: &str = include_str!("./platform_ca_root_cert.pem");
pub const PLATFORM_TLS_ROOT_FINGERPRINT_SHA256: &str = "c2f083325f24102d28c0dbd50c431284a825f8552e7a53e8a9ba66bfb7bcb144";

pub fn platform_tls_root_cert_pem() -> &'static str {
    PLATFORM_TLS_ROOT_CERT_PEM
}

pub fn platform_tls_root_fingerprint_sha256() -> &'static str {
    PLATFORM_TLS_ROOT_FINGERPRINT_SHA256
}