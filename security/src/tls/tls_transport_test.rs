use super::negotiate_connector_stream;
use super::tls_transport::certificate_matches_server_name;
use super::tls_transport::validate_tls_certificate_subject_alt_names;

use rcgen::{CertificateParams, DnType, IsCa, KeyPair};
use tokio::net::TcpListener;

fn generate_test_certificate(san: &str) -> Vec<u8> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, "test.local");
    params.is_ca = IsCa::NoCa;
    params.subject_alt_names.push(rcgen::SanType::DnsName(san.try_into().unwrap()));

    let key_pair = KeyPair::generate().expect("key should generate");
    let cert = params.self_signed(&key_pair).expect("cert should build");
    cert.der().to_vec()
}

fn generate_test_certificate_with_ip_san(ip: std::net::IpAddr) -> Vec<u8> {
    let mut params = CertificateParams::default();
    params.distinguished_name.push(DnType::CommonName, "test.local");
    params.is_ca = IsCa::NoCa;
    params.subject_alt_names.push(rcgen::SanType::IpAddress(ip));

    let key_pair = KeyPair::generate().expect("key should generate");
    let cert = params.self_signed(&key_pair).expect("cert should build");
    cert.der().to_vec()
}

#[test]
fn certificate_matches_request_name_when_sni_matches_san() {
    let cert_der = generate_test_certificate("public.example.com");

    assert!(certificate_matches_server_name(&cert_der, "public.example.com"));
    assert!(!certificate_matches_server_name(&cert_der, "other.example"));
}

#[test]
fn certificate_matches_request_name_when_ip_san_matches() {
    let cert_der = generate_test_certificate_with_ip_san(
        "127.0.0.1".parse().expect("valid ipv4"),
    );

    assert!(certificate_matches_server_name(&cert_der, "127.0.0.1"));
    assert!(!certificate_matches_server_name(&cert_der, "127.0.0.2"));
}

#[test]
fn validate_tls_certificate_subject_alt_names_requires_declared_names() {
    let cert_der = generate_test_certificate("public.example.com");
    let cert = openssl::x509::X509::from_der(&cert_der).expect("cert should parse");
    let cert_pem = cert.to_pem().expect("cert should serialize to pem");

    let dir = std::env::temp_dir().join("distdb-tls-transport-test");
    let _ = std::fs::create_dir_all(&dir);
    let cert_path = dir.join("leaf-cert.pem");
    std::fs::write(&cert_path, cert_pem).expect("write cert pem");

    validate_tls_certificate_subject_alt_names(
        &cert_path,
        &["public.example.com".to_string()],
    )
    .expect("matching SAN should validate");

    let err = validate_tls_certificate_subject_alt_names(
        &cert_path,
        &["other.example".to_string()],
    )
    .expect_err("missing SAN should fail validation");

    assert!(err.contains("other.example"));

    let _ = std::fs::remove_file(&cert_path);
}

#[tokio::test]
async fn required_tls_without_acceptor_fails_and_does_not_fallback() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("listener addr");

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept should work");
        negotiate_connector_stream(
            stream,
            &addr.to_string(),
            common::TlsMode::Required,
            None,
        )
        .await
    });

    let _client = tokio::net::TcpStream::connect(addr)
        .await
        .expect("client should connect");

    let result = server.await.expect("server task should complete");
    assert!(result.is_err(), "required TLS must fail without acceptor");
}
