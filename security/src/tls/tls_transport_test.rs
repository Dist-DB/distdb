use super::negotiate_connector_stream;
use super::tls_transport::certificate_matches_server_name;

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

#[test]
fn certificate_matches_request_name_when_sni_matches_san() {
    let cert_der = generate_test_certificate("public.example.com");

    assert!(certificate_matches_server_name(&cert_der, "public.example.com"));
    assert!(!certificate_matches_server_name(&cert_der, "other.example"));
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
