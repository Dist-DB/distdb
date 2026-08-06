use std::fs::File;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::Path;
use std::sync::Arc;

use openssl::x509::X509;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ResolvesServerCert;
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use super::TlsConfig;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
pub type BoxedConnectorStream = Box<dyn AsyncReadWrite>;

fn load_tls_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {

    let file = File::open(path)
        .map_err(|err| format!("failed to open tls cert file '{}': {}", path.display(), err))?;

    let mut reader = std::io::BufReader::new(file);

    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to parse tls cert file '{}': {}", path.display(), err))?;

    if certs.is_empty() {
        return Err(format!(
            "tls cert file '{}' does not contain any certificates",
            path.display()
        ));
    }

    Ok(certs)

}

fn load_tls_certificates_from_pem(pem: &str) -> Result<Vec<CertificateDer<'static>>, String> {

    let mut reader = std::io::Cursor::new(pem.as_bytes());

    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("failed to parse tls certificate PEM: {err}"))?;

    if certs.is_empty() {
        return Err("tls certificate PEM does not contain any certificates".to_string());
    }

    Ok(certs)

}

fn load_tls_private_key_from_pem(pem: &str) -> Result<PrivateKeyDer<'static>, String> {

    let mut reader = std::io::Cursor::new(pem.as_bytes());

    rustls_pemfile::private_key(&mut reader)
        .map_err(|err| format!("failed to parse tls private key PEM: {err}"))?
        .ok_or_else(|| "tls private key PEM does not contain a supported private key".to_string())

}

pub fn validate_tls_certificate_subject_alt_names(
    cert_path: &Path,
    required_subject_alt_names: &[String],
) -> Result<(), String> {

    let certs = load_tls_certificates(cert_path)?;
    let leaf_cert = certs
        .first()
        .ok_or_else(|| format!("tls cert file '{}' does not contain any certificates", cert_path.display()))?;

    let missing_names = required_subject_alt_names
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .filter(|name| !certificate_matches_server_name(leaf_cert.as_ref(), name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if !missing_names.is_empty() {
        return Err(format!(
            "tls cert file '{}' is missing SAN entries for: {}",
            cert_path.display(),
            missing_names.join(", ")
        ));
    }

    Ok(())

}

pub fn validate_tls_certificate_subject_alt_names_pem(
    cert_pem: &str,
    required_subject_alt_names: &[String],
) -> Result<(), String> {

    let certs = load_tls_certificates_from_pem(cert_pem)?;
    let leaf_cert = certs
        .first()
        .ok_or_else(|| "tls certificate PEM does not contain any certificates".to_string())?;

    let missing_names = required_subject_alt_names
        .iter()
        .map(|name| name.trim())
        .filter(|name| !name.is_empty())
        .filter(|name| !certificate_matches_server_name(leaf_cert.as_ref(), name))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if !missing_names.is_empty() {
        return Err(format!(
            "tls certificate PEM is missing SAN entries for: {}",
            missing_names.join(", ")
        ));
    }

    Ok(())

}

fn load_tls_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {

    let file = File::open(path)
        .map_err(|err| format!("failed to open tls key file '{}': {}", path.display(), err))?;

    let mut reader = std::io::BufReader::new(file);

    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|err| format!("failed to parse tls key file '{}': {}", path.display(), err))?
        .ok_or_else(|| {
            format!(
                "tls key file '{}' does not contain a supported private key",
                path.display()
            )
        })?;

    Ok(key)

}

#[derive(Debug)]
struct StaticServerCertResolver {
    cert: Arc<CertifiedKey>,
}

impl StaticServerCertResolver {

    fn new(cert_chain: Vec<CertificateDer<'static>>, private_key: PrivateKeyDer<'static>) -> Result<Self, String> {

        let provider = rustls::crypto::ring::default_provider();
        let cert = Arc::new(
            CertifiedKey::from_der(cert_chain, private_key, &provider)
                .map_err(|err| format!("invalid tls cert/key pair: {err}"))?,
        );

        Ok(Self { cert })

    }

}

impl ResolvesServerCert for StaticServerCertResolver {

    fn resolve(&self, _client_hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.cert))
    }

}

pub fn certificate_matches_server_name(cert_der: &[u8], server_name: &str) -> bool {

    let Ok(cert) = X509::from_der(cert_der) else {
        return false;
    };

    let Some(sans) = cert.subject_alt_names() else {
        return false;
    };

    let expected_ip = server_name.parse::<IpAddr>().ok();

    sans.iter().any(|san| {
        san.dnsname()
            .is_some_and(|name| name.eq_ignore_ascii_case(server_name))
            || san
                .ipaddress()
                .and_then(ip_addr_from_san_bytes)
                .is_some_and(|ip| Some(ip) == expected_ip)
    })

}

fn ip_addr_from_san_bytes(raw: &[u8]) -> Option<IpAddr> {

    match raw.len() {

        4 => Some(IpAddr::V4(Ipv4Addr::new(raw[0], raw[1], raw[2], raw[3]))),

        16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(raw);
            Some(IpAddr::V6(Ipv6Addr::from(octets)))
        },

        _ => None,

    }

}

pub fn build_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor, String> {
    
    let cert_path = config
        .cert_path
        .as_deref()
        .ok_or_else(|| "tls_cert is required when tls is required".to_string())?;

    let key_path = config
        .key_path
        .as_deref()
        .ok_or_else(|| "tls_key is required when tls is required".to_string())?;

    let cert_chain = load_tls_certificates(cert_path)?;
    let private_key = load_tls_private_key(key_path)?;
    let resolver = StaticServerCertResolver::new(cert_chain, private_key)?;

    let mut tls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver));

    tls_config.alpn_protocols = vec![b"distdb-p2p/1".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(tls_config)))

}

pub fn build_tls_acceptor_from_pem(cert_pem: &str, key_pem: &str) -> Result<TlsAcceptor, String> {

    let cert_chain = load_tls_certificates_from_pem(cert_pem)?;
    let private_key = load_tls_private_key_from_pem(key_pem)?;
    let provider = rustls::crypto::ring::default_provider();

    let cert = Arc::new(
        CertifiedKey::from_der(cert_chain, private_key, &provider)
            .map_err(|err| format!("invalid tls cert/key pair: {err}"))?,
    );

    let resolver = StaticServerCertResolver { cert };

    let mut tls_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_cert_resolver(Arc::new(resolver));

    tls_config.alpn_protocols = vec![b"distdb-p2p/1".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(tls_config)))

}

pub fn build_tls_client_config(config: &TlsConfig) -> Result<Arc<ClientConfig>, String> {

    let root_path = config
        .ca_path
        .as_deref()
        .or(config.cert_path.as_deref())
        .ok_or_else(|| {
            "tls_ca (or tls_cert for self-signed trust) is required for outbound tls".to_string()
        })?;

    let mut roots = RootCertStore::empty();
    let certs = load_tls_certificates(root_path)?;
    let cert_count = certs.len();

    for cert in certs {
        roots
            .add(cert)
            .map_err(|err| format!("failed to add tls root cert from '{}': {err}", root_path.display()))?;
    }

    if cert_count == 0 {
        return Err(format!(
            "tls root cert file '{}' does not contain any certificates",
            root_path.display()
        ));
    }

    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"distdb-p2p/1".to_vec()];

    Ok(Arc::new(client))

}

pub fn build_tls_client_config_from_pem(ca_pem: &str) -> Result<Arc<ClientConfig>, String> {

    let certs = load_tls_certificates_from_pem(ca_pem)?;

    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .map_err(|err| format!("failed to add tls root cert from in-memory CA: {err}"))?;
    }

    let mut client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    
    client.alpn_protocols = vec![b"distdb-p2p/1".to_vec()];

    Ok(Arc::new(client))

}

pub async fn negotiate_connector_stream(
    stream: TcpStream,
    peer_addr: &str,
    tls_mode: common::TlsMode,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<BoxedConnectorStream, Box<dyn std::error::Error + Send + Sync>> {

    match tls_mode {

        common::TlsMode::Required => {

            let acceptor = tls_acceptor.ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "tls mode is required but no tls acceptor is configured",
                )
            })?;

            let tls_stream = acceptor.accept(stream).await.map_err(|err| {
                std::io::Error::new(
                    std::io::ErrorKind::ConnectionAborted,
                    format!("tls handshake failed for {peer_addr}: {err}"),
                )
            })?;

            Ok(Box::new(tls_stream))

        },
        
        _ => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("p2p network requires tls=required for peer {peer_addr}"),
        ))),

    }
    
}
