use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;

use crate::core::config::ServerTlsConfig;

pub trait AsyncReadWrite: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> AsyncReadWrite for T where T: AsyncRead + AsyncWrite + Unpin + Send {}
pub type BoxedConnectorStream = Box<dyn AsyncReadWrite>;

pub fn parse_tls_mode_from_args(args: &[String]) -> Result<common::TlsMode, String> {
    match args.iter().find_map(|arg| arg.strip_prefix("tls=")) {
        Some(raw) => common::TlsMode::parse(raw)
            .ok_or_else(|| format!("invalid tls mode '{raw}'; use off|optional|required")),
        None => Ok(common::TlsMode::Optional),
    }
}

pub fn parse_tls_config_from_args(args: &[String]) -> ServerTlsConfig {

    let cert_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_cert="))
        .map(std::path::PathBuf::from);

    let key_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_key="))
        .map(std::path::PathBuf::from);

    let ca_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_ca="))
        .map(std::path::PathBuf::from);

    ServerTlsConfig {
        cert_path,
        key_path,
        ca_path,
    }

}

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

pub fn build_tls_acceptor(config: &ServerTlsConfig) -> Result<TlsAcceptor, String> {

    let cert_path = config
        .cert_path
        .as_deref()
        .ok_or_else(|| "tls_cert is required when tls is optional|required".to_string())?;

    let key_path = config
        .key_path
        .as_deref()
        .ok_or_else(|| "tls_key is required when tls is optional|required".to_string())?;

    let cert_chain = load_tls_certificates(cert_path)?;
    let private_key = load_tls_private_key(key_path)?;

    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|err| format!("invalid tls cert/key pair: {err}"))?;

    tls_config.alpn_protocols = vec![b"distdb-p2p/1".to_vec()];

    Ok(TlsAcceptor::from(Arc::new(tls_config)))

}

pub fn build_tls_client_config(config: &ServerTlsConfig) -> Result<Arc<ClientConfig>, String> {

    let root_path = config
        .ca_path
        .as_deref()
        .or(config.cert_path.as_deref())
        .ok_or_else(|| {
            "tls_ca (or tls_cert for self-signed trust) is required for outbound tls"
                .to_string()
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

fn looks_like_tls_client_hello(buf: &[u8]) -> bool {
    // TLS record: ContentType(22=Handshake), Version(3,x)
    buf.len() >= 3 && buf[0] == 22 && buf[1] == 3 && (1..=4).contains(&buf[2])
}

pub async fn negotiate_connector_stream(
    stream: TcpStream,
    peer_addr: &str,
    tls_mode: common::TlsMode,
    tls_acceptor: Option<TlsAcceptor>,
) -> Result<BoxedConnectorStream, Box<dyn std::error::Error + Send + Sync>> {

    match tls_mode {

        common::TlsMode::Off => Ok(Box::new(stream)),

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

        common::TlsMode::Optional => {
            let Some(acceptor) = tls_acceptor else {
                return Ok(Box::new(stream));
            };

            let mut probe = [0u8; 8];
            let bytes_peeked = stream.peek(&mut probe).await?;
            if looks_like_tls_client_hello(&probe[..bytes_peeked]) {
                let tls_stream = acceptor.accept(stream).await.map_err(|err| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        format!("optional tls handshake failed for {peer_addr}: {err}"),
                    )
                })?;
                return Ok(Box::new(tls_stream));
            }

            Ok(Box::new(stream))
        }

    }
    
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn default_tls_mode_is_optional() {
        let args: Vec<String> = vec![];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Optional, "TLS must default to optional for secure-by-default behaviour");
    }

    #[test]
    fn explicit_tls_off_overrides_default() {
        let args = vec!["tls=off".to_string()];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Off);
    }

    #[test]
    fn explicit_tls_required_is_accepted() {
        let args = vec!["tls=required".to_string()];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Required);
    }

    #[test]
    fn invalid_tls_mode_is_rejected() {
        let args = vec!["tls=unsafe".to_string()];
        assert!(parse_tls_mode_from_args(&args).is_err());
    }

}
