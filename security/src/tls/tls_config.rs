use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TlsConfig {
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub ca_path: Option<PathBuf>,
    pub issuer_addr: Option<String>,
}

pub fn parse_tls_config_from_args(args: &[String]) -> TlsConfig {
    
    let cert_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_cert="))
        .map(PathBuf::from);

    let key_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_key="))
        .map(PathBuf::from);

    let ca_path = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_ca="))
        .map(PathBuf::from);

    let issuer_addr = args
        .iter()
        .find_map(|arg| arg.strip_prefix("tls_server="))
        .map(ToOwned::to_owned);

    TlsConfig {
        cert_path,
        key_path,
        ca_path,
        issuer_addr,
    }

}
