use security::ensure_or_generate_tls_cert;
use tlsserver::service::serve_tls_certificate_requests;

const DEFAULT_TLS_SERVER_NODE_ID: &str = "tlsserver-node-01";
const DEFAULT_TLS_SERVER_PORT: u16 = 5443;

fn parse_ca_root_from_args(args: &[String]) -> bool {

    if args.iter().any(|arg| arg == "ca_root") {
        return true;
    }

    args
        .iter()
        .find_map(|arg| arg.strip_prefix("ca_root="))
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"))
        .unwrap_or(true)

}

fn main() -> Result<(), Box<dyn std::error::Error>> {

    env_logger::init();

    let args = std::env::args().collect::<Vec<_>>();
    
    let node_id = args
        .iter()
        .find_map(|arg| arg.strip_prefix("node_id=").map(ToOwned::to_owned))
        .unwrap_or_else(|| DEFAULT_TLS_SERVER_NODE_ID.to_string());

    let data_dir = args
        .iter()
        .find_map(|arg| arg.strip_prefix("datadir=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "./data".to_string());

    let listen_addr = args
        .iter()
        .find_map(|arg| arg.strip_prefix("listen_addr=").map(ToOwned::to_owned))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let port = args
        .iter()
        .find_map(|arg| arg.strip_prefix("port=").and_then(|v| v.parse::<u16>().ok()))
        .unwrap_or(DEFAULT_TLS_SERVER_PORT);

    let ca_root_enabled = parse_ca_root_from_args(&args);
    let bind_addr = format!("{}:{}", listen_addr, port);
    let node_data_dir = std::path::PathBuf::from(data_dir).join(&node_id);

    let issued_paths = ensure_or_generate_tls_cert(&node_data_dir, &node_id, &bind_addr, &[])
        .map_err(std::io::Error::other)?;

    log::info!(
        "tlsserver startup material ready before accept loop cert={} key={} ca={} ca_root={}",
        issued_paths.cert_path.display(),
        issued_paths.key_path.display(),
        issued_paths.ca_path.display(),
        ca_root_enabled,
    );

    serve_tls_certificate_requests(&bind_addr, &node_data_dir, ca_root_enabled)
        .map_err(std::io::Error::other)?;

    Ok(())
    
}
