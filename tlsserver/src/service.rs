use std::io::{Read, Write};
use std::path::Path;

use crate::issuance::build_tls_certificate_response;
use crate::protocol::{
    decode_tls_certificate_request, encode_tls_certificate_response,
};

pub fn serve_tls_certificate_requests(
    bind_addr: &str,
    node_data_dir: &Path,
    ca_root_enabled: bool,
) -> Result<(), String> {

    let listener = std::net::TcpListener::bind(bind_addr)
        .map_err(|err| format!("bind tlsserver listener '{}' failed: {}", bind_addr, err))?;

    log::info!("tlsserver listening on {}", bind_addr);

    for inbound in listener.incoming() {
        match inbound {
            Ok(mut stream) => {
                if let Err(err) = handle_tls_certificate_stream(&mut stream, node_data_dir, ca_root_enabled) {
                    log::warn!("tlsserver request handling failed: {}", err);
                }
            }
            Err(err) => {
                log::warn!("tlsserver accept failed: {}", err);
            }
        }
    }

    Ok(())
    
}

fn handle_tls_certificate_stream(
    stream: &mut std::net::TcpStream,
    node_data_dir: &Path,
    ca_root_enabled: bool,
) -> Result<(), String> {

    let mut len_buf = [0u8; 4];

    stream
        .read_exact(&mut len_buf)
        .map_err(|err| format!("read tls certificate request header failed: {}", err))?;

    let frame_len = u32::from_le_bytes(len_buf) as usize;
    let mut payload = vec![0u8; frame_len];
    
    stream
        .read_exact(&mut payload)
        .map_err(|err| format!("read tls certificate request payload failed: {}", err))?;

    let request = decode_tls_certificate_request(&payload)
        .ok_or_else(|| "invalid tls certificate request payload".to_string())?;

    log::info!(
        "tlsserver certificate request requester_id={} requested_for={} request_id={}",
        request.requester_id,
        request.requested_for,
        request.request_id,
    );

    let response = build_tls_certificate_response(node_data_dir, ca_root_enabled, &request);
    let encoded = encode_tls_certificate_response(&response)
        .ok_or_else(|| "failed to encode tls certificate response".to_string())?;

    let len = encoded.len() as u32;
    
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&encoded))
        .and_then(|_| stream.flush())
        .map_err(|err| format!("write tls certificate response failed: {}", err))?;

    Ok(())

}