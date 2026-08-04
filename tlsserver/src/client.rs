
use std::io::{Read, Write};

use crate::protocol::{
    TlsCertificateRequest, TlsCertificateResponse, decode_tls_certificate_response,
    encode_tls_certificate_request,
};

fn write_tls_certificate_request_frame(
    stream: &mut impl Write,
    request: &TlsCertificateRequest,
    addr: &str,
) -> Result<(), String> {

    let payload = encode_tls_certificate_request(request)
        .ok_or_else(|| "failed to encode tls certificate request".to_string())?;

    let len = payload.len() as u32;
    
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|err| format!("write tls certificate request to '{}' failed: {}", addr, err))

}

fn read_tls_certificate_response_frame(
    stream: &mut impl Read,
    addr: &str,
) -> Result<TlsCertificateResponse, String> {

    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .map_err(|err| format!("read tls certificate response header from '{}' failed: {}", addr, err))?;

    let payload_len = u32::from_le_bytes(header) as usize;
    let mut response_payload = vec![0u8; payload_len];
    stream
        .read_exact(&mut response_payload)
        .map_err(|err| format!("read tls certificate response payload from '{}' failed: {}", addr, err))?;

    decode_tls_certificate_response(&response_payload)
        .ok_or_else(|| format!("decode tls certificate response from '{}' failed", addr))
        
}

pub fn request_certificate_from_tls_server(
    addr: &str,
    request: &TlsCertificateRequest,
) -> Result<TlsCertificateResponse, String> {

    let mut stream = std::net::TcpStream::connect(addr)
        .map_err(|err| format!("connect to tls-server '{}' failed: {}", addr, err))?;

    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|err| format!("set write timeout for tls-server '{}' failed: {}", addr, err))?;

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|err| format!("set read timeout for tls-server '{}' failed: {}", addr, err))?;

    write_tls_certificate_request_frame(&mut stream, request, addr)?;
    read_tls_certificate_response_frame(&mut stream, addr)

}

#[cfg(test)]
pub(crate) fn encode_request_frame_for_test(request: &TlsCertificateRequest) -> Vec<u8> {

    let mut out = Vec::new();
    
    write_tls_certificate_request_frame(&mut out, request, "test")
        .expect("request frame should encode");
    
    out
    
}

#[cfg(test)]
pub(crate) fn decode_response_frame_for_test(payload: Vec<u8>) -> Result<TlsCertificateResponse, String> {
    let mut cursor = std::io::Cursor::new(payload);
    read_tls_certificate_response_frame(&mut cursor, "test")
}