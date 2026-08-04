use std::io::{Read, Write};
use std::sync::{Arc, OnceLock};

use rustls::pki_types::ServerName;
use rustls::{ClientConfig, ClientConnection, StreamOwned};
use peerlib::ServiceMessage;

use crate::core::comms::p2p_wire::{decode_service_message, encode_service_message};

#[derive(Clone)]
struct OutboundTlsState {
    mode: common::TlsMode,
    client_config: Option<Arc<ClientConfig>>,
}

impl Default for OutboundTlsState {
    fn default() -> Self {
        Self {
            mode: common::TlsMode::Off,
            client_config: None,
        }
    }
}

#[expect(clippy::large_enum_variant, reason="necessary to support both plain and tls outbound streams without heap allocation")]
enum OutboundServiceStream {
    Plain(std::net::TcpStream),
    Tls(StreamOwned<ClientConnection, std::net::TcpStream>),
}

impl Read for OutboundServiceStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for OutboundServiceStream {

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }

}

impl OutboundServiceStream {

    fn set_write_timeout(&self, timeout: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.set_write_timeout(timeout),
            Self::Tls(stream) => stream.sock.set_write_timeout(timeout),
        }
    }

    fn set_read_timeout(&self, timeout: Option<std::time::Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.set_read_timeout(timeout),
            Self::Tls(stream) => stream.sock.set_read_timeout(timeout),
        }
    }

}

static OUTBOUND_TLS_STATE: OnceLock<OutboundTlsState> = OnceLock::new();

pub fn configure_outbound_tls_state(
    mode: common::TlsMode,
    client_config: Option<Arc<ClientConfig>>,
) {
    let _ = OUTBOUND_TLS_STATE.set(OutboundTlsState { mode, client_config });
}

fn outbound_tls_state() -> OutboundTlsState {
    OUTBOUND_TLS_STATE.get().cloned().unwrap_or_default()
}

fn outbound_server_name_from_addr(addr: &str) -> Result<ServerName<'static>, String> {

    let host = addr
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(addr)
        .trim_matches('[')
        .trim_matches(']');

    if host.is_empty() {
        return Err(format!("unable to derive tls server name from addr '{addr}'"));
    }

    ServerName::try_from(host.to_string()).map_err(|_| format!("invalid tls server name '{}'", host))
}

fn connect_tls_outbound_stream(
    addr: &str,
    client_config: Arc<ClientConfig>,
) -> Result<OutboundServiceStream, String> {

    let tcp = std::net::TcpStream::connect(addr)
        .map_err(|err| format!("connect to {} failed: {}", addr, err))?;

    let server_name = outbound_server_name_from_addr(addr)?;
    let connection = ClientConnection::new(client_config, server_name)
        .map_err(|err| format!("create tls client connection for {} failed: {}", addr, err))?;

    let mut tls_stream = StreamOwned::new(connection, tcp);
    
    tls_stream
        .conn
        .complete_io(&mut tls_stream.sock)
        .map_err(|err| format!("tls handshake with {} failed: {}", addr, err))?;

    Ok(OutboundServiceStream::Tls(tls_stream))

}

fn connect_outbound_service_stream(
    addr: &str,
) -> Result<OutboundServiceStream, serverlib::helpers::error::ServerLibError> {

    let state = outbound_tls_state();

    match state.mode {
        common::TlsMode::Required => {
            let client_config = state.client_config.ok_or_else(|| {
                serverlib::helpers::error::ServerLibError::Network(
                    "tls required but outbound tls client is not configured".to_string(),
                )
            })?;

            connect_tls_outbound_stream(addr, client_config)
                .map_err(serverlib::helpers::error::ServerLibError::Network)
        }

        _ => Err(serverlib::helpers::error::ServerLibError::Network(
            "p2p network requires tls=required".to_string(),
        )),
    }

}

fn consume_initial_challenge_frame(
    stream: &mut OutboundServiceStream,
    addr: &str,
) -> serverlib::helpers::error::Result<()> {

    let mut header = [0u8; 4];
    stream.read_exact(&mut header).map_err(|err| {
        serverlib::helpers::error::ServerLibError::Network(format!(
            "read initial challenge header from {} failed: {}",
            addr, err
        ))
    })?;

    let payload_len = u32::from_le_bytes(header) as usize;
    let mut payload = vec![0u8; payload_len];

    stream.read_exact(&mut payload).map_err(|err| {
        serverlib::helpers::error::ServerLibError::Network(format!(
            "read initial challenge payload from {} failed: {}",
            addr, err
        ))
    })?;

    Ok(())

}

pub fn send_service_message_to_addr(
    addr: &str,
    message: &ServiceMessage,
) -> serverlib::helpers::error::Result<()> {

    let mut stream = connect_outbound_service_stream(addr)?;

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(2)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set read timeout for {} failed: {}",
                addr, err
            ))
        })?;

    consume_initial_challenge_frame(&mut stream, addr)?;

    stream
        .set_write_timeout(Some(std::time::Duration::from_millis(500)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set write timeout for {} failed: {}",
                addr, err
            ))
        })?;

    let payload = encode_service_message(message).ok_or_else(|| {
        serverlib::helpers::error::ServerLibError::Network(
            "unsupported service message for wire encoding".to_string(),
        )
    })?;

    let len = payload.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "write service frame to {} failed: {}",
                addr, err
            ))
        })?;

    Ok(())

}

pub fn send_service_request_to_addr(
    addr: &str,
    message: &ServiceMessage,
) -> serverlib::helpers::error::Result<Option<ServiceMessage>> {

    let mut stream = connect_outbound_service_stream(addr)?;

    stream
        .set_write_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set write timeout for {} failed: {}",
                addr, err
            ))
        })?;

    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "set read timeout for {} failed: {}",
                addr, err
            ))
        })?;

    consume_initial_challenge_frame(&mut stream, addr)?;

    let payload = encode_service_message(message).ok_or_else(|| {
        serverlib::helpers::error::ServerLibError::Network(
            "unsupported service message for wire encoding".to_string(),
        )
    })?;

    let len = payload.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|err| {
            serverlib::helpers::error::ServerLibError::Network(format!(
                "write service frame to {} failed: {}",
                addr, err
            ))
        })?;

    let mut header = [0u8; 4];

    if let Err(err) = stream.read_exact(&mut header) {
        let timed_out = matches!(
            err.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        );
        if timed_out {
            return Ok(None);
        }

        return Err(serverlib::helpers::error::ServerLibError::Network(format!(
            "read response header from {} failed: {}",
            addr, err
        )));
    }

    let payload_len = u32::from_le_bytes(header) as usize;
    let mut response_payload = vec![0u8; payload_len];

    stream.read_exact(&mut response_payload).map_err(|err| {
        serverlib::helpers::error::ServerLibError::Network(format!(
            "read response payload from {} failed: {}",
            addr, err
        ))
    })?;

    decode_service_message(&response_payload).map(Some).ok_or_else(|| {
        serverlib::helpers::error::ServerLibError::Network(format!(
            "decode response message from {} failed",
            addr
        ))
    })

}
