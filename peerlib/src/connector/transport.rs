use connector::{
    ConnectorError, ConnectorRequest, ConnectorResponse, ConnectorResult,
    ConnectorTransport, ResponseStatus,
};

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Cursor, Read, Write};
use std::net::IpAddr;
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use common::{DEFAULT_SERVER_PORT, PeerSession, epoch_nanos};
use common::helpers::utils::{md5};
use rustls::DigitallySignedStruct;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, ClientConnection, Error as RustlsError, RootCertStore, SignatureScheme, StreamOwned};
use sha2::{Digest, Sha256};
use security::platform_tls_root_cert_pem;
use x509_parser::prelude::{FromDer, X509Certificate};

const SERVER_PASSWORD_CHALLENGE_REQUEST_ID: &str = "__p2p_password_challenge__";
const SERVER_BOOTSTRAP_REJECT_REQUEST_ID: &str = "__distdb_bootstrap__";
const CONNECTOR_STREAM_TIMEOUT_SECS_DEFAULT: u64 = 300;
const CONNECTOR_CONNECT_TIMEOUT_SECS_DEFAULT: u64 = 3;
const CONNECTOR_HANDSHAKE_TIMEOUT_SECS_DEFAULT: u64 = 60;
const CONNECTOR_CONNECT_RETRY_ATTEMPTS_DEFAULT: u64 = 3;
const CONNECTOR_STREAM_TIMEOUT_SECS_ENV: &str = "DISTDB_CONNECTOR_STREAM_TIMEOUT_SECS";
const CONNECTOR_CONNECT_TIMEOUT_SECS_ENV: &str = "DISTDB_CONNECTOR_CONNECT_TIMEOUT_SECS";
const CONNECTOR_HANDSHAKE_TIMEOUT_SECS_ENV: &str = "DISTDB_CONNECTOR_HANDSHAKE_TIMEOUT_SECS";
const CONNECTOR_CONNECT_RETRY_ATTEMPTS_ENV: &str = "DISTDB_CONNECTOR_CONNECT_RETRY_ATTEMPTS";
const PLATFORM_TLS_FINGERPRINT_ENV: &str = "DISTDB_PLATFORM_TLS_FINGERPRINT";
const CONNECTOR_TLS_FINGERPRINT_ENV: &str = "DISTDB_CONNECTOR_TLS_FINGERPRINT";
const CONNECTOR_TLS_FINGERPRINT_FILE: &str = "ca-fingerprint.sha256";
const MAX_QUEUED_RESPONSES: usize = 8192;

#[derive(Debug)]
struct FingerprintServerCertVerifier {
    expected_fingerprint: String,
    supported_algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
}

impl FingerprintServerCertVerifier {
    fn new(
        expected_fingerprint: String,
        supported_algorithms: rustls::crypto::WebPkiSupportedAlgorithms,
    ) -> Self {
        Self {
            expected_fingerprint,
            supported_algorithms,
        }
    }
}

impl ServerCertVerifier for FingerprintServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        let presented_fingerprints = presented_chain_fingerprints(end_entity, _intermediates);

        if presented_fingerprints
            .iter()
            .any(|actual| actual == &self.expected_fingerprint)
        {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(RustlsError::General(format!(
                "tls fingerprint mismatch: expected '{}' got presented '{}'",
                self.expected_fingerprint,
                presented_fingerprints.join(",")
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.supported_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.supported_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.supported_algorithms.supported_schemes()
    }
}

fn connector_connect_timeout_secs() -> u64 {
    std::env::var(CONNECTOR_CONNECT_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
    .map(|value| value.clamp(1, 60))
        .unwrap_or(CONNECTOR_CONNECT_TIMEOUT_SECS_DEFAULT)
}

fn connector_stream_timeout_secs() -> u64 {
    std::env::var(CONNECTOR_STREAM_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.clamp(5, 3600))
        .unwrap_or(CONNECTOR_STREAM_TIMEOUT_SECS_DEFAULT)
}

fn connector_handshake_timeout_secs() -> u64 {
    std::env::var(CONNECTOR_HANDSHAKE_TIMEOUT_SECS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.clamp(5, 300))
        .unwrap_or(CONNECTOR_HANDSHAKE_TIMEOUT_SECS_DEFAULT)
}

fn connector_connect_retry_attempts() -> u64 {
    std::env::var(CONNECTOR_CONNECT_RETRY_ATTEMPTS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(|value| value.clamp(1, 10))
        .unwrap_or(CONNECTOR_CONNECT_RETRY_ATTEMPTS_DEFAULT)
}

fn is_transient_connect_error(err: &ConnectorError) -> bool {
    match err {
        ConnectorError::Transport(message) => {
            let normalized = message.to_ascii_lowercase();
            normalized.contains("timed out")
                || normalized.contains("resource temporarily unavailable")
                || normalized.contains("would block")
                || normalized.contains("os error 35")
        }
        _ => false,
    }
}

fn normalize_tls_fingerprint(raw: &str) -> Option<String> {
    let normalized = raw
        .chars()
        .filter(|ch| ch.is_ascii_hexdigit())
        .collect::<String>()
        .to_ascii_lowercase();

    if normalized.len() == 64 {
        Some(normalized)
    } else {
        None
    }
}

fn try_load_local_dev_fingerprint(socket_addr: &str) -> Option<String> {
    if !is_local_loopback_socket_addr(socket_addr) {
        return None;
    }

    let candidates = [
        PathBuf::from(format!("../server/data/p2p-tls/{}", CONNECTOR_TLS_FINGERPRINT_FILE)),
        PathBuf::from(format!("./data/p2p-tls/{}", CONNECTOR_TLS_FINGERPRINT_FILE)),
        PathBuf::from(format!("../data/p2p-tls/{}", CONNECTOR_TLS_FINGERPRINT_FILE)),
    ];

    for candidate in candidates {
        let Ok(raw) = std::fs::read_to_string(&candidate) else {
            continue;
        };
        if let Some(normalized) = normalize_tls_fingerprint(raw.trim()) {
            log::info!(
                "connector loaded local dev fingerprint from {} for peer bootstrap",
                candidate.display()
            );
            return Some(normalized);
        }
    }

    None
}

fn global_tls_fingerprint(socket_addr: &str) -> Option<String> {

    if let Ok(raw) = std::env::var(PLATFORM_TLS_FINGERPRINT_ENV) {
        if let Some(normalized) = normalize_tls_fingerprint(raw.trim()) {
            return Some(normalized);
        }

        log::warn!(
            "ignoring invalid platform TLS fingerprint from {}",
            PLATFORM_TLS_FINGERPRINT_ENV
        );
    }

    if let Ok(raw) = std::env::var(CONNECTOR_TLS_FINGERPRINT_ENV) {
        if let Some(normalized) = normalize_tls_fingerprint(raw.trim()) {
            return Some(normalized);
        }

        log::warn!(
            "ignoring invalid connector TLS fingerprint from {}",
            CONNECTOR_TLS_FINGERPRINT_ENV
        );
    }

    if let Some(local_fp) = try_load_local_dev_fingerprint(socket_addr) {
        return Some(local_fp);
    }

    None
}

fn certificate_sha256_fingerprint(cert_der: &[u8]) -> String {
    let digest = Sha256::digest(cert_der);
    digest.iter().map(|byte| format!("{:02x}", byte)).collect::<String>()
}

fn certificate_spki_sha256_fingerprint(cert_der: &[u8]) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    let spki_der = cert.public_key().raw;
    let digest = Sha256::digest(spki_der);
    Some(digest.iter().map(|byte| format!("{:02x}", byte)).collect::<String>())
}

fn presented_chain_fingerprints(
    end_entity: &CertificateDer<'_>,
    intermediates: &[CertificateDer<'_>],
) -> Vec<String> {
    let mut fingerprints = Vec::with_capacity((intermediates.len() + 1) * 2);

    // Prefer issuer/intermediate identities first so pinned trust can remain stable
    // across end-entity certificate rotation.
    for cert in intermediates {
        if let Some(spki_fp) = certificate_spki_sha256_fingerprint(cert.as_ref())
            && !fingerprints.contains(&spki_fp)
        {
            fingerprints.push(spki_fp);
        }
        let fp = certificate_sha256_fingerprint(cert.as_ref());
        if !fingerprints.contains(&fp) {
            fingerprints.push(fp);
        }
    }

    if let Some(end_entity_spki_fp) = certificate_spki_sha256_fingerprint(end_entity.as_ref())
        && !fingerprints.contains(&end_entity_spki_fp)
    {
        fingerprints.push(end_entity_spki_fp);
    }

    let end_entity_fp = certificate_sha256_fingerprint(end_entity.as_ref());
    if !fingerprints.contains(&end_entity_fp) {
        fingerprints.push(end_entity_fp);
    }

    fingerprints
}

    fn is_local_loopback_socket_addr(socket_addr: &str) -> bool {
        let host = socket_addr
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(socket_addr)
            .trim_matches('[')
            .trim_matches(']')
            .to_ascii_lowercase();

        matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1")
    }

    fn try_load_local_dev_ca_pem(socket_addr: &str) -> Option<String> {
        if !is_local_loopback_socket_addr(socket_addr) {
            return None;
        }

        let candidates = [
            PathBuf::from("../server/data/p2p-tls/ca-cert.pem"),
            PathBuf::from("./data/p2p-tls/ca-cert.pem"),
            PathBuf::from("../data/p2p-tls/ca-cert.pem"),
        ];

        for candidate in candidates {
            let Ok(pem) = std::fs::read_to_string(&candidate) else {
                continue;
            };

            let trimmed = pem.trim();
            if trimmed.is_empty() {
                continue;
            }

            log::info!(
                "connector loaded local dev CA certificate from {} for peer bootstrap",
                candidate.display()
            );

            return Some(trimmed.to_string());
        }

        None
    }

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConnectorTlsConfig {
    pub mode: common::TlsMode,
    pub ca_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorDiscoveryMode {
    Kademlia,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorP2pConfig {
    pub protocol: String,
    pub bootstrap_peers: Vec<String>,
    pub tls: ConnectorTlsConfig,
}

impl ConnectorP2pConfig {
    pub fn new(protocol: impl Into<String>) -> Self {
        Self {
            protocol: protocol.into(),
            bootstrap_peers: Vec::new(),
            tls: ConnectorTlsConfig::default(),
        }
    }

    pub fn with_bootstrap_peers(mut self, peers: Vec<String>) -> Self {
        self.bootstrap_peers = peers;
        self
    }

    pub fn with_tls_mode(mut self, mode: common::TlsMode) -> Self {
        self.tls.mode = mode;
        self
    }

    pub fn with_tls_ca_path(mut self, ca_path: impl Into<PathBuf>) -> Self {
        self.tls.ca_path = Some(ca_path.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorPeer {
    pub peer_id: String,
    pub addrs: Vec<String>,
    pub is_discovered: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectorP2pTransport {
    config: ConnectorP2pConfig,
    peers: HashMap<String, ConnectorPeer>,
    active_peer_id: Option<String>,
    queued_responses: Arc<Mutex<HashMap<String, ConnectorResponse>>>,
    live_connection: Arc<Mutex<Option<LiveConnection>>>,
    cached_ca_pem: Arc<Mutex<Option<String>>>,
}

#[derive(Debug)]
struct LiveConnection {
    peer_id: String,
    stream: ConnectorWireStream,
    session: PeerSession,
}

enum ConnectorWireStream {
    Tls(StreamOwned<ClientConnection, TcpStream>),
}

impl std::fmt::Debug for ConnectorWireStream {
    
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        match self {
            Self::Tls(_) => f.write_str("ConnectorWireStream::Tls"),
        }

    }

}

impl Read for ConnectorWireStream {
    
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {

        match self {
            Self::Tls(stream) => stream.read(buf),
        }

    }

}

impl Write for ConnectorWireStream {

    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {

        match self {
            Self::Tls(stream) => stream.write(buf),
        }

    }

    fn flush(&mut self) -> std::io::Result<()> {

        match self {
            Self::Tls(stream) => stream.flush(),
        }

    }

}

impl ConnectorWireStream {

    fn set_timeouts(
        &mut self,
        read_timeout: Option<std::time::Duration>,
        write_timeout: Option<std::time::Duration>,
    ) -> Result<(), ConnectorError> {

        match self {
            Self::Tls(stream) => {

                let tcp = stream.get_mut();

                tcp.set_read_timeout(read_timeout)
                    .map_err(|e| ConnectorError::Transport(format!("failed to set read timeout: {e}")))?;

                tcp.set_write_timeout(write_timeout)
                    .map_err(|e| ConnectorError::Transport(format!("failed to set write timeout: {e}")))?;

                Ok(())

            }

        }

    }

}

impl ConnectorP2pTransport {

    pub fn new(config: ConnectorP2pConfig) -> Self {

        Self {
            config,
            peers: HashMap::new(),
            active_peer_id: None,
            queued_responses: Arc::new(Mutex::new(HashMap::new())),
            live_connection: Arc::new(Mutex::new(None)),
            cached_ca_pem: Arc::new(Mutex::new(None)),
        }

    }

    pub fn cached_ca_pem(&self) -> Option<String> {

        self.cached_ca_pem
            .lock()
            .ok()
            .and_then(|guard| guard.clone())

    }

    pub fn discovery_mode(&self) -> ConnectorDiscoveryMode {
        ConnectorDiscoveryMode::Kademlia
    }

    pub fn protocol(&self) -> &str {
        &self.config.protocol
    }

    pub fn bootstrap_peers(&self) -> &[String] {
        &self.config.bootstrap_peers
    }

    pub fn tls_mode(&self) -> common::TlsMode {
        self.config.tls.mode
    }

    pub fn tls_ca_path(&self) -> Option<&PathBuf> {
        self.config.tls.ca_path.as_ref()
    }

    fn normalize_peer_addrs(addrs: &[String]) -> Vec<String> {

        let mut normalized = addrs.to_vec();

        normalized.sort_by(|left, right| {

            let left_is_loopback = left.trim().split(':').next().is_some_and(|host| {
                let host = host.trim_matches('[').trim_matches(']').to_ascii_lowercase();
                matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1")
            });

            let right_is_loopback = right.trim().split(':').next().is_some_and(|host| {
                let host = host.trim_matches('[').trim_matches(']').to_ascii_lowercase();
                matches!(host.as_str(), "127.0.0.1" | "localhost" | "::1")
            });

            match (left_is_loopback, right_is_loopback) {
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                _ => std::cmp::Ordering::Equal,
            }

        });

        normalized

    }

    fn peer_addrs_share_port(left: &[String], right: &[String]) -> bool {

        let right_ports = right
            .iter()
            .filter_map(|addr| normalize_peer_addr(addr)
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
            )
            .collect::<HashSet<_>>();

        left
            .iter()
            .filter_map(|addr| normalize_peer_addr(addr)
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>()
                .ok())
            )
            .any(|port| right_ports.contains(&port))

    }

    fn peer_is_loopback_only(addrs: &[String]) -> bool {
        addrs.iter().all(|addr| is_local_loopback_socket_addr(addr))
    }

    fn merge_peer_addrs(existing: &[String], incoming: &[String]) -> Vec<String> {

        let mut merged = Vec::with_capacity(existing.len() + incoming.len());
        let mut seen = HashSet::with_capacity(existing.len() + incoming.len());

        for addr in existing.iter().map(|addr| normalize_peer_addr(addr)) {
            if seen.insert(addr.clone()) {
                merged.push(addr);
            }
        }

        for addr in incoming.iter().map(|addr| normalize_peer_addr(addr)) {
            if seen.insert(addr.clone()) {
                merged.push(addr);
            }
        }

        Self::normalize_peer_addrs(&merged)

    }

    fn find_public_merge_target_peer_id(
        &self,
        peer_id: &str,
        peer_addrs: &[String],
        incoming_is_loopback_only: bool,
    ) -> Option<String> {

        if !incoming_is_loopback_only {
            return None;
        }

        self.peers
            .iter()
            .find(|(existing_peer_id, existing_peer)| {
                existing_peer_id.as_str() != peer_id
                    && Self::peer_addrs_share_port(&existing_peer.addrs, peer_addrs)
                    && existing_peer
                        .addrs
                        .iter()
                        .any(|addr| !is_local_loopback_socket_addr(addr))
            })
            .map(|(existing_peer_id, _)| existing_peer_id.clone())
            
    }

    fn find_bootstrap_alias_peer_id(
        &self,
        peer_id: &str,
        peer_addrs: &[String],
        incoming_has_non_loopback: bool,
    ) -> Option<String> {

        if !incoming_has_non_loopback {
            return None;
        }

        let bootstrap_addrs = self
            .config
            .bootstrap_peers
            .iter()
            .map(|addr| normalize_peer_addr(addr))
            .collect::<HashSet<_>>();

        self.peers
            .iter()
            .find(|(existing_peer_id, existing_peer)| {
                existing_peer_id.as_str() != peer_id
                    && Self::peer_addrs_share_port(&existing_peer.addrs, peer_addrs)
                    && Self::peer_is_loopback_only(&existing_peer.addrs)
                    && (bootstrap_addrs.contains(&normalize_peer_addr(existing_peer_id))
                        || existing_peer
                            .addrs
                            .iter()
                            .map(|addr| normalize_peer_addr(addr))
                            .any(|addr| bootstrap_addrs.contains(&addr)))
            })
            .map(|(existing_peer_id, _)| existing_peer_id.clone())

    }

    fn rebind_live_connection_peer_id(&self, old_peer_id: &str, new_peer_id: &str) {
        if let Ok(mut connection) = self.live_connection.lock()
            && let Some(live) = connection.as_mut()
            && live.peer_id == old_peer_id {
                log::debug!(
                    "connector transport rebound live connection peer old_peer_id={} new_peer_id={}",
                    old_peer_id,
                    new_peer_id
                );
                live.peer_id = new_peer_id.to_string();
            }
    }

    pub fn upsert_peer(&mut self, peer: ConnectorPeer) {

        let peer_id = peer.peer_id.clone();
        let existing_peer = self.peers.get(&peer_id).cloned();
        let is_discovered = peer.is_discovered || existing_peer.as_ref().is_some_and(|existing| existing.is_discovered);
        let mut normalized_peer = peer.clone();

        let mut merged_addrs = existing_peer
            .as_ref()
            .map(|existing| {
                existing
                    .addrs
                    .iter()
                    .map(|addr| normalize_peer_addr(addr))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let mut merged_addr_set = merged_addrs.iter().cloned().collect::<HashSet<_>>();

        for addr in peer.addrs.iter().map(|addr| normalize_peer_addr(addr)) {
            if merged_addr_set.insert(addr.clone()) {
                merged_addrs.push(addr);
            }
        }

        normalized_peer.addrs = Self::normalize_peer_addrs(&merged_addrs);

        let incoming_is_loopback_only = Self::peer_is_loopback_only(&peer.addrs);
        let incoming_has_non_loopback = !incoming_is_loopback_only;

        if let Some(existing_peer_id) = self.find_public_merge_target_peer_id(
            &peer_id,
            &peer.addrs,
            incoming_is_loopback_only,
        ) {
            if let Some(existing_peer) = self.peers.get_mut(&existing_peer_id) {
                let merged_addrs = Self::merge_peer_addrs(&existing_peer.addrs, &peer.addrs);
                existing_peer.addrs = merged_addrs;
                existing_peer.is_discovered = existing_peer.is_discovered || is_discovered;
            }

            if self.active_peer_id.as_deref() == Some(peer_id.as_str()) {
                self.active_peer_id = Some(existing_peer_id);
            }
            return;
        }

        if let Some(existing_peer_id) = self.find_bootstrap_alias_peer_id(
            &peer_id,
            &peer.addrs,
            incoming_has_non_loopback,
        ) {
            self.rebind_live_connection_peer_id(&existing_peer_id, &peer_id);

            if let Some(existing_peer) = self.peers.remove(&existing_peer_id) {
                normalized_peer.addrs = Self::merge_peer_addrs(&existing_peer.addrs, &normalized_peer.addrs);
                log::debug!(
                    "connector transport merged bootstrap alias peer old_peer_id={} new_peer_id={} addrs={}",
                    existing_peer_id,
                    peer_id,
                    normalized_peer.addrs.join(",")
                );
            }

            if self.active_peer_id.as_deref() == Some(existing_peer_id.as_str()) {
                self.active_peer_id = Some(peer_id.clone());
            }
        }

        log::debug!(
            "connector transport upsert peer peer_id={} addrs={}",
            peer_id,
            normalized_peer.addrs.join(",")
        );

        let normalized_addrs = normalized_peer
            .addrs
            .iter()
            .cloned()
            .collect::<HashSet<_>>();

        let stale_peer_ids = self
            .peers
            .iter()
            .filter(|(existing_peer_id, existing_peer)| {

                **existing_peer_id != peer_id
                    && existing_peer
                        .addrs
                        .iter()
                        .any(|existing_addr| normalized_addrs.contains(existing_addr))
                        
            })
            .map(|(existing_peer_id, _)| existing_peer_id.clone())
            .collect::<Vec<_>>();

        let active_was_stale = stale_peer_ids
            .iter()
            .any(|stale_peer_id| self.active_peer_id.as_deref() == Some(stale_peer_id.as_str()));

        for stale_peer_id in stale_peer_ids {
            log::debug!(
                "connector transport replacing stale peer identity old_peer_id={} new_peer_id={}",
                stale_peer_id,
                peer_id
            );
            self.rebind_live_connection_peer_id(&stale_peer_id, &peer_id);
            self.peers.remove(&stale_peer_id);
        }

        self.peers.insert(
            peer_id.clone(),
            ConnectorPeer {
                is_discovered,
                ..normalized_peer
            },
        );

        // First discovered peer becomes the sticky session peer.
        if is_discovered && (self.active_peer_id.is_none() || active_was_stale) {
            self.active_peer_id = Some(peer_id);
        }
        
    }

    pub fn discovered_peers(&self) -> Vec<ConnectorPeer> {
        self.peers
            .values()
            .filter(|peer| peer.is_discovered)
            .cloned()
            .collect()
    }

    pub fn known_peers(&self) -> Vec<ConnectorPeer> {
        self.peers.values().cloned().collect()
    }

    pub fn active_peer_id(&self) -> Option<&str> {
        self.active_peer_id.as_deref()
    }

    pub fn select_peer(&mut self, peer_id: impl AsRef<str>) -> Result<(), ConnectorError> {

        let peer_id = peer_id.as_ref();

        if self.peers.contains_key(peer_id) {
            if self.active_peer_id.as_deref() != Some(peer_id) {
                self.clear_live_connection("peer switch");
            }
            self.active_peer_id = Some(peer_id.to_string());
            log::info!("connector transport active peer set to {}", peer_id);
            return Ok(());
        }

        Err(ConnectorError::Transport(format!(
            "peer '{peer_id}' is not discovered"
        )))

    }

    pub fn active_peer(&self) -> Option<&ConnectorPeer> {

        self.active_peer_id
            .as_ref()
            .and_then(|peer_id| self.peers.get(peer_id))

    }

    /// Queue a response by request id. This is used by tests and by future
    /// network handlers that decode p2p responses and hand them to the client.
    pub fn queue_response(&mut self, response: ConnectorResponse) {

        log::debug!(
            "connector transport queue response request_id={} status={:?}",
            response.request_id,
            response.status
        );

        if let Ok(mut queued_responses) = self.queued_responses.lock() {

            if queued_responses.len() >= MAX_QUEUED_RESPONSES {
                log::debug!(
                    "resetting queued response cache at {} entries",
                    queued_responses.len()
                );
                queued_responses.clear();
            }

            queued_responses.insert(response.request_id.clone(), response);
        }

    }

    pub fn queued_response_count(&self) -> usize {
        
        self.queued_responses
            .lock()
            .map(|queued_responses| queued_responses.len())
            .unwrap_or(0)

    }

    pub fn has_live_connection(&self) -> bool {

        self.live_connection
            .lock()
            .map(|connection| connection.is_some())
            .unwrap_or(false)

    }

    pub fn connect_active_peer(&mut self) -> Result<(), ConnectorError> {

        if self.active_peer_id.is_none()
            && let Some(addr) = self.config.bootstrap_peers.first().cloned() {
                self.peers.entry(addr.clone()).or_insert(ConnectorPeer {
                    peer_id: addr.clone(),
                    addrs: vec![addr.clone()],
                    is_discovered: false,
                });
                self.active_peer_id = Some(addr);
            }

        let Some(peer) = self.active_peer().cloned() else {
            return Err(ConnectorError::Transport(
                "no connected peer selected for session routing".to_string(),
            ));
        };

        ensure_live_connection(self, &peer)

    }

    pub fn disconnect_active_peer(&self) {
        self.clear_live_connection("disconnect directive");
    }

    pub fn set_active_connection_timeouts(
        &self,
        read_timeout: Option<std::time::Duration>,
        write_timeout: Option<std::time::Duration>,
    ) -> Result<(), ConnectorError> {

        let mut connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_mut() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for timeout update".to_string(),
            ));
        };

        live.stream.set_timeouts(read_timeout, write_timeout)

    }

    pub fn set_session_auth_token(&self, token: Option<String>) -> Result<(), ConnectorError> {

        let mut connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_mut() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for auth token update".to_string(),
            ));
        };

        live.session.auth_token = token;
        
        Ok(())

    }

    pub fn session_auth_token(&self) -> Result<Option<String>, ConnectorError> {

        let connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_ref() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for auth token retrieval".to_string(),
            ));
        };

        Ok(live.session.auth_token.clone())
    }

    pub fn session_id(&self) -> Result<Option<String>, ConnectorError> {

        let connection = self
            .live_connection
            .lock()
            .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

        let Some(live) = connection.as_ref() else {
            return Err(ConnectorError::Transport(
                "no active peer connection for session id retrieval".to_string(),
            ));
        };

        Ok(live.session.session_id.clone())
    }

    fn clear_live_connection(&self, reason: &str) {
        
        if let Ok(mut connection) = self.live_connection.lock()
            && let Some(live) = connection.take() {
                log::info!(
                    "connector transport disconnected peer={} reason={}",
                    live.peer_id,
                    reason
                );
            }
        
    }

}

impl ConnectorTransport for ConnectorP2pTransport {

    fn request(&self, request: &ConnectorRequest) -> Result<ConnectorResponse, ConnectorError> {

        if self.peers.is_empty() && self.config.bootstrap_peers.is_empty() {
            log::warn!("connector transport request failed: no peers or bootstrap peers configured");
            return Err(ConnectorError::Transport(
                "no Kademlia peers available for routing".to_string(),
            ));
        }

        if self.active_peer_id.is_none() {
            log::warn!("connector transport request failed: no active peer selected");
            return Err(ConnectorError::Transport(
                "no connected peer selected for session routing".to_string(),
            ));
        }

        let has_live_connection = self.has_live_connection();

        if let Some(active_peer) = self.active_peer_id() {
            log::debug!(
                "connector transport routing request_id={} to peer={}",
                request.request_id,
                active_peer
            );
        }

        if !has_live_connection {
            let queued_response = self
                .queued_responses
                .lock()
                .ok()
                .and_then(|mut queued_responses| queued_responses.remove(&request.request_id));

            if let Some(response) = queued_response {
                log::debug!(
                    "connector transport returned queued response request_id={} status={:?}",
                    response.request_id,
                    response.status
                );
                return Ok(response);
            }
        }

        let peer = self.active_peer().ok_or_else(|| {
            ConnectorError::Transport(
                "no connected peer selected for session routing".to_string(),
            )
        })?;

        match send_request_over_tcp(self, peer, request) {
            Ok(response) => {
                log::debug!(
                    "connector transport received network response request_id={} status={:?}",
                    response.request_id,
                    response.status
                );
                Ok(response)
            }

            Err(err) => {
                log::warn!(
                    "connector transport network request failed for request_id={}: {}",
                    request.request_id,
                    err
                );
                Err(err)
            }
        }
    
    }

}

fn send_request_over_tcp(
    transport: &ConnectorP2pTransport,
    peer: &ConnectorPeer,
    request: &ConnectorRequest,
) -> Result<ConnectorResponse, ConnectorError> {

    ensure_live_connection(transport, peer)?;

    let mut connection = transport
        .live_connection
        .lock()
        .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

    let response = {
        let Some(live) = connection.as_mut() else {
            return Err(ConnectorError::Transport(
                "active connection missing after connect".to_string(),
            ));
        };
        send_request_frame(&mut live.stream, request)
    };

    if response.is_err() {
        let _ = connection.take();
    }

    response

}

fn ensure_live_connection(
    transport: &ConnectorP2pTransport,
    peer: &ConnectorPeer,
) -> Result<(), ConnectorError> {

    let mut connection = transport
        .live_connection
        .lock()
        .map_err(|_| ConnectorError::Transport("connector connection lock poisoned".to_string()))?;

    let should_reconnect = connection
        .as_ref()
        .map(|live| live.peer_id != peer.peer_id)
        .unwrap_or(true);

    if !should_reconnect {
        return Ok(());
    }

    if peer.addrs.is_empty() {
        return Err(ConnectorError::Transport(
            "active peer has no address for routing".to_string(),
        ));
    }

    let handshake_timeout_secs = connector_handshake_timeout_secs();
    let stream_timeout_secs = connector_stream_timeout_secs();
    let mut last_err: Option<ConnectorError> = None;

    let mut candidate_addrs = peer
        .addrs
        .iter()
        .map(|addr| normalize_peer_addr(addr))
        .collect::<Vec<_>>();

    for bootstrap_addr in transport
        .config
        .bootstrap_peers
        .iter()
        .map(|addr| normalize_peer_addr(addr))
    {
        if !candidate_addrs.contains(&bootstrap_addr) {
            candidate_addrs.push(bootstrap_addr);
        }
    }

    'next_addr: for socket_addr in candidate_addrs {

        // With legacy CA bootstrap removed, only an explicitly configured CA file
        // or the local development CA fallback can seed connector trust.
        let ca_pem_override = if transport.config.tls.ca_path.is_none() {

            let cached = transport.cached_ca_pem();

            if cached.is_none() {
                try_load_local_dev_ca_pem(&socket_addr)
            } else {
                log::debug!(
                    "connector using cached CA cert for peer={} addr={}",
                    peer.peer_id,
                    socket_addr
                );
                cached
            }

        } else {
            None
        };

        let cached_ca_pem = transport.cached_ca_pem();
        let ca_pem_ref = ca_pem_override.as_deref().or(cached_ca_pem.as_deref());

        log::debug!(
            "connector TLS root selection peer={} addr={} ca_override={} ca_cached={}",
            peer.peer_id,
            socket_addr,
            ca_pem_override.is_some(),
            cached_ca_pem.is_some()
        );

        let mut stream = {
            let mut attempt = 1;
            let max_attempts = connector_connect_retry_attempts();
            loop {
                match connect_connector_stream(&socket_addr, &transport.config.tls, ca_pem_ref) {
                    Ok(stream) => break stream,
                    Err(err) => {
                        let transient = is_transient_connect_error(&err);
                        if transient && attempt < max_attempts {
                            let backoff_ms = 200u64.saturating_mul(attempt);
                            log::warn!(
                                "connector transient connect failure peer={} addr={} attempt={}/{} err={} backoff_ms={}",
                                peer.peer_id,
                                socket_addr,
                                attempt,
                                max_attempts,
                                err,
                                backoff_ms
                            );
                            std::thread::sleep(std::time::Duration::from_millis(backoff_ms));
                            attempt += 1;
                            continue;
                        }

                        log::debug!(
                            "connector failed address candidate peer={} addr={} attempts={} err={}",
                            peer.peer_id,
                            socket_addr,
                            attempt,
                            err
                        );
                        last_err = Some(err);
                        continue 'next_addr;
                    }
                }
            }
        };

        let establish_result: Result<PeerSession, ConnectorError> = (|| {

            stream.set_timeouts(
                Some(std::time::Duration::from_secs(handshake_timeout_secs)),
                Some(std::time::Duration::from_secs(handshake_timeout_secs)),
            )?;

            let challenge = read_response_frame(&mut stream)?;

            if challenge.request_id == SERVER_BOOTSTRAP_REJECT_REQUEST_ID {
                return match (&challenge.status, &challenge.result) {
                    (ResponseStatus::Rejected, ConnectorResult::Error(message)) => {
                        Err(ConnectorError::Rejected(message.clone()))
                    }
                    _ => Err(ConnectorError::InvalidResponse(
                        "bootstrap rejection frame had unexpected status/result".to_string(),
                    )),
                };
            }

            if challenge.request_id != SERVER_PASSWORD_CHALLENGE_REQUEST_ID {
                return Err(ConnectorError::InvalidResponse(format!(
                    "missing server password challenge on connect; received request_id='{}'",
                    challenge.request_id
                )));
            }

            match (&challenge.status, &challenge.result) {
                (ResponseStatus::Rejected, ConnectorResult::Error(_message)) => {}
                _ => {
                    return Err(ConnectorError::InvalidResponse(
                        "server challenge frame had unexpected status/result".to_string(),
                    ));
                }
            }

            let server_session_id = match &challenge.result {
                ConnectorResult::Error(message) => extract_session_id(message),
                _ => None,
            };
            let shared_session_token = generate_shared_session_token(
                &peer.peer_id,
                server_session_id.as_deref(),
            );

            Ok(PeerSession::new().with_session_id(shared_session_token))

        })();

        match establish_result {

            Ok(session) => {

                log::info!(
                    "connector transport established persistent stream peer={} addr={}",
                    peer.peer_id,
                    socket_addr
                );

                stream.set_timeouts(
                    Some(std::time::Duration::from_secs(stream_timeout_secs)),
                    Some(std::time::Duration::from_secs(stream_timeout_secs)),
                )?;

                *connection = Some(LiveConnection {
                    peer_id: peer.peer_id.clone(),
                    stream,
                    session,
                });

                return Ok(());
            },

            Err(err) => {
                log::debug!(
                    "connector failed address candidate after connect peer={} addr={} err={}",
                    peer.peer_id,
                    socket_addr,
                    err
                );
                last_err = Some(err);
            }

        }

    }

    Err(last_err.unwrap_or_else(|| {
        ConnectorError::Transport("failed to establish connection to any peer address".to_string())
    }))

}

fn send_request_frame(
    stream: &mut ConnectorWireStream,
    request: &ConnectorRequest,
) -> Result<ConnectorResponse, ConnectorError> {

    let payload = common::helpers::bincode_compat::serialize(request).map_err(|e| {
        ConnectorError::Transport(format!("failed to serialize request payload: {e}"))
    })?;

    let len = payload.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .and_then(|_| stream.write_all(&payload))
        .map_err(|e| ConnectorError::Transport(format!("failed to write request: {e}")))?;

    stream
        .flush()
        .map_err(|e| ConnectorError::Transport(format!("failed to flush request: {e}")))?;

    read_response_frame(stream)

}

fn read_response_frame(stream: &mut ConnectorWireStream) -> Result<ConnectorResponse, ConnectorError> {

    let mut response_len_buf = [0u8; 4];

    stream
        .read_exact(&mut response_len_buf)
        .map_err(|e| ConnectorError::Transport(format!("failed to read response length: {e}")))?;

    let response_len = u32::from_le_bytes(response_len_buf) as usize;
    let mut response_buf = vec![0u8; response_len];

    stream
        .read_exact(&mut response_buf)
        .map_err(|e| ConnectorError::Transport(format!("failed to read response payload: {e}")))?;

    common::helpers::bincode_compat::deserialize::<ConnectorResponse>(&response_buf)
        .map_err(|e| ConnectorError::Transport(format!("failed to decode response payload: {e}")))

}

fn load_tls_root_store(path: &PathBuf) -> Result<RootCertStore, ConnectorError> {

    let file = File::open(path).map_err(|err| {
        ConnectorError::Transport(format!("failed to open tls CA file '{}': {err}", path.display()))
    })?;

    load_tls_root_store_from_reader(&mut std::io::BufReader::new(file), &path.display().to_string())

}

fn load_tls_root_store_from_pem(pem: &str) -> Result<RootCertStore, ConnectorError> {
    let cursor = Cursor::new(pem.as_bytes());
    load_tls_root_store_from_reader(&mut BufReader::new(cursor), "<in-memory>")
}

fn load_tls_root_store_from_reader<R: Read>(
    reader: &mut BufReader<R>,
    source_label: &str,
) -> Result<RootCertStore, ConnectorError> {

    let certs = rustls_pemfile::certs(reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| {
            ConnectorError::Transport(format!(
                "failed to parse tls CA from '{}': {err}",
                source_label
            ))
        })?;

    if certs.is_empty() {
        return Err(ConnectorError::Transport(format!(
            "tls CA from '{}' is empty",
            source_label
        )));
    }

    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots.add(cert).map_err(|err| {
            ConnectorError::Transport(format!(
                "failed to add tls root from '{}': {err}",
                source_label
            ))
        })?;
    }

    Ok(roots)

}

fn load_system_tls_root_store() -> Result<RootCertStore, ConnectorError> {
    let certs = rustls_native_certs::load_native_certs();

    if certs.certs.is_empty() {
        return Err(ConnectorError::Transport(
            "system trust store is empty".to_string(),
        ));
    }

    let mut roots = RootCertStore::empty();

    for cert in certs.certs {
        roots.add(cert).map_err(|err| {
            ConnectorError::Transport(format!(
                "failed to add system trust anchor: {err}"
            ))
        })?;
    }

    Ok(roots)
}

fn server_names_from_socket_addr(socket_addr: &str) -> Vec<String> {

    let host = socket_addr
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(socket_addr)
        .trim_matches('[')
        .trim_matches(']');

    let mut candidates = Vec::new();

    if host.is_empty() {
        return candidates;
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        candidates.push(ip.to_string());
        if ip.is_loopback() {
            candidates.push("localhost".to_string());
        }
    } else {
        candidates.push(host.to_string());
        candidates.push("localhost".to_string());
    }

    candidates.dedup();
    candidates

}

fn server_name_from_socket_addr(socket_addr: &str) -> Result<ServerName<'static>, ConnectorError> {

    let candidates = server_names_from_socket_addr(socket_addr);
    if candidates.is_empty() {
        return Err(ConnectorError::Transport(format!(
            "cannot derive tls server name from '{socket_addr}'"
        )));
    }

    for candidate in &candidates {
        if let Ok(ip) = candidate.parse::<IpAddr>() {
            return Ok(ServerName::IpAddress(ip.into()));
        }

        if let Ok(name) = ServerName::try_from(candidate.clone()) {
            return Ok(name);
        }
    }

    Err(ConnectorError::Transport(format!(
        "invalid tls server name from '{}': {}",
        socket_addr,
        candidates.join(",")
    )))

}

fn connect_tls_stream(
    socket_addr: &str,
    tls: &ConnectorTlsConfig,
    ca_pem_override: Option<&str>,
) -> Result<ConnectorWireStream, ConnectorError> {

    let global_fingerprint = global_tls_fingerprint(socket_addr);

    let mut client_config = if let Some(pem) = ca_pem_override {
        let roots = load_tls_root_store_from_pem(pem)?;
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else if let Some(ca_path) = tls.ca_path.as_ref() {
        let roots = load_tls_root_store(ca_path)?;
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    } else if let Some(expected_fingerprint) = global_fingerprint {
        let provider = rustls::crypto::CryptoProvider::get_default()
            .cloned()
            .ok_or_else(|| ConnectorError::Transport("rustls crypto provider is not initialized".to_string()))?;
        let verifier = Arc::new(FingerprintServerCertVerifier::new(
            expected_fingerprint,
            provider.signature_verification_algorithms,
        ));
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    } else {
        let mut roots = load_system_tls_root_store().unwrap_or_else(|err| {
            log::warn!(
                "connector failed to load system trust store; falling back to platform roots: {}",
                err
            );
            RootCertStore::empty()
        });

        let platform_roots = load_tls_root_store_from_pem(platform_tls_root_cert_pem())?;
        for cert in platform_roots.roots {
            roots.roots.push(cert);
        }

        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    client_config.alpn_protocols = vec![b"distdb-p2p/1".to_vec()];

    let mut tcp = connect_tcp_with_timeout(socket_addr)?;
    let handshake_timeout_secs = connector_handshake_timeout_secs();
    let handshake_deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(handshake_timeout_secs);
    let io_timeout = std::time::Duration::from_millis(500);

    tcp.set_read_timeout(Some(io_timeout))
        .map_err(|e| ConnectorError::Transport(format!("failed to set read timeout: {e}")))?;
    
    tcp.set_write_timeout(Some(io_timeout))
        .map_err(|e| ConnectorError::Transport(format!("failed to set write timeout: {e}")))?;
    
    tcp.set_nodelay(true)
        .map_err(|e| ConnectorError::Transport(format!("failed to set TCP_NODELAY: {e}")))?;

    let server_name = server_name_from_socket_addr(socket_addr)?;
    log::debug!(
        "connector TLS handshake target socket_addr={} server_name={:?}",
        socket_addr,
        server_name
    );
    let mut connection = ClientConnection::new(Arc::new(client_config), server_name).map_err(|e| {
        ConnectorError::Transport(format!("failed to create TLS client connection: {e}"))
    })?;

    while connection.is_handshaking() {

        match connection.complete_io(&mut tcp) {
            
            Ok(_) => {},
            
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) =>
            {
                if std::time::Instant::now() >= handshake_deadline {
                    return Err(ConnectorError::Transport(format!(
                        "TLS handshake timed out after {}s: {err}",
                        handshake_timeout_secs,
                    )));
                }
                continue;
            },

            Err(err) => {
                return Err(ConnectorError::Transport(format!("TLS handshake failed: {err}")));
            }

        }

    }

    Ok(ConnectorWireStream::Tls(StreamOwned::new(connection, tcp)))

}

fn connect_connector_stream(
    socket_addr: &str,
    tls: &ConnectorTlsConfig,
    ca_pem_override: Option<&str>,
) -> Result<ConnectorWireStream, ConnectorError> {

    match tls.mode {
        common::TlsMode::Required => connect_tls_stream(socket_addr, tls, ca_pem_override),
        _ => Err(ConnectorError::Transport(
            "p2p network requires tls=required".to_string(),
        )),
    }

}
fn normalize_peer_addr(raw: &str) -> String {

    let trimmed = raw.trim();

    if let Some(rest) = trimmed.strip_prefix("/ip4/")
        && let Some((host, port)) = rest.split_once("/tcp/")
            && !host.is_empty() && port.parse::<u16>().is_ok() {
                return format!("{host}:{port}");
            }

    if let Some(rest) = trimmed.strip_prefix("/dns/")
        && let Some((host, port)) = rest.split_once("/tcp/")
            && !host.is_empty() && port.parse::<u16>().is_ok() {
                return format!("{host}:{port}");
            }

    if trimmed.contains(':') {
        trimmed.to_string()
    } else {    
        format!("{trimmed}:{DEFAULT_SERVER_PORT}")
    }

}

fn connect_tcp_with_timeout(socket_addr: &str) -> Result<TcpStream, ConnectorError> {

    let timeout = std::time::Duration::from_secs(connector_connect_timeout_secs());

    let addrs = socket_addr
        .to_socket_addrs()
        .map_err(|err| ConnectorError::Transport(format!("failed to resolve {socket_addr}: {err}")))?
        .collect::<Vec<_>>();

    if addrs.is_empty() {
        return Err(ConnectorError::Transport(format!(
            "failed to resolve {socket_addr}: no socket addresses",
        )));
    }

    let mut last_err: Option<std::io::Error> = None;

    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => {
                return Ok(stream);
            },
            Err(err) => {
                last_err = Some(err);
            }
        }
    }

    let err = last_err
        .map(|e| e.to_string())
        .unwrap_or_else(|| "unknown connect error".to_string());

    Err(ConnectorError::Transport(format!(
        "failed to connect to {socket_addr}: {err}",
    )))

}

fn extract_session_id(message: &str) -> Option<String> {

    for part in message.split_whitespace() {

        if let Some(value) = part.strip_prefix("session_id=") {
            let token = value.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }

        // Backward compatibility for servers that still emit the old label.
        if let Some(value) = part.strip_prefix("shared_authorization=") {
            let token = value.trim();
            if !token.is_empty() {
                return Some(token.to_string());
            }
        }
    
    }
    
    None

}

fn generate_shared_session_token(peer_id: &str, server_token: Option<&str>) -> String {

    let entropy = format!(
        "{}:{}:{}",
        peer_id,
        epoch_nanos!(),
        server_token.unwrap_or("server-token-unavailable")
    );
    
    md5(entropy.as_bytes())

}

#[cfg(test)]
#[path = "transport_test.rs"]
mod tests;
