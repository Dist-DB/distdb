use super::*;
use peerlib::ConnectorTlsConfig;

#[test]
fn new_requires_at_least_one_server() {
    let result = ConsoleSession::new(Vec::new(), ConnectorTlsConfig::default());
    assert!(result.is_err());

    let error_text = result
        .err()
        .expect("empty server list should be rejected")
        .to_string();
    assert!(error_text.contains("at least one server address is required"));
}

#[test]
fn next_request_id_increments_monotonically() {
    let mut session = ConsoleSession::new(
        vec!["127.0.0.1:4001".to_string()],
        ConnectorTlsConfig::default(),
    )
    .expect("session should initialize with one bootstrap peer");

    assert_eq!(session.next_request_id(), "console-req-1");
    assert_eq!(session.next_request_id(), "console-req-2");
    assert_eq!(session.next_request_id(), "console-req-3");
}

#[test]
fn resolve_startup_peer_id_prefers_single_bootstrap_peer_when_unknown() {
    let session = ConsoleSession::new(
        vec!["127.0.0.1:4001".to_string()],
        ConnectorTlsConfig::default(),
    )
    .expect("session should initialize with one bootstrap peer");

    let resolved = session
        .resolve_startup_peer_id("missing-peer")
        .expect("single bootstrap peer should be selected");

    assert_eq!(resolved, "/ip4/127.0.0.1/tcp/4001");
}
