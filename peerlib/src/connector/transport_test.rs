    
    use super::*;
    use connector::{
        ConnectorCommand, ConnectorRequest, ConnectorResult, MutationResult,
        ResponseStatus,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    fn write_frame(stream: &mut std::net::TcpStream, response: &ConnectorResponse) {
        let payload = bincode::serialize(response).expect("response should serialize");
        let len = payload.len() as u32;
        stream
            .write_all(&len.to_le_bytes())
            .and_then(|_| stream.write_all(&payload))
            .expect("frame should write");
        stream.flush().expect("frame should flush");
    }

    fn write_raw_frame(stream: &mut std::net::TcpStream, payload: &[u8]) {
        let len = payload.len() as u32;
        stream
            .write_all(&len.to_le_bytes())
            .and_then(|_| stream.write_all(payload))
            .expect("raw frame should write");
        stream.flush().expect("raw frame should flush");
    }

    fn read_request(stream: &mut std::net::TcpStream) -> ConnectorRequest {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .expect("request length should read");
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream
            .read_exact(&mut payload)
            .expect("request payload should read");
        bincode::deserialize::<ConnectorRequest>(&payload).expect("request should decode")
    }

    fn read_len_prefixed_payload(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .expect("payload length should read");
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream
            .read_exact(&mut payload)
            .expect("payload bytes should read");
        payload
    }

    #[test]
    fn transport_uses_kademlia_mode() {
        let transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));
        assert_eq!(transport.discovery_mode(), ConnectorDiscoveryMode::Kademlia);
    }

    #[test]
    fn request_fails_when_no_peers_are_available() {
        let transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));
        let req = ConnectorRequest::new(
            "req-1",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let result = transport.request(&req);
        assert!(matches!(result, Err(ConnectorError::Transport(_))));
    }

    #[test]
    fn queued_response_is_returned_for_matching_request() {
        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0")
                .with_bootstrap_peers(vec!["bootstrap-peer-1".to_string()]),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
            is_discovered: true,
        });

        transport.queue_response(ConnectorResponse {
            request_id: "req-9".to_string(),
            status: ResponseStatus::Applied,
            result: ConnectorResult::Mutation(MutationResult { affected_rows: 2 }),
        });

        let req = ConnectorRequest::new(
            "req-9",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let response = transport.request(&req).expect("response should be routed");
        assert_eq!(response.request_id, "req-9");
        assert_eq!(response.status, ResponseStatus::Applied);
    }

    #[test]
    fn first_discovered_peer_becomes_active_session_peer() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
            is_discovered: true,
        });

        assert_eq!(transport.active_peer_id(), Some("peer-1"));
    }

    #[test]
    fn select_peer_switches_active_session_peer() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["/ip4/10.0.0.1/tcp/4001".to_string()],
            is_discovered: true,
        });
        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-2".to_string(),
            addrs: vec!["/ip4/10.0.0.2/tcp/4001".to_string()],
            is_discovered: true,
        });

        transport
            .select_peer("peer-2")
            .expect("peer switch should succeed");

        assert_eq!(transport.active_peer_id(), Some("peer-2"));
    }

    #[test]
    fn upsert_peer_replaces_stale_identity_when_addr_matches() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_discovered: true,
        });

        transport.upsert_peer(ConnectorPeer {
            peer_id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_discovered: true,
        });

        let peers = transport.discovered_peers();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].peer_id, "sam01");
        assert_eq!(transport.active_peer_id(), Some("sam01"));
    }

    #[test]
    fn normalize_peer_addr_parses_supported_multiaddrs() {
        assert_eq!(
            normalize_peer_addr("/ip4/127.0.0.1/tcp/4001"),
            "127.0.0.1:4001"
        );
        assert_eq!(
            normalize_peer_addr("/dns/server-node-01/tcp/9400"),
            "server-node-01:9400"
        );
    }

    #[test]
    fn normalize_peer_addr_keeps_host_port_and_defaults_port() {
        assert_eq!(normalize_peer_addr("127.0.0.1:4001"), "127.0.0.1:4001");
        assert_eq!(
            normalize_peer_addr("localhost"),
            format!("localhost:{}", DEFAULT_SERVER_PORT)
        );
    }

    #[test]
    fn connector_timeout_env_values_are_clamped_and_defaulted() {
        unsafe {
            std::env::set_var(CONNECTOR_CONNECT_TIMEOUT_SECS_ENV, "99");
            std::env::set_var(CONNECTOR_HANDSHAKE_TIMEOUT_SECS_ENV, "0");
        }

        assert_eq!(connector_connect_timeout_secs(), 30);
        assert_eq!(connector_handshake_timeout_secs(), 1);

        unsafe {
            std::env::remove_var(CONNECTOR_CONNECT_TIMEOUT_SECS_ENV);
            std::env::remove_var(CONNECTOR_HANDSHAKE_TIMEOUT_SECS_ENV);
        }

        assert_eq!(connector_connect_timeout_secs(), 1);
        assert_eq!(connector_handshake_timeout_secs(), 1);
    }

    #[test]
    fn server_name_parser_supports_ip_and_hostname_and_rejects_empty() {
        let ip = server_name_from_socket_addr("127.0.0.1:9400").expect("ip should parse");
        let host = server_name_from_socket_addr("node-1.local:9400").expect("host should parse");

        assert!(matches!(ip, ServerName::IpAddress(_)));
        assert!(matches!(host, ServerName::DnsName(_)));

        let err = server_name_from_socket_addr(":9400").expect_err("empty host should fail");
        assert!(matches!(err, ConnectorError::Transport(_)));
    }

    #[test]
    fn extract_session_id_supports_both_labels() {
        assert_eq!(
            extract_session_id("challenge session_id=sess-123"),
            Some("sess-123".to_string())
        );
        assert_eq!(
            extract_session_id("challenge shared_authorization=legacy-token"),
            Some("legacy-token".to_string())
        );
        assert_eq!(extract_session_id("challenge without token"), None);
    }

    #[test]
    fn shared_session_token_changes_with_server_token() {
        let a = generate_shared_session_token("peer-a", Some("token-a"));
        let b = generate_shared_session_token("peer-a", Some("token-b"));
        assert_ne!(a, b);
        assert!(!a.is_empty());
    }

    #[test]
    fn request_returns_selected_peer_error_when_no_active_peer() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));
        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec!["127.0.0.1:1".to_string()],
            is_discovered: true,
        });
        transport.active_peer_id = None;

        let req = ConnectorRequest::new(
            "req-no-active",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let err = transport.request(&req).expect_err("request should fail");
        assert!(matches!(err, ConnectorError::Transport(_)));
    }

    #[test]
    fn connect_active_peer_and_request_roundtrip_over_plain_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept");

            write_frame(
                &mut stream,
                &ConnectorResponse::rejected(
                    SERVER_PASSWORD_CHALLENGE_REQUEST_ID,
                    "auth required session_id=server-seed",
                ),
            );

            let req = read_request(&mut stream);
            write_frame(
                &mut stream,
                &ConnectorResponse::applied(
                    req.request_id,
                    ConnectorResult::Mutation(MutationResult { affected_rows: 7 }),
                ),
            );
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Off),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        transport
            .connect_active_peer()
            .expect("active peer should connect");
        assert!(transport.has_live_connection());
        assert!(transport
            .session_id()
            .expect("session id should be readable")
            .is_some());

        let req = ConnectorRequest::new(
            "req-live-1",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let response = transport.request(&req).expect("request should succeed");
        assert_eq!(response.status, ResponseStatus::Applied);

        server.join().expect("server thread should finish");
    }

    #[test]
    fn connect_active_peer_surfaces_bootstrap_rejection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept");
            write_frame(
                &mut stream,
                &ConnectorResponse::rejected(
                    SERVER_BOOTSTRAP_REJECT_REQUEST_ID,
                    "bootstrap rejected",
                ),
            );
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Off),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        let err = transport
            .connect_active_peer()
            .expect_err("bootstrap rejection should fail connect");
        assert!(matches!(err, ConnectorError::Rejected(_)));

        server.join().expect("server thread should finish");
    }

    #[test]
    fn connect_active_peer_rejects_malformed_challenge_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept");
            write_raw_frame(&mut stream, b"not-bincode");
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Off),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        let err = transport
            .connect_active_peer()
            .expect_err("malformed challenge should fail connect");

        match err {
            ConnectorError::Transport(message) => {
                assert!(
                    message.contains("failed to decode response payload"),
                    "expected decode error, got: {}",
                    message
                );
            }
            other => panic!("expected transport decode error, got: {:?}", other),
        }

        server.join().expect("server thread should finish");
    }

    #[test]
    fn request_drops_live_connection_after_malformed_response_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept");

            write_frame(
                &mut stream,
                &ConnectorResponse::rejected(
                    SERVER_PASSWORD_CHALLENGE_REQUEST_ID,
                    "auth required session_id=server-seed",
                ),
            );

            let _req = read_request(&mut stream);
            write_raw_frame(&mut stream, b"malformed-response");
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Off),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        transport
            .connect_active_peer()
            .expect("active peer should connect");
        assert!(transport.has_live_connection());

        let req = ConnectorRequest::new(
            "req-malformed",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let err = transport
            .request(&req)
            .expect_err("malformed response should fail request");

        match err {
            ConnectorError::Transport(message) => {
                assert!(
                    message.contains("failed to decode response payload"),
                    "expected decode error, got: {}",
                    message
                );
            }
            other => panic!("expected transport decode error, got: {:?}", other),
        }

        assert!(
            !transport.has_live_connection(),
            "live connection should be dropped after malformed response"
        );

        server.join().expect("server thread should finish");
    }

    #[test]
    fn fetch_ca_pem_from_peer_returns_none_on_malformed_direct_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept");

            // Consume request frame, then return malformed CA bootstrap response bytes.
            let _request = read_len_prefixed_payload(&mut stream);
            write_raw_frame(&mut stream, b"malformed-ca-bootstrap-response");
        });

        let result = fetch_ca_pem_from_peer(&addr.to_string(), "peer-1")
            .expect("malformed CA response should not hard-fail transport");

        assert!(
            result.is_none(),
            "malformed CA response should be treated as absent CA"
        );

        server.join().expect("server thread should finish");
    }

    #[test]
    fn fetch_ca_pem_from_peer_returns_none_when_challenge_precedes_malformed_ca_response() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("server should accept");

            let _request = read_len_prefixed_payload(&mut stream);

            write_frame(
                &mut stream,
                &ConnectorResponse::rejected(
                    SERVER_PASSWORD_CHALLENGE_REQUEST_ID,
                    "auth required session_id=server-seed",
                ),
            );

            write_raw_frame(&mut stream, b"malformed-post-challenge-ca-response");
        });

        let result = fetch_ca_pem_from_peer(&addr.to_string(), "peer-1")
            .expect("malformed post-challenge CA response should not hard-fail transport");

        assert!(
            result.is_none(),
            "malformed post-challenge CA response should be treated as absent CA"
        );

        server.join().expect("server thread should finish");
    }
    