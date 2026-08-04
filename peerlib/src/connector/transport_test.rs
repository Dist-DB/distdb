    
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

    fn assert_tls_connect_failure(err: ConnectorError) {
        match err {
            ConnectorError::Transport(message) => {
                assert!(
                    message.contains("TLS handshake failed")
                        || message.contains("failed to create TLS client connection"),
                    "unexpected transport error message: {message}"
                );
            }
            other => panic!("expected transport error, got: {:?}", other),
        }
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
    fn upsert_peer_prefers_non_loopback_addresses_when_available() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec![
                "127.0.0.1:4001".to_string(),
                "public.example.com:4001".to_string(),
            ],
            is_discovered: true,
        });

        let peer = transport.active_peer().expect("peer should be active");
        assert_eq!(peer.addrs.first().unwrap(), "public.example.com:4001");
        assert_eq!(peer.addrs.get(1).unwrap(), "127.0.0.1:4001");
    }

    #[test]
    fn upsert_peer_preserves_previously_discovered_non_loopback_addresses() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec!["public.example:4001".to_string()],
            is_discovered: true,
        });

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec!["127.0.0.1:4001".to_string()],
            is_discovered: true,
        });

        let peer = transport.active_peer().expect("peer should be active");
        assert_eq!(peer.addrs.len(), 2);
        assert_eq!(peer.addrs.first().unwrap(), "public.example:4001");
        assert_eq!(peer.addrs.get(1).unwrap(), "127.0.0.1:4001");
    }

    #[test]
    fn upsert_peer_merges_loopback_refresh_into_existing_public_peer() {
        let mut transport = ConnectorP2pTransport::new(ConnectorP2pConfig::new("/distdb/kad/1.0.0"));

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-01".to_string(),
            addrs: vec!["public.example.com:4001".to_string()],
            is_discovered: true,
        });

        transport.upsert_peer(ConnectorPeer {
            peer_id: "server-node-02".to_string(),
            addrs: vec!["127.0.0.1:4001".to_string()],
            is_discovered: true,
        });

        let peers = transport.known_peers();
        assert_eq!(peers.len(), 1);
        let peer = peers.first().unwrap();
        assert_eq!(peer.peer_id, "server-node-01");
        assert_eq!(peer.addrs.len(), 2);
        assert_eq!(peer.addrs.first().unwrap(), "public.example.com:4001");
        assert_eq!(peer.addrs.get(1).unwrap(), "127.0.0.1:4001");
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
    fn server_name_candidates_cover_loopback_aliases() {
        let candidates = server_names_from_socket_addr("127.0.0.1:4001");
        let names = candidates
            .iter()
            .map(|name| name.to_string())
            .collect::<Vec<_>>();

        assert!(names.iter().any(|name| name == "127.0.0.1"));
        assert!(names.iter().any(|name| name == "localhost"));
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
            std::env::set_var(CONNECTOR_STREAM_TIMEOUT_SECS_ENV, "2");
            std::env::set_var(CONNECTOR_CONNECT_TIMEOUT_SECS_ENV, "99");
            std::env::set_var(CONNECTOR_HANDSHAKE_TIMEOUT_SECS_ENV, "0");
        }

        assert_eq!(connector_stream_timeout_secs(), 5);
        assert_eq!(connector_connect_timeout_secs(), 30);
        assert_eq!(connector_handshake_timeout_secs(), 1);

        unsafe {
            std::env::remove_var(CONNECTOR_STREAM_TIMEOUT_SECS_ENV);
            std::env::remove_var(CONNECTOR_CONNECT_TIMEOUT_SECS_ENV);
            std::env::remove_var(CONNECTOR_HANDSHAKE_TIMEOUT_SECS_ENV);
        }

        assert_eq!(connector_stream_timeout_secs(), 300);
        assert_eq!(connector_connect_timeout_secs(), 1);
        assert_eq!(connector_handshake_timeout_secs(), 1);
    }

    #[test]
    fn normalize_tls_fingerprint_accepts_hex_with_or_without_delimiters() {
        let raw = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99";
        let normalized = normalize_tls_fingerprint(raw).expect("fingerprint should normalize");
        assert_eq!(normalized, "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899");
        assert!(normalize_tls_fingerprint("invalid").is_none());
    }

    #[test]
    fn global_tls_fingerprint_reads_from_baked_constant() {
        let loaded = global_tls_fingerprint().expect("fingerprint should load from baked constant");
        assert_eq!(loaded, "7289c9ec291a7f0cff0542c982d8497b77bf314882316649b28634da10449c30");
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
    fn server_name_parser_prefers_hostname_over_localhost_for_public_dns_names() {
        let host = server_name_from_socket_addr("public.example.com:4001")
            .expect("host should parse");

        assert!(matches!(host, ServerName::DnsName(_)));
        assert!(format!("{host:?}").contains("public.example.com"));
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
            let _stream = listener.accept().expect("server should accept");
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Required),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        let err = transport
            .connect_active_peer()
            .expect_err("plaintext peer should not accept a TLS-only connection");
        assert_tls_connect_failure(err);

        server.join().expect("server thread should finish");
    }

    #[test]
    fn connect_active_peer_surfaces_bootstrap_rejection() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let _stream = listener.accept().expect("server should accept");
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Required),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        let err = transport
            .connect_active_peer()
            .expect_err("plaintext peer should not accept a TLS-only connection");
        assert_tls_connect_failure(err);

        server.join().expect("server thread should finish");
    }

    #[test]
    fn connect_active_peer_rejects_malformed_challenge_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let _stream = listener.accept().expect("server should accept");
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Required),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        let err = transport
            .connect_active_peer()
            .expect_err("plaintext peer should not accept a TLS-only connection");
        assert_tls_connect_failure(err);

        server.join().expect("server thread should finish");
    }

    #[test]
    fn request_drops_live_connection_after_malformed_response_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let addr = listener.local_addr().expect("local addr should exist");

        let server = thread::spawn(move || {
            let _stream = listener.accept().expect("server should accept");
        });

        let mut transport = ConnectorP2pTransport::new(
            ConnectorP2pConfig::new("/distdb/kad/1.0.0").with_tls_mode(common::TlsMode::Required),
        );

        transport.upsert_peer(ConnectorPeer {
            peer_id: "peer-1".to_string(),
            addrs: vec![addr.to_string()],
            is_discovered: true,
        });

        let err = transport
            .connect_active_peer()
            .expect_err("plaintext peer should not accept a TLS-only connection");
        assert_tls_connect_failure(err);

        server.join().expect("server thread should finish");
    }
