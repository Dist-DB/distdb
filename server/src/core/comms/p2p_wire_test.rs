    use super::*;

    #[test]
    fn normalize_bootstrap_addr_accepts_multiaddr_passthrough() {
        let addr = "/ip4/127.0.0.1/tcp/9400";
        assert_eq!(normalize_bootstrap_addr(addr), Some(addr.to_string()));
    }

    #[test]
    fn normalize_bootstrap_addr_parses_host_port() {
        assert_eq!(
            normalize_bootstrap_addr("127.0.0.1:9400"),
            Some("/ip4/127.0.0.1/tcp/9400".to_string())
        );
        assert_eq!(
            normalize_bootstrap_addr("node.local:9400"),
            Some("/dns/node.local/tcp/9400".to_string())
        );
    }

    #[test]
    fn multiaddr_to_socket_addr_parses_ip4_and_dns() {
        assert_eq!(
            multiaddr_to_socket_addr("/ip4/127.0.0.1/tcp/4001"),
            Some("127.0.0.1:4001".to_string())
        );
        assert_eq!(
            multiaddr_to_socket_addr("/dns/node.local/tcp/4002"),
            Some("node.local:4002".to_string())
        );
        assert_eq!(multiaddr_to_socket_addr("127.0.0.1:4001"), None);
    }

    #[test]
    fn node_announce_wire_encoding_roundtrips() {
        let message = ServiceMessage::NodeAnnounce(peerlib::PeerNode {
            id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        });

        let encoded = encode_service_message(&message).expect("message should encode");
        let decoded = decode_service_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn schema_catalog_wire_encoding_roundtrips() {
        let message = ServiceMessage::SchemaCatalogRequest(
            peerlib::SchemaCatalogRequest {
                request_id: "req-1".to_string(),
                affinity_id: "aff-1".to_string(),
                database_id: "main".to_string(),
                expected_schema_identifier: 1,
                expected_schema_hash: Some("hash".to_string()),
            },
        );

        let encoded = encode_service_message(&message).expect("message should encode");
        let decoded = decode_service_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }

    #[test]
    fn decode_service_message_rejects_missing_magic_prefix() {
        let payload = vec![1u8, 2u8, 3u8, 4u8];
        assert!(decode_service_message(&payload).is_none());
    }

    #[test]
    fn decode_service_message_rejects_truncated_bincode_payload() {
        let message = ServiceMessage::NodeAnnounce(peerlib::PeerNode {
            id: "sam01".to_string(),
            addrs: vec!["/ip4/127.0.0.1/tcp/4001".to_string()],
            is_local: false,
        });

        let mut encoded = encode_service_message(&message).expect("message should encode");
        encoded.truncate(SERVICE_MESSAGE_MAGIC.len() + 1);

        assert!(decode_service_message(&encoded).is_none());
    }

    #[test]
    fn normalize_bootstrap_addr_parses_bare_port() {
        assert_eq!(
            normalize_bootstrap_addr("4001"),
            Some("/ip4/127.0.0.1/tcp/4001".to_string())
        );
        assert_eq!(
            normalize_bootstrap_addr(":4002"),
            Some("/ip4/127.0.0.1/tcp/4002".to_string())
        );
    }

    #[test]
    fn advertised_listen_addr_defaults_wildcard_to_localhost() {
        let args = vec!["server".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "127.0.0.1".to_string()
        );
        assert_eq!(
            advertised_listen_addr_from_args(&args, "192.168.1.10"),
            "192.168.1.10".to_string()
        );
    }

    #[test]
    fn advertised_listen_addr_prefers_explicit_override() {
        let args = vec!["server".to_string(), "advertise_addr=10.1.1.5".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "10.1.1.5".to_string()
        );
    }

    #[test]
    fn bootstrap_nodes_use_normalized_addrs() {
        let nodes = bootstrap_nodes_from_server_list(&[
            "/ip4/127.0.0.1/tcp/9400".to_string(),
            "/dns/node.local/tcp/9400".to_string(),
        ]);

        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].addrs, vec!["/ip4/127.0.0.1/tcp/9400".to_string()]);
        assert_eq!(nodes[1].addrs, vec!["/dns/node.local/tcp/9400".to_string()]);
        assert!(nodes.iter().all(|node| !node.id.is_empty()));
    }
