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
    fn advertised_listen_addr_defaults_wildcard_to_localhost_when_no_hint_available() {
        let args = vec!["server".to_string()];
        assert_eq!(
            resolve_advertise_host(&args, "0.0.0.0", None),
            "127.0.0.1".to_string()
        );
        assert_eq!(
            resolve_advertise_host(&args, "192.168.1.10", None),
            "192.168.1.10".to_string()
        );
    }

    #[test]
    fn advertised_listen_addr_uses_positional_host_when_present() {
        let args = vec!["/tmp/distdb-server".to_string(), "provision.distdb.com".to_string()];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "provision.distdb.com".to_string()
        );
    }

    #[test]
    fn advertised_listen_addr_uses_public_host_from_server_list_when_available() {
        let args = vec![
            "server".to_string(),
            "servers=provision.distdb.com:4001".to_string(),
        ];
        assert_eq!(
            advertised_listen_addr_from_args(&args, "0.0.0.0"),
            "provision.distdb.com".to_string()
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
    fn advertised_listen_addr_ignores_placeholder_hosts() {
        let args = vec!["server".to_string(), "--".to_string()];
        assert_eq!(
            resolve_advertise_host(&args, "0.0.0.0", None),
            "127.0.0.1".to_string()
        );

        let args = vec!["server".to_string(), "advertise_addr=--".to_string()];
        assert_eq!(
            resolve_advertise_host(&args, "0.0.0.0", None),
            "127.0.0.1".to_string()
        );
    }

    #[test]
    fn advertised_listen_addr_uses_hostname_hint_when_listen_addr_is_wildcard() {
        let args = vec!["server".to_string()];
        assert_eq!(
            resolve_advertise_host(&args, "0.0.0.0", Some("provision.distdb.com")),
            "provision.distdb.com"
        );
    }

    #[test]
    fn advertised_listen_addr_uses_public_hostname_env_when_available() {
        unsafe {
            std::env::set_var("HOSTNAME", "provision.distdb.com");
        }

        let args = vec!["server".to_string()];
        let resolved = advertised_listen_addr_from_args(&args, "0.0.0.0");

        unsafe {
            std::env::remove_var("HOSTNAME");
        }

        assert_eq!(resolved, "provision.distdb.com");
    }

    #[test]
    fn advertised_listen_addr_prefers_hostname_hint_over_server_list() {
        let args = vec![
            "server".to_string(),
            "servers=distdb1-fra.samcolak.com:4001".to_string(),
        ];

        assert_eq!(
            resolve_advertise_host(&args, "0.0.0.0", Some("provision.distdb.com")),
            "provision.distdb.com"
        );
    }

    #[test]
    fn advertised_listen_addr_prefers_positional_host_over_hostname_hint() {
        let args = vec![
            "server".to_string(),
            "distdb1-fra.samcolak.com".to_string(),
        ];

        assert_eq!(
            resolve_advertise_host(&args, "0.0.0.0", Some("provision.distdb.com")),
            "distdb1-fra.samcolak.com"
        );
    }

    #[test]
    fn prefer_public_hostname_in_addrs_rewrites_local_node_multiaddrs() {
        let rewritten = prefer_public_hostname_in_addrs(
            &["/dns/distdb1-fra.samcolak.com/tcp/4001".to_string()],
            Some("provision.distdb.com"),
        );

        assert_eq!(rewritten, vec!["/dns/provision.distdb.com/tcp/4001".to_string()]);
    }

    #[test]
    fn extract_public_hostname_from_hosts_content_finds_public_alias() {
        let content = "127.0.0.1 localhost\n203.0.113.10 provision.distdb.com\n";
        assert_eq!(
            extract_public_hostname_from_hosts_content(content),
            Some("provision.distdb.com".to_string())
        );
    }

    #[test]
    fn resolve_public_hostname_from_hosts_paths_prefers_public_alias() {
        let temp_hosts = std::env::temp_dir().join(format!("distdb-hosts-{}.tmp", std::process::id()));
        std::fs::write(&temp_hosts, "127.0.0.1 localhost\n203.0.113.10 provision.distdb.com\n")
            .expect("hosts fixture should write");

        let resolved = resolve_public_hostname_from_hosts_paths(&[temp_hosts.to_str().expect("temp path should be valid")]);

        let _ = std::fs::remove_file(&temp_hosts);

        assert_eq!(resolved, Some("provision.distdb.com".to_string()));
    }

    #[test]
    fn normalize_advertise_addr_falls_back_to_loopback_for_placeholder_hosts() {
        assert_eq!(
            normalize_advertise_addr("--", 4001),
            "/ip4/127.0.0.1/tcp/4001".to_string()
        );
        assert_eq!(
            normalize_advertise_addr("*", 4002),
            "/ip4/127.0.0.1/tcp/4002".to_string()
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
