
    use super::*;

    #[test]
    fn default_local_listens_on_all_interfaces() {
        let config = ServerRuntimeConfig::default_local();

        assert_eq!(config.node_id, DEFAULT_LOCAL_NODE_ID);
        assert_eq!(
            config.listen_addrs,
            vec![format!("/ip4/0.0.0.0/tcp/{DEFAULT_SERVER_PORT}")]
        );
        assert_eq!(config.tls_mode, common::TlsMode::Off);
        assert_eq!(config.tls, ServerTlsConfig::default());
    }
    
