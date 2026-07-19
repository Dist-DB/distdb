
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn default_tls_mode_is_required() {
        let args: Vec<String> = vec![];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Required, "TLS must default to required");
    }

    #[test]
    fn explicit_tls_off_overrides_default() {
        let args = vec!["tls=off".to_string()];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Off);
    }

    #[test]
    fn explicit_tls_required_is_accepted() {
        let args = vec!["tls=required".to_string()];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Required);
    }

    #[test]
    fn invalid_tls_mode_is_rejected() {
        let args = vec!["tls=unsafe".to_string()];
        assert!(parse_tls_mode_from_args(&args).is_err());
    }

    #[tokio::test]
    async fn required_tls_without_acceptor_fails_and_does_not_fallback() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener should bind");
        let addr = listener.local_addr().expect("listener addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept should work");
            negotiate_connector_stream(
                stream,
                &addr.to_string(),
                common::TlsMode::Required,
                None,
            )
            .await
        });

        let _client = tokio::net::TcpStream::connect(addr)
            .await
            .expect("client should connect");

        let result = server.await.expect("server task should complete");
        assert!(result.is_err(), "required TLS must fail without acceptor");
    }

