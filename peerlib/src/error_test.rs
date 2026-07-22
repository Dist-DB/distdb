    use super::*;

    #[test]
    fn peer_error_display_formats_all_variants() {
        assert_eq!(
            PeerError::InvalidConfig("cfg".to_string()).to_string(),
            "invalid config: cfg"
        );
        assert_eq!(
            PeerError::InvalidState("state".to_string()).to_string(),
            "invalid state: state"
        );
        assert_eq!(
            PeerError::Network("net".to_string()).to_string(),
            "network error: net"
        );
        assert_eq!(
            PeerError::Codec("codec".to_string()).to_string(),
            "codec error: codec"
        );
        assert_eq!(
            PeerError::Storage("store".to_string()).to_string(),
            "storage error: store"
        );
    }

    #[expect(clippy::unnecessary_literal_unwrap, reason="we want to test the error display formatting")]
    #[test]
    fn result_alias_supports_ok_and_err() {
        let ok_value: Result<u64> = Ok(42);
        let err_value: Result<u64> = Err(PeerError::InvalidState("bad".to_string()));

        assert_eq!(ok_value.expect("ok should unwrap"), 42);
        assert!(matches!(err_value, Err(PeerError::InvalidState(_))));
    }