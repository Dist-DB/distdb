
    use super::*;

    #[test]
    fn default_tls_mode_is_optional() {
        let args: Vec<String> = vec![];
        let mode = parse_tls_mode_from_args(&args).expect("should parse");
        assert_eq!(mode, common::TlsMode::Optional, "TLS must default to optional for secure-by-default behaviour");
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

