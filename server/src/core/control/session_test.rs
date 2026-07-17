
    use super::{
        apply_bootstrap_password_wal_payload,
        encode_set_password_wal_payload,
        extract_auth_token,
        extract_set_password_directive,
        set_bootstrap_password,
    };

    #[test]
    fn extract_auth_token_accepts_password_token_command() {
        assert_eq!(extract_auth_token("password_token abc123;"), Some("abc123"));
    }

    #[test]
    fn extract_auth_token_accepts_password_command_alias() {
        assert_eq!(extract_auth_token("password abc123;"), Some("abc123"));
    }

    #[test]
    fn extract_auth_token_rejects_extra_tokens() {
        assert_eq!(extract_auth_token("password_token abc123 extra"), None);
    }

    #[test]
    fn extract_auth_token_rejects_set_password_for_syntax() {
        assert_eq!(
            extract_auth_token("SET PASSWORD FOR 'root' = PASSWORD('secret');"),
            None
        );
    }

    #[test]
    fn extract_auth_token_rejects_set_password_without_quotes() {
        assert_eq!(
            extract_auth_token("SET PASSWORD FOR root = PASSWORD(secret);"),
            None
        );
    }

    #[test]
    fn extract_set_password_directive_accepts_quoted_syntax() {
        assert_eq!(
            extract_set_password_directive("SET PASSWORD FOR 'root' = PASSWORD('secret');"),
            Some(super::SetPasswordDirective {
                user_id: "root",
                password: "secret",
            })
        );
    }

    #[test]
    fn set_bootstrap_password_rejects_non_root_user() {
        assert!(set_bootstrap_password("alice", "secret").is_err());
    }

    #[test]
    fn set_password_wal_payload_round_trip_applies() {
        let payload = encode_set_password_wal_payload("root", "sam")
            .expect("payload should encode");

        assert!(apply_bootstrap_password_wal_payload(&payload).is_ok());
    }

    #[test]
    fn apply_bootstrap_password_wal_payload_rejects_non_utf8_payload() {
        let payload = vec![0xff, 0xfe, 0xfd, 0x00];

        let err = apply_bootstrap_password_wal_payload(&payload)
            .expect_err("non-utf8 payload should be rejected");
        assert!(
            err.contains("cannot decode wal payload"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn apply_bootstrap_password_wal_payload_rejects_malformed_segments() {
        let payload = b"root\nnonce-only".to_vec();

        let err = apply_bootstrap_password_wal_payload(&payload)
            .expect_err("malformed segment count should be rejected");
        assert!(
            err.contains("wal payload is malformed"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn apply_bootstrap_password_wal_payload_rejects_invalid_encrypted_password_state() {
        let payload = b"root\nnonce\nnot-valid-ciphertext".to_vec();

        assert!(
            apply_bootstrap_password_wal_payload(&payload).is_err(),
            "invalid encrypted payload state should be rejected"
        );
    }
    
