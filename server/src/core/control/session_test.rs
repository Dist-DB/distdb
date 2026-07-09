
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
    
