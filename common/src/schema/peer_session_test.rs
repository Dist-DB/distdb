
    use super::*;

    #[test]
    fn default_service_type_is_client() {
        assert_eq!(PeerSession::new().service_type, PeerServiceType::Client);
    }

    #[test]
    fn with_service_type_sets_data_node() {
        let session = PeerSession::new().with_service_type(PeerServiceType::DataNode);
        assert_eq!(session.service_type, PeerServiceType::DataNode);
    }

    #[test]
    fn service_type_is_copy() {
        let t = PeerServiceType::Client;
        let copy = t;
        assert_eq!(t, copy);
    }

    #[test]
    fn clear_connection_state_resets_connection_fields() {
        let mut session = PeerSession::new()
            .with_service_type(PeerServiceType::DataNode)
            .with_database("main")
            .with_auth_token("token")
            .with_session_id("sid-1")
            .with_user_id("root");

        session.clear_connection_state();

        assert_eq!(session.service_type, PeerServiceType::DataNode);
        assert_eq!(session.current_database, None);
        assert_eq!(session.auth_token, None);
        assert_eq!(session.session_id, None);
        assert_eq!(session.user_id, None);
    }
