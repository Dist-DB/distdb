    
    use super::*;

    #[test]
    fn request_serializes_roundtrip() {
        let req = ConnectorRequest::new(
            "req-1",
            ConnectorCommand::CreateDatabase {
                database_name: "main".to_string(),
            },
        );

        let bytes = common::helpers::bincode_compat::serialize(&req).expect("request should serialize");
        let decoded: ConnectorRequest =
            common::helpers::bincode_compat::deserialize(&bytes).expect("request should deserialize");

        assert_eq!(decoded, req);
    }
