    use super::*;

    struct StringCodec;

    impl TransferCodec<String> for StringCodec {
        fn encode(&self, message: &String) -> std::result::Result<TransferEnvelope, String> {
            Ok(TransferEnvelope::new(
                "sync",
                "string",
                Some("req-1".to_string()),
                message.as_bytes().to_vec(),
            ))
        }

        fn decode(&self, envelope: &TransferEnvelope) -> std::result::Result<String, String> {
            String::from_utf8(envelope.payload.clone()).map_err(|err| err.to_string())
        }
    }

    #[test]
    fn transfer_envelope_builder_sets_fields_and_headers() {
        let envelope = TransferEnvelope::new(
            "events",
            "snapshot",
            Some("req-9".to_string()),
            b"payload".to_vec(),
        )
        .with_header("x-version", "1")
        .with_header("x-source", "node-1");

        assert_eq!(envelope.channel, "events");
        assert_eq!(envelope.message_type, "snapshot");
        assert_eq!(envelope.request_id.as_deref(), Some("req-9"));
        assert_eq!(envelope.payload, b"payload".to_vec());
        assert_eq!(envelope.headers.get("x-version"), Some(&"1".to_string()));
        assert_eq!(envelope.headers.get("x-source"), Some(&"node-1".to_string()));
    }

    #[test]
    fn transfer_codec_roundtrip_works() {
        let codec = StringCodec;
        let source = "hello peer".to_string();

        let envelope = codec.encode(&source).expect("encode should succeed");
        let decoded = codec.decode(&envelope).expect("decode should succeed");

        assert_eq!(decoded, source);
    }