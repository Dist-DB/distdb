    
    use super::{b64_decode, b64_encode_bytes};

    #[test]
    fn encode_matches_standard_no_pad_vectors() {
        assert_eq!(b64_encode_bytes(b""), "");
        assert_eq!(b64_encode_bytes(b"f"), "Zg");
        assert_eq!(b64_encode_bytes(b"fo"), "Zm8");
        assert_eq!(b64_encode_bytes(b"foo"), "Zm9v");
        assert_eq!(b64_encode_bytes(b"hello world"), "aGVsbG8gd29ybGQ");
    }

    #[test]
    fn decode_accepts_standard_and_padded_input() {
        assert_eq!(b64_decode("aGVsbG8gd29ybGQ"), b"hello world");
        assert_eq!(b64_decode("aGVsbG8gd29ybGQ="), b"hello world");
    }

    #[test]
    fn decode_accepts_url_safe_variants() {
        assert_eq!(b64_decode("-w"), vec![0xfb]);
        assert_eq!(b64_decode("_w"), vec![0xff]);
        assert_eq!(b64_decode("AAE_"), vec![0x00, 0x01, 0x3f]);
    }

    #[test]
    fn decode_accepts_mixed_alphabet_fallback() {
        assert_eq!(b64_decode("+_8"), vec![0xfb, 0xff]);
    }

    #[test]
    fn decode_invalid_returns_empty() {
        assert!(b64_decode("abc$").is_empty());
        assert!(b64_decode("A").is_empty());
    }