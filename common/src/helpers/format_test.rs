
    use super::*;

    #[test]
    fn make_header_round_trips_through_verify() {
        let header = make_header(FileKind::Data);
        let version = verify_header(FileKind::Data, &header).expect("header should be valid");
        assert_eq!(version, FORMAT_VERSION);
    }

    #[test]
    fn verify_rejects_bad_magic() {
        let mut header = make_header(FileKind::Data);
        header[0] = b'X';
        assert_eq!(verify_header(FileKind::Data, &header), Err(HeaderError::BadMagic));
    }

    #[test]
    fn verify_rejects_wrong_version() {
        let mut header = make_header(FileKind::Data);
        header[4] = 99;
        assert_eq!(verify_header(FileKind::Data, &header), Err(HeaderError::UnsupportedVersion(99)));
    }

    #[test]
    fn verify_rejects_too_short() {
        assert_eq!(verify_header(FileKind::Data, &[0u8; 3]), Err(HeaderError::TooShort));
    }

    #[test]
    fn file_kind_formats_extension_and_name() {
        assert_eq!(FileKind::Data.extension(), "dtbl");
        assert_eq!(FileKind::Catalog.extension(), "dbcat");
        assert_eq!(FileKind::Entity.extension(), "ent");
        assert_eq!(FileKind::Catalog.file_name("demo-db"), "demo-db.dbcat");
        assert_eq!(FileKind::Entity.file_name("users_v"), "users_v.ent");
        assert_eq!(FileKind::Data.magic().as_bytes(), *b"DTBL");
        assert_eq!(FileKind::Catalog.magic().as_bytes(), *b"DBCT");
        assert_eq!(FileKind::Entity.magic().as_bytes(), *b"DBEN");
    }
    